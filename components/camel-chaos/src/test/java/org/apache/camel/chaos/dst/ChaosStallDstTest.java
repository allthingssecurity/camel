/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.camel.chaos.dst;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.camel.Exchange;
import org.apache.camel.ServiceStatus;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.chaos.ChaosManager;
import org.apache.camel.chaos.ChaosRule;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.DefaultConsumer;
import org.apache.camel.test.dst.Dst;
import org.apache.camel.test.dst.DstCamel;
import org.apache.camel.test.dst.DstExecutor;
import org.apache.camel.util.json.JsonObject;
import org.apache.camel.util.json.Jsoner;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A chaos run inside a camel-test-dst world. camel-chaos reads the clock and sleeps through Camel's internal
 * {@code TimeSource} and runs its expiry check and its route suspends on pools of the {@code ExecutorServiceManager},
 * so in a DST run the rules live in virtual time: a stall of ten minutes ends after ten virtual minutes and a few
 * milliseconds of real time, a latency rule is a virtual wait, and the whole report, times included, is a function of
 * the seed. Strict mode is on: a clock read or a sleep of chaos from a thread outside the harness would fail the run.
 */
class ChaosStallDstTest {

    private static final long STALL_MS = 600_000;
    private static final long STALL_AT = 5_500;
    private static final int MESSAGES = 20;

    @Test
    void aTenMinuteStallEndsInVirtualTimeAndTheRunReplaysFromItsSeed() throws Exception {
        long start = System.nanoTime();
        Run first = run(7);
        long realMs = (System.nanoTime() - start) / 1_000_000;

        // the stall lasted ten virtual minutes (resumed by the expiry check, which runs every 100 virtual ms)
        assertThat(first.stall.getState()).isEqualTo("ended");
        assertThat(rule(first.report, "stall-orders").get("endReason")).isEqualTo("expired");
        assertThat(first.resumedAt - first.stalledAt).isBetween(STALL_MS, STALL_MS + 200);
        assertThat(first.stalledAt).isEqualTo(Dst.EPOCH_MILLIS + STALL_AT);
        // and took no real time to speak of
        assertThat(realMs).isLessThan(10_000);
        // nothing was lost: the backlog of the stall was routed after the resume
        assertThat(first.report.get("result")).isEqualTo("PASS");
        assertThat(first.completed).isEqualTo(MESSAGES);

        // the same seed gives the same run: the DST trace and the whole chaos report, times included
        Run again = run(7);
        assertThat(again.traceHash).isEqualTo(first.traceHash);
        assertThat(again.json).isEqualTo(first.json);
        // another seed draws other latency faults
        Run other = run(8);
        assertThat(other.latencyCalls).isNotEqualTo(first.latencyCalls);

        Files.deleteIfExists(proofDir().resolve("dst-stall.txt"));
        proof("dst-stall", "seed 7: stall " + STALL_MS + " virtual ms at t=" + STALL_AT + " ms, stalled at "
                           + Instant.ofEpochMilli(first.stalledAt) + ", resumed at " + Instant.ofEpochMilli(first.resumedAt)
                           + " (" + (first.resumedAt - first.stalledAt) + " virtual ms), real time of the run " + realMs
                           + " ms, " + first.steps + " DST steps");
        proof("dst-stall", "seed 7: result " + first.report.get("result") + ", completed " + first.completed + "/"
                           + MESSAGES + ", latency faults on calls " + first.latencyCalls + ", trace "
                           + first.traceHash.substring(0, 16) + ", report sha256 " + sha(first.json).substring(0, 16));
        proof("dst-stall", "seed 7 again: trace " + again.traceHash.substring(0, 16) + ", report sha256 "
                           + sha(again.json).substring(0, 16) + " (identical, times included)");
        proof("dst-stall", "seed 8: latency faults on calls " + other.latencyCalls + ", report sha256 "
                           + sha(other.json).substring(0, 16));
    }

    private static final class Run {
        ChaosRule stall;
        JsonObject report;
        String json;
        String traceHash;
        long stalledAt;
        long resumedAt;
        long steps;
        int completed;
        Object latencyCalls;
    }

    private static Run run(long seed) throws Exception {
        Run r = new Run();
        Dst dst = new Dst(seed);
        DefaultCamelContext context = null;
        try {
            dst.strict(true, List.of());
            context = DstCamel.newContext(dst);
            Properties props = new Properties();
            props.put(ChaosManager.ENABLED, "true");
            props.put(ChaosManager.SEED, Long.toString(seed));
            context.getPropertiesComponent().setOverrideProperties(props);
            int[] completed = new int[1];
            context.addRoutes(new RouteBuilder() {
                @Override
                public void configure() {
                    from("direct:orders").routeId("orders").to("mock:backend").process(e -> completed[0]++);
                }
            });
            context.start();
            ChaosManager manager = ChaosManager.get(context);
            // a latency rule: half of the first 20 calls wait 2 virtual seconds, drawn from the chaos seed
            manager.addRule(ChaosRule.latency("mock:backend", 2_000).probability(0.5).calls(MESSAGES).id("slow"));
            manager.expect("orders", MESSAGES);

            // upstream: one message per virtual second, kept back while the route is not started
            DstExecutor client = dst.executor("client", 1);
            Deque<String> backlog = new ArrayDeque<>();
            int[] offered = new int[1];
            DefaultCamelContext ctx = context;
            Runnable[] tick = new Runnable[1];
            tick[0] = () -> {
                if (offered[0] < MESSAGES) {
                    backlog.add("order-" + ++offered[0]);
                }
                while (!backlog.isEmpty() && ctx.getRouteController().getRouteStatus("orders") == ServiceStatus.Started) {
                    send(ctx, backlog.poll());
                }
                if (offered[0] < MESSAGES || !backlog.isEmpty()) {
                    client.schedule("tick", 1_000, tick[0]);
                }
            };
            client.schedule("tick", 1_000, tick[0]);

            // the operator stalls the route for ten minutes
            ChaosRule[] stall = new ChaosRule[1];
            dst.executor("operator", 1).schedule("add stall", STALL_AT,
                    () -> stall[0] = manager.addRule(ChaosRule.stall("orders", STALL_MS).id("stall-orders")));

            dst.runUntil(() -> completed[0] == MESSAGES && stall[0] != null && !stall[0].getState().equals("active"),
                    "all messages routed and the stall ended");
            dst.runFor(500);

            r.stall = stall[0];
            r.completed = completed[0];
            r.report = manager.report();
            r.json = Jsoner.serialize(r.report);
            r.latencyCalls = rule(r.report, "slow").get("injectedCallNumbers");
            for (Object o : (List<?>) r.report.get("timeline")) {
                Map<?, ?> m = (Map<?, ?>) o;
                if ("route-stalled".equals(m.get("event"))) {
                    r.stalledAt = Instant.parse((String) m.get("time")).toEpochMilli();
                } else if ("route-resumed".equals(m.get("event"))) {
                    r.resumedAt = Instant.parse((String) m.get("time")).toEpochMilli();
                }
            }
            r.traceHash = dst.traceHash();
            r.steps = dst.steps();
            assertThat(dst.escape()).isNull();
        } finally {
            dst.dropPending();
            if (context != null) {
                context.stop();
            }
            dst.close();
        }
        return r;
    }

    private static Map<?, ?> rule(JsonObject report, String id) {
        for (Object o : (List<?>) report.get("rules")) {
            if (id.equals(((Map<?, ?>) o).get("id"))) {
                return (Map<?, ?>) o;
            }
        }
        throw new IllegalArgumentException("no rule " + id);
    }

    /** Sends without waiting (no blocking wait on the loop thread). */
    private static void send(DefaultCamelContext context, String body) {
        DefaultConsumer consumer = (DefaultConsumer) context.getRoute("orders").getConsumer();
        Exchange exchange = consumer.createExchange(false);
        exchange.getMessage().setBody(body);
        consumer.getAsyncProcessor().process(exchange, doneSync -> consumer.releaseExchange(exchange, false));
    }

    private static String sha(String text) throws Exception {
        return HexFormat.of()
                .formatHex(MessageDigest.getInstance("SHA-256").digest(text.getBytes(StandardCharsets.UTF_8)));
    }

    private static Path proofDir() {
        return Path.of(System.getProperty("chaos.proof.dir", "target/proof"));
    }

    private static void proof(String name, String line) throws Exception {
        System.out.println("[proof " + name + "] " + line);
        Path dir = proofDir();
        Files.createDirectories(dir);
        Files.writeString(dir.resolve(name + ".txt"), line + System.lineSeparator(), StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }
}
