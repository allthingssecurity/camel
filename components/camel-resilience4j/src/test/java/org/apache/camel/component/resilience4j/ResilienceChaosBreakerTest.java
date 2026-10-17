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
package org.apache.camel.component.resilience4j;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.ExchangePropertyKey;
import org.apache.camel.RoutesBuilder;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.chaos.ChaosManager;
import org.apache.camel.chaos.ChaosRule;
import org.apache.camel.test.junit6.CamelTestSupport;
import org.apache.camel.util.json.JsonObject;
import org.junit.jupiter.api.Test;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Proof point (g2) of the camel-chaos proposal: a fault parameter chosen by Z3, and two state-machine properties of the
 * circuit breaker checked from the chaos report.
 * <p>
 * {@code components/camel-chaos/models/fault_parameters_z3.py} computes, for this route's configuration (COUNT_BASED
 * window of 10, minimumNumberOfCalls 10, failureRateThreshold 50 %) after 10 successful calls, that 5 consecutive
 * failing calls open the breaker and proves that 4 failing calls never do. The chaos {@code fail} rule takes
 * {@code calls=5} (and {@code calls=4} as the control).
 * <p>
 * The properties come from resilience4j's documented state machine (not from the TLA+ model in camel-lean, which is
 * about the timeout and the fallback): while the breaker is OPEN no call reaches the protected endpoint; after the wait
 * in OPEN, a successful call in HALF_OPEN closes it, and calls reach the endpoint again. A {@code fail} rule with
 * probability 0 placed first on the endpoint is the probe: its {@code matched} counter is the number of calls that
 * reached the endpoint. A chaos invariant checks each exchange: a rejected exchange (breaker OPEN, call not attempted)
 * has the fallback's body and never reached the backend.
 */
class ResilienceChaosBreakerTest extends CamelTestSupport {

    private static final String PROOF = "g2-breaker-z3";
    private static final int WARMUP = 10;
    // from fault_parameters_z3.py, part 1
    private static final int OPENING_FAILURES = 5;

    @Override
    protected CamelContext createCamelContext() throws Exception {
        CamelContext context = super.createCamelContext();
        Properties props = new Properties();
        props.put("camel.chaos.enabled", "true");
        props.put("camel.chaos.seed", "5");
        context.getPropertiesComponent().setOverrideProperties(props);
        return context;
    }

    @Override
    protected RoutesBuilder createRouteBuilder() {
        return new RouteBuilder() {
            @Override
            public void configure() {
                from("direct:start").routeId("breaker")
                        .circuitBreaker()
                            .resilience4jConfiguration()
                                .slidingWindowType("COUNT_BASED").slidingWindowSize(10).minimumNumberOfCalls(10)
                                .failureRateThreshold(50)
                                .waitDurationInOpenState("1s").permittedNumberOfCallsInHalfOpenState(1)
                                .automaticTransitionFromOpenToHalfOpenEnabled(false)
                            .end()
                            .to("direct:backend")
                        .onFallback()
                            .transform().constant("fallback")
                        .end();

                from("direct:backend").routeId("backend")
                        .setHeader("Reached-Backend", constant(true))
                        .transform().constant("ok");
            }
        };
    }

    @Test
    void fiveFailuresOpenTheBreakerAndNoCallReachesTheEndpointWhileOpen() throws Exception {
        ChaosManager chaos = ChaosManager.get(context);
        log("=== fail rule calls=" + OPENING_FAILURES + " (Z3: the smallest count that opens the breaker)");
        ChaosRule probe = start(chaos);
        ChaosRule fail = chaos.addRule(ChaosRule.fail("direct:backend", 1.0).calls(OPENING_FAILURES).id("open-it"));
        for (int i = 0; i < OPENING_FAILURES; i++) {
            assertEquals("fallback", call().getMessage().getBody(String.class));
        }
        assertEquals(OPENING_FAILURES, fail.getInjected());
        long reachedBeforeOpen = probe.getMatched();
        log("after warm-up and the fail rule: probe matched=" + reachedBeforeOpen + " fail injected=" + fail.getInjected());

        // OPEN: no call reaches the protected endpoint
        Exchange last = null;
        for (int i = 0; i < 20; i++) {
            last = call();
            assertEquals("fallback", last.getMessage().getBody(String.class));
            assertEquals(Boolean.TRUE, last.getProperty(ExchangePropertyKey.CIRCUIT_BREAKER_RESPONSE_REJECTED));
        }
        assertEquals(reachedBeforeOpen, probe.getMatched(), "no call reaches the endpoint while the breaker is OPEN");
        log("OPEN: 20 calls, state=" + last.getProperty(ExchangePropertyKey.CIRCUIT_BREAKER_RESPONSE_STATE)
            + ", probe matched=" + probe.getMatched() + " (unchanged)");

        // after waitDurationInOpenState (1 s): the next call is the half-open probe; it succeeds and the breaker closes
        await().pollDelay(Duration.ofMillis(1200)).atMost(5, TimeUnit.SECONDS).until(() -> true);
        Exchange halfOpen = call();
        assertEquals("ok", halfOpen.getMessage().getBody(String.class));
        assertEquals(reachedBeforeOpen + 1, probe.getMatched());
        for (int i = 0; i < 10; i++) {
            last = call();
            assertEquals("ok", last.getMessage().getBody(String.class));
        }
        assertEquals("CLOSED", last.getProperty(ExchangePropertyKey.CIRCUIT_BREAKER_RESPONSE_STATE));
        assertEquals(reachedBeforeOpen + 11, probe.getMatched());
        log("HALF_OPEN success then CLOSED: state=" + last.getProperty(ExchangePropertyKey.CIRCUIT_BREAKER_RESPONSE_STATE)
            + ", probe matched=" + probe.getMatched() + " (+1 half-open call, +10 after closing)");
        finish(chaos, probe);
    }

    @Test
    void fourFailuresDoNotOpenTheBreaker() throws Exception {
        ChaosManager chaos = ChaosManager.get(context);
        log("=== fail rule calls=" + (OPENING_FAILURES - 1) + " (Z3: proved never to open the breaker)");
        ChaosRule probe = start(chaos);
        chaos.addRule(ChaosRule.fail("direct:backend", 1.0).calls(OPENING_FAILURES - 1).id("almost-open-it"));
        for (int i = 0; i < OPENING_FAILURES - 1; i++) {
            call();
        }
        Exchange last = null;
        for (int i = 0; i < 20; i++) {
            last = call();
            assertEquals("ok", last.getMessage().getBody(String.class));
        }
        assertEquals(WARMUP + OPENING_FAILURES - 1 + 20, probe.getMatched(), "every call reached the endpoint");
        assertEquals("CLOSED", last.getProperty(ExchangePropertyKey.CIRCUIT_BREAKER_RESPONSE_STATE));
        log("CLOSED throughout: 20 calls after the rule, probe matched=" + probe.getMatched() + " = " + WARMUP + " + "
            + (OPENING_FAILURES - 1) + " + 20");
        finish(chaos, probe);
    }

    private ChaosRule start(ChaosManager chaos) throws Exception {
        chaos.invariant("breaker", "rejected => fallback, backend not reached",
                e -> !Boolean.TRUE.equals(e.getProperty(ExchangePropertyKey.CIRCUIT_BREAKER_RESPONSE_REJECTED))
                        || "fallback".equals(e.getMessage().getBody(String.class))
                                && e.getMessage().getHeader("Reached-Backend") == null);
        // the probe: first on the endpoint, never injects, counts the calls that reach the endpoint
        ChaosRule probe = chaos.addRule(ChaosRule.fail("direct:backend", 0.0).id("probe-calls-reaching-backend"));
        for (int i = 0; i < WARMUP; i++) {
            assertEquals("ok", call().getMessage().getBody(String.class));
        }
        return probe;
    }

    private void finish(ChaosManager chaos, ChaosRule probe) throws IOException {
        chaos.removeRule(probe.getId());
        JsonObject report = chaos.report();
        log(chaos.reportText());
        assertEquals("PASS", report.get("result"));
    }

    private Exchange call() {
        return template.request("direct:start", e -> e.getIn().setBody("Hello"));
    }

    private static void log(String text) throws IOException {
        System.out.println(text);
        Path dir = Path.of(System.getProperty("chaos.proof.dir", "target/proof"));
        Files.createDirectories(dir);
        Files.writeString(dir.resolve(PROOF + ".txt"), text + System.lineSeparator(), StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }
}
