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
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

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
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Proof point (g1) of the camel-chaos proposal: an invariant of the TLA+ model of the circuit breaker's timeout and
 * fallback ({@code camel-lean/tla/circuitbreaker/CircuitBreaker.tla}: {@code SingleWriter}, {@code NoConcurrentWrite},
 * {@code TimeoutAnswered}) checked on a real {@code circuitBreaker()} route by a chaos run.
 * <p>
 * The model's fault is {@code CTimeout}: the time limiter fires while the task is still running. As a chaos rule it is
 * a latency rule on the protected endpoint whose delay puts the end of the task at about the timeout; the window is
 * below a millisecond, so the run sweeps the delay over the timeout (18 to 22 ms for a 20 ms timeout) with 32 callers.
 * The model's invariants, projected on what an exchange shows when it completes, become one chaos invariant: the
 * exchange tells one story, either the task's (task body, not from the fallback, not timed out) or the fallback's
 * (fallback body, from the fallback, not a successful execution), and carries no exception.
 * <p>
 * On main (with CAMEL-25682) the report says PASS. On the code before CAMEL-25682 (ResilienceProcessor.java of
 * 24cd63f10fb2^) run it with {@code -Dchaos.expect=FAIL}: the report is expected to count violations. The text report
 * is appended to {@code target/proof/g1-circuitbreaker-tla.txt} (or {@code -Dchaos.proof.dir}).
 */
class ResilienceChaosTimeoutTest extends CamelTestSupport {

    private static final String PROOF = "g1-circuitbreaker-tla";
    private static final int TIMEOUT = 20;
    private static final int[] DELAYS = { 18, 19, 20, 21, 22 };
    private static final int CALLS_PER_DELAY = Integer.getInteger("chaos.callsPerDelay", 4000);
    private static final int CALLERS = 32;
    private static final String TASK = "Task result";
    private static final String FALLBACK = "Fallback result";

    @Override
    protected CamelContext createCamelContext() throws Exception {
        CamelContext context = super.createCamelContext();
        Properties props = new Properties();
        props.put("camel.chaos.enabled", "true");
        props.put("camel.chaos.seed", "25682");
        context.getPropertiesComponent().setOverrideProperties(props);
        context.getRegistry().bind("chaosTimeoutPool",
                context.getExecutorServiceManager().newFixedThreadPool(this, "ChaosTimeoutPool", 64));
        return context;
    }

    @Override
    protected RoutesBuilder createRouteBuilder() {
        return new RouteBuilder() {
            @Override
            public void configure() {
                from("direct:start").routeId("cb")
                        .circuitBreaker()
                            .resilience4jConfiguration()
                                .timeoutEnabled(true)
                                .timeoutDuration(TIMEOUT)
                                .timeoutExecutorService("chaosTimeoutPool")
                                // keep the breaker closed: this run is about the timeout, not the state machine
                                .slidingWindowType("TIME_BASED").slidingWindowSize(1).minimumNumberOfCalls(1_000_000)
                            .end()
                            .to("direct:backend")
                        .onFallback()
                            .transform().constant(FALLBACK)
                        .end();

                from("direct:backend").routeId("backend")
                        .transform().constant(TASK);
            }
        };
    }

    /** The TLA+ invariants SingleWriter and TimeoutAnswered, as seen on the exchange when it completes. */
    static boolean oneStory(Exchange e) {
        if (e.getException() != null) {
            return false;
        }
        String body = e.getMessage().getBody(String.class);
        Object fromFallback = e.getProperty(ExchangePropertyKey.CIRCUIT_BREAKER_RESPONSE_FROM_FALLBACK);
        Object success = e.getProperty(ExchangePropertyKey.CIRCUIT_BREAKER_RESPONSE_SUCCESSFUL_EXECUTION);
        Object timedOut = e.getProperty(ExchangePropertyKey.CIRCUIT_BREAKER_RESPONSE_TIMED_OUT);
        if (TASK.equals(body)) {
            return !Boolean.TRUE.equals(fromFallback) && !Boolean.TRUE.equals(timedOut);
        }
        if (FALLBACK.equals(body)) {
            return Boolean.TRUE.equals(fromFallback) && !Boolean.TRUE.equals(success);
        }
        return false;
    }

    @Test
    void timeoutDuringTheTaskKeepsOneStoryPerExchange() throws Exception {
        ChaosManager chaos = ChaosManager.get(context);
        ChaosManager.Invariant invariant
                = chaos.invariant("cb", "one-story (SingleWriter, TimeoutAnswered)", ResilienceChaosTimeoutTest::oneStory);

        ExecutorService callers = context.getExecutorServiceManager().newFixedThreadPool(this, "ChaosCallers", CALLERS);
        AtomicLong fallbackAnswers = new AtomicLong();
        AtomicLong taskAnswers = new AtomicLong();
        AtomicLong callerSawViolation = new AtomicLong();
        List<String> perDelay = new ArrayList<>();
        try {
            for (int delay : DELAYS) {
                long before = invariant.getViolated();
                ChaosRule rule = chaos.addRule(ChaosRule.latency("direct:backend", delay).calls(CALLS_PER_DELAY)
                        .id("task-ends-at-the-timeout-" + delay + "ms"));
                List<Future<Exchange>> replies = new ArrayList<>();
                for (int i = 0; i < CALLS_PER_DELAY; i++) {
                    replies.add(callers.submit(() -> template.request("direct:start", x -> x.getIn().setBody("Hello"))));
                }
                for (Future<Exchange> f : replies) {
                    Exchange e = f.get(60, TimeUnit.SECONDS);
                    if (FALLBACK.equals(e.getMessage().getBody(String.class))) {
                        fallbackAnswers.incrementAndGet();
                    } else if (TASK.equals(e.getMessage().getBody(String.class))) {
                        taskAnswers.incrementAndGet();
                    }
                    if (!oneStory(e)) {
                        callerSawViolation.incrementAndGet();
                    }
                }
                await().atMost(10, TimeUnit.SECONDS).until(() -> "ended".equals(rule.getState()));
                perDelay.add("delay " + delay + " ms: " + rule.getInjected() + " delayed calls, "
                             + (invariant.getViolated() - before) + " exchanges violated one-story");
            }
        } finally {
            context.getExecutorServiceManager().shutdownNow(callers);
        }

        JsonObject report = chaos.report();
        String expected = System.getProperty("chaos.expect", "PASS");
        log("code: " + System.getProperty("chaos.codeLabel", "main") + ", expected report result " + expected);
        log("timeout " + TIMEOUT + " ms, " + CALLERS + " callers, " + CALLS_PER_DELAY + " calls per delay");
        perDelay.forEach(ResilienceChaosTimeoutTest::log);
        log("answers: task=" + taskAnswers + " fallback=" + fallbackAnswers + "; invariant checked="
            + invariant.getChecked() + " violated=" + invariant.getViolated() + "; violations seen by the callers="
            + callerSawViolation);
        log(chaos.reportText());
        assertEquals(expected, report.get("result"));
        if ("PASS".equals(expected)) {
            assertEquals(0, invariant.getViolated());
            assertEquals(0, callerSawViolation.get());
            assertTrue(taskAnswers.get() > 0 && fallbackAnswers.get() > 0,
                    "the sweep must cross the timeout: some calls answered by the task, some by the fallback");
        } else {
            assertTrue(invariant.getViolated() > 0, "violations are counted");
        }
    }

    private static void log(String text) {
        System.out.println(text);
        try {
            Path dir = Path.of(System.getProperty("chaos.proof.dir", "target/proof"));
            Files.createDirectories(dir);
            Files.writeString(dir.resolve(PROOF + ".txt"), text + System.lineSeparator(), StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }
}
