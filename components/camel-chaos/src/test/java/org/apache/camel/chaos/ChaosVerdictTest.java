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
package org.apache.camel.chaos;

import java.util.Map;

import org.apache.camel.CamelContext;
import org.apache.camel.CamelExecutionException;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.util.json.JsonObject;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Proof point (e): the verdict is right. The same route, the same traffic and the same seed (so the same calls fail):
 * with redelivery the report says PASS with no lost message; without error handling it says FAIL and counts exactly the
 * messages that were lost.
 */
class ChaosVerdictTest {

    private static final String P = "e-verdicts";
    private static final int MESSAGES = 200;
    private static final long SEED = 2026;

    @BeforeAll
    static void title() {
        Proof.start(P, "Proof point (e): recovery verdicts (ChaosVerdictTest), " + MESSAGES
                       + " messages, 30% of backend calls fail, seed " + SEED);
    }

    @Test
    void routeWithRedeliveryPasses() throws Exception {
        Outcome o = run(new RouteBuilder() {
            @Override
            public void configure() {
                errorHandler(defaultErrorHandler().maximumRedeliveries(5).redeliveryDelay(0));
                from("direct:orders").routeId("orders").to("mock:backend");
            }
        });
        assertThat(o.route.getString("result")).isEqualTo("PASS");
        assertThat(o.route.getString("verdict")).isEqualTo("RECOVERED");
        assertThat(o.route.getLong("lostMessages")).isZero();
        assertThat(o.callerSawFailure).isZero();
        assertThat(o.delivered).isEqualTo(MESSAGES);
        assertThat(o.report.get("result")).isEqualTo("PASS");
        Proof.log(P, "WITH redelivery (5):   result=PASS verdict=" + o.route.getString("verdict") + " faults="
                     + o.route.get("faultsInjected") + " redeliveries=" + o.route.get("redeliveries") + " lost="
                     + o.route.get("lostMessages") + " delivered=" + o.delivered + "/" + MESSAGES);
    }

    @Test
    void sameRouteWithoutErrorHandlingFailsAndCountsTheLostMessages() throws Exception {
        Outcome o = run(new RouteBuilder() {
            @Override
            public void configure() {
                from("direct:orders").routeId("orders").to("mock:backend");
            }
        });
        assertThat(o.route.getString("result")).isEqualTo("FAIL");
        assertThat(o.route.getLong("lostMessages")).isPositive()
                .isEqualTo(o.callerSawFailure)
                .isEqualTo(MESSAGES - o.delivered)
                .isEqualTo(o.injected);
        assertThat(o.report.get("result")).isEqualTo("FAIL");
        Proof.log(P, "WITHOUT error handling: result=FAIL verdict=" + o.route.getString("verdict") + " faults="
                     + o.route.get("faultsInjected") + " lost=" + o.route.get("lostMessages") + " (callers saw "
                     + o.callerSawFailure + " failures, backend got " + o.delivered + "/" + MESSAGES + ")");
    }

    @Test
    void deadLetterChannelKeepsTheMessagesAndPasses() throws Exception {
        Outcome o = run(new RouteBuilder() {
            @Override
            public void configure() {
                errorHandler(deadLetterChannel("mock:dead").maximumRedeliveries(1).redeliveryDelay(0));
                from("direct:orders").routeId("orders").to("mock:backend");
            }
        });
        long dead = o.deadLettered;
        assertThat(dead).isPositive();
        assertThat(o.route.getLong("failureHandled")).isEqualTo(dead);
        assertThat(o.route.getString("result")).isEqualTo("PASS");
        assertThat(o.delivered + dead).isEqualTo(MESSAGES);
        Proof.log(P, "WITH dead letter channel (1 redelivery): result=PASS lost=0 delivered=" + o.delivered
                     + " deadLettered=" + dead + " (handled by the error handler, counted as failureHandled)");
    }

    private Outcome run(RouteBuilder routes) throws Exception {
        CamelContext context = ChaosManagerTest.newContext(SEED, true, routes);
        try {
            ChaosManager manager = ChaosManager.get(context);
            ChaosRule rule = manager.addRule(ChaosRule.fail("mock:backend", 0.3).id("backend-30pct"));
            ProducerTemplate template = context.createProducerTemplate();
            int failures = 0;
            for (int i = 0; i < MESSAGES; i++) {
                try {
                    template.sendBody("direct:orders", "order-" + i);
                } catch (CamelExecutionException e) {
                    failures++;
                }
            }
            manager.removeRule("backend-30pct");
            // traffic after the fault, so recovery can be judged
            template.sendBody("direct:orders", "after");
            JsonObject report = manager.report();
            JsonObject route = (JsonObject) ((Map<?, ?>) report.get("routes")).get("orders");
            int delivered = context.getEndpoint("mock:backend", MockEndpoint.class).getReceivedCounter() - 1;
            MockEndpoint dead = (MockEndpoint) context.hasEndpoint("mock:dead");
            return new Outcome(
                    report, route, failures, delivered, rule.getInjected(),
                    dead != null ? dead.getReceivedCounter() : 0);
        } finally {
            context.stop();
        }
    }

    private record Outcome(
            JsonObject report, JsonObject route, int callerSawFailure, int delivered, long injected,
            long deadLettered) {
    }
}
