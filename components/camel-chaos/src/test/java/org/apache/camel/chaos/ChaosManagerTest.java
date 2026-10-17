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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.camel.CamelContext;
import org.apache.camel.CamelExecutionException;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.RoutesBuilder;
import org.apache.camel.ServiceStatus;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.util.json.JsonObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

class ChaosManagerTest {

    private CamelContext context;

    @AfterEach
    void stop() {
        if (context != null) {
            context.stop();
        }
    }

    static CamelContext newContext(long seed, boolean enabled, RoutesBuilder routes) throws Exception {
        CamelContext context = new DefaultCamelContext();
        Properties props = new Properties();
        props.put(ChaosManager.ENABLED, Boolean.toString(enabled));
        props.put(ChaosManager.SEED, Long.toString(seed));
        context.getPropertiesComponent().setOverrideProperties(props);
        context.addRoutes(routes);
        context.start();
        return context;
    }

    static RouteBuilder ordersWithDeadLetter() {
        return new RouteBuilder() {
            @Override
            public void configure() {
                errorHandler(deadLetterChannel("mock:dead").maximumRedeliveries(1).redeliveryDelay(0));
                from("direct:orders").routeId("orders").to("mock:backend");
            }
        };
    }

    static RouteBuilder ordersNoRedelivery() {
        return new RouteBuilder() {
            @Override
            public void configure() {
                from("direct:orders").routeId("orders").to("mock:backend");
            }
        };
    }

    @Test
    void installedFromClasspathAndOffByDefault() throws Exception {
        context = new DefaultCamelContext();
        context.addRoutes(ordersNoRedelivery());
        context.start();

        ChaosManager manager = ChaosManager.get(context);
        assertThat(manager).as("installed by the ContextServicePlugin").isNotNull();
        assertThat(manager.isEnabled()).isFalse();
        assertThatThrownBy(() -> manager.addRule(ChaosRule.fail("mock:*", 1.0)))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("disabled");
    }

    @Test
    void neverEnabledWithProfileProd() throws Exception {
        context = newContext(1, true, ordersNoRedelivery());
        context.getCamelContextExtension().setProfile("prod");
        ChaosManager manager = ChaosManager.get(context);
        assertThat(manager.isEnabled()).isFalse();
        assertThatThrownBy(() -> manager.addRule(ChaosRule.fail("mock:*", 1.0)))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void thirtyPercentFailuresAreRedeliveredOrDeadLettered() throws Exception {
        context = newContext(42, true, ordersWithDeadLetter());
        ChaosManager manager = ChaosManager.get(context);
        ProducerTemplate template = context.createProducerTemplate();

        ChaosRule rule = manager.addRule(ChaosRule.fail("mock:backend", 0.3).id("backend-30"));
        for (int i = 0; i < 100; i++) {
            template.sendBody("direct:orders", "order-" + i);
        }
        manager.removeRule("backend-30");
        for (int i = 100; i < 110; i++) {
            template.sendBody("direct:orders", "order-" + i);
        }

        MockEndpoint backend = context.getEndpoint("mock:backend", MockEndpoint.class);
        MockEndpoint dead = context.getEndpoint("mock:dead", MockEndpoint.class);
        JsonObject report = manager.report();
        JsonObject orders = (JsonObject) ((Map<?, ?>) report.get("routes")).get("orders");

        assertThat(rule.getInjected()).isBetween(20L, 50L);
        assertThat(rule.getMatched()).isEqualTo(100L + orders.getLong("redeliveries"));
        assertThat(orders.getLong("redeliveries")).isPositive();
        assertThat(dead.getReceivedCounter()).as("some exchanges fail twice and go to the dead letter channel").isPositive();
        assertThat(orders.getLong("failureHandled")).isEqualTo(dead.getReceivedCounter());
        assertThat(orders.getLong("completed")).isEqualTo(backend.getReceivedCounter());
        assertThat(orders.getLong("completed") + orders.getLong("failureHandled")).isEqualTo(110L);
        assertThat(orders.getLong("failed")).isZero();
        assertThat(orders.getLong("completedAfterLastFault")).isGreaterThanOrEqualTo(10L);
        assertThat(orders.getString("verdict")).isEqualTo("RECOVERED");
        assertThat(rule.getState()).isEqualTo("ended");
    }

    @Test
    void nextThreeCallsFailThenRuleEnds() throws Exception {
        context = newContext(42, true, ordersNoRedelivery());
        ChaosManager manager = ChaosManager.get(context);
        ProducerTemplate template = context.createProducerTemplate();

        ChaosRule rule = manager.addRule(ChaosRule.fail("mock:*", 1.0).calls(3)
                .exception(IOException.class.getName(), "connection reset by chaos"));
        int failures = 0;
        for (int i = 0; i < 5; i++) {
            try {
                template.sendBody("direct:orders", "order-" + i);
            } catch (CamelExecutionException e) {
                assertThat(e.getCause()).isInstanceOf(IOException.class).hasMessage("connection reset by chaos");
                failures++;
            }
        }
        assertThat(failures).isEqualTo(3);
        assertThat(rule.getInjectedCallNumbers()).containsExactly(1L, 2L, 3L);
        assertThat(context.getEndpoint("mock:backend", MockEndpoint.class).getReceivedCounter()).isEqualTo(2);

        String verdict = ((JsonObject) ((Map<?, ?>) manager.report().get("routes")).get("orders")).getString("verdict");
        assertThat(verdict).startsWith("RECOVERED, but 3 exchange(s) failed unhandled");
        assertThat(rule.getState()).isEqualTo("ended");
    }

    @Test
    void sameSeedGivesSameFaults() throws Exception {
        List<Long> first = injectedCalls(7);
        List<Long> second = injectedCalls(7);
        List<Long> other = injectedCalls(8);
        assertThat(first).isNotEmpty().isEqualTo(second);
        assertThat(other).isNotEqualTo(first);
    }

    private static List<Long> injectedCalls(long seed) throws Exception {
        CamelContext ctx = newContext(seed, true, ordersWithDeadLetter());
        try {
            ChaosRule rule = ChaosManager.get(ctx).addRule(ChaosRule.fail("mock:backend", 0.3));
            ProducerTemplate template = ctx.createProducerTemplate();
            for (int i = 0; i < 50; i++) {
                template.sendBody("direct:orders", "order-" + i);
            }
            return rule.getInjectedCallNumbers();
        } finally {
            ctx.stop();
        }
    }

    @Test
    void latencyIsAdded() throws Exception {
        context = newContext(42, true, ordersNoRedelivery());
        ChaosManager manager = ChaosManager.get(context);
        ProducerTemplate template = context.createProducerTemplate();

        ChaosRule rule = manager.addRule(ChaosRule.latency("mock:backend", 300).calls(1));
        long start = System.nanoTime();
        template.sendBody("direct:orders", "slow");
        long tookMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        template.sendBody("direct:orders", "fast");

        assertThat(tookMs).isGreaterThanOrEqualTo(300);
        assertThat(rule.getInjected()).isEqualTo(1);
        assertThat(context.getEndpoint("mock:backend", MockEndpoint.class).getReceivedCounter()).isEqualTo(2);
    }

    @Test
    void ruleExpiresAfterDuration() throws Exception {
        context = newContext(42, true, ordersNoRedelivery());
        ChaosManager manager = ChaosManager.get(context);
        ChaosRule rule = manager.addRule(ChaosRule.fail("mock:*", 1.0).duration(200));
        assertThat(manager.getActiveRules()).contains(rule);
        await().atMost(5, TimeUnit.SECONDS).until(() -> !manager.getActiveRules().contains(rule));
        assertThat(rule.toJson().getString("endReason")).isEqualTo("expired");
    }

    @Test
    void stalledConsumerIsResumed() throws Exception {
        context = newContext(42, true, new RouteBuilder() {
            @Override
            public void configure() {
                from("timer:tick?period=20").routeId("ticker").to("mock:ticks");
            }
        });
        ChaosManager manager = ChaosManager.get(context);
        MockEndpoint ticks = context.getEndpoint("mock:ticks", MockEndpoint.class);
        await().atMost(5, TimeUnit.SECONDS).until(() -> ticks.getReceivedCounter() > 2);

        ChaosRule rule = manager.addRule(ChaosRule.stall("ticker", 500));
        ServiceStatus during = context.getRouteController().getRouteStatus("ticker");
        assertThat(during.isStarted()).isFalse();

        await().atMost(5, TimeUnit.SECONDS).until(() -> "ended".equals(rule.getState()));
        assertThat(context.getRouteController().getRouteStatus("ticker").isStarted()).isTrue();
        int after = ticks.getReceivedCounter();
        await().atMost(5, TimeUnit.SECONDS).until(() -> ticks.getReceivedCounter() > after + 2);

        JsonObject ticker = (JsonObject) ((Map<?, ?>) manager.report().get("routes")).get("ticker");
        assertThat(ticker.getString("verdict")).isEqualTo("RECOVERED");
    }
}
