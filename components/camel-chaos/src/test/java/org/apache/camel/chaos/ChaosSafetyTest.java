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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.camel.CamelContext;
import org.apache.camel.CamelExecutionException;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.console.DevConsole;
import org.apache.camel.console.DevConsoleRegistry;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.util.json.JsonObject;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * Proof point (d), safety: chaos cannot start unless it is enabled explicitly, never with profile prod, rules end by
 * themselves, and removing a rule gives back the normal behaviour.
 */
class ChaosSafetyTest {

    private static final String P = "d-safety";
    private CamelContext context;

    @BeforeAll
    static void title() {
        Proof.start(P, "Proof point (d): safety (ChaosSafetyTest)");
    }

    @AfterAll
    static void done() {
        Proof.log(P, "all safety tests passed");
    }

    @AfterEach
    void stop() {
        if (context != null) {
            context.stop();
        }
    }

    @Test
    void offByDefaultAndNoFaultReachesTheRoute() throws Exception {
        context = new DefaultCamelContext();
        context.addRoutes(ChaosManagerTest.ordersNoRedelivery());
        context.start();

        ChaosManager manager = ChaosManager.get(context);
        assertThat(manager).as("installed by the ContextServicePlugin").isNotNull();
        assertThat(manager.isEnabled()).isFalse();
        assertThatThrownBy(() -> manager.addRule(ChaosRule.fail("mock:*", 1.0)))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("disabled");

        // the dev console (and so the CLI and the MCP tool) cannot add a rule either, and has no way to enable chaos
        DevConsole console = DevConsoleRegistry.get(context).resolveById("chaos");
        JsonObject added = call(console, "action", "add", "type", "fail", "pattern", "mock:*");
        assertThat(added.getString("error")).contains("disabled");
        JsonObject enable = call(console, "action", "enable");
        assertThat(enable.getString("error")).contains("unknown action");

        ProducerTemplate template = context.createProducerTemplate();
        for (int i = 0; i < 20; i++) {
            template.sendBody("direct:orders", i);
        }
        assertThat(context.getEndpoint("mock:backend", MockEndpoint.class).getReceivedCounter()).isEqualTo(20);
        assertThat(manager.getRules()).isEmpty();

        // the hooks are gone, and chaos cannot be switched on at runtime, not even from code
        assertThat(context.getCamelContextExtension().getInterceptStrategies()).doesNotContain(manager);
        assertThat(context.getManagementStrategy().getEventNotifiers())
                .noneMatch(n -> n.getClass().getName().startsWith(ChaosManager.class.getName()));
        manager.setEnabled(true);
        System.setProperty(ChaosManager.ENABLED, "true");
        try {
            assertThat(manager.isEnabled()).isFalse();
            assertThatThrownBy(() -> manager.addRule(ChaosRule.fail("mock:*", 1.0)))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("when the application started");
        } finally {
            System.clearProperty(ChaosManager.ENABLED);
        }
        Proof.log(P, "off by default: add refused by API and console, no 'enable' action, 20/20 exchanges untouched,"
                     + " hooks removed at startup, cannot be switched on at runtime (setEnabled/system property)");
    }

    @Test
    void neverEnabledWithProfileProd() throws Exception {
        context = ChaosManagerTest.newContext(1, true, ChaosManagerTest.ordersNoRedelivery());
        context.getCamelContextExtension().setProfile("prod");
        ChaosManager manager = ChaosManager.get(context);
        assertThat(manager.isEnabled()).isFalse();
        assertThatThrownBy(() -> manager.addRule(ChaosRule.fail("mock:*", 1.0)))
                .isInstanceOf(IllegalStateException.class);
        manager.setEnabled(true);
        assertThat(manager.isEnabled()).as("not even when forced in code").isFalse();
        Proof.log(P, "profile prod: refused even with camel.chaos.enabled=true and setEnabled(true)");
    }

    @Test
    void ruleExpiresAfterDuration() throws Exception {
        context = ChaosManagerTest.newContext(42, true, ChaosManagerTest.ordersNoRedelivery());
        ChaosManager manager = ChaosManager.get(context);
        ChaosRule rule = manager.addRule(ChaosRule.fail("mock:*", 1.0).duration(200));
        assertThat(manager.getActiveRules()).contains(rule);
        await().atMost(5, TimeUnit.SECONDS).until(() -> !manager.getActiveRules().contains(rule));
        assertThat(rule.toJson().getString("endReason")).isEqualTo("expired");

        // with no traffic at all, and normal behaviour afterwards
        context.createProducerTemplate().sendBody("direct:orders", "after");
        assertThat(context.getEndpoint("mock:backend", MockEndpoint.class).getReceivedCounter()).isEqualTo(1);
        Proof.log(P, "rule with duration=200ms ended by itself (endReason=expired), next exchange routed normally");
    }

    @Test
    void ruleEndsAfterItsCalls() throws Exception {
        context = ChaosManagerTest.newContext(42, true, ChaosManagerTest.ordersNoRedelivery());
        ChaosManager manager = ChaosManager.get(context);
        ChaosRule rule = manager.addRule(ChaosRule.fail("mock:*", 1.0).calls(5));
        ProducerTemplate template = context.createProducerTemplate();
        int failures = 0;
        for (int i = 0; i < 25; i++) {
            try {
                template.sendBody("direct:orders", i);
            } catch (CamelExecutionException e) {
                failures++;
            }
        }
        assertThat(failures).isEqualTo(5);
        await().atMost(5, TimeUnit.SECONDS).until(() -> "ended".equals(rule.getState()));
        assertThat(rule.toJson().getString("endReason")).isEqualTo("calls-reached");
        Proof.log(P, "rule with calls=5: exactly 5 of 25 exchanges failed, then the rule ended (calls-reached)");
    }

    @Test
    void removingARuleRestoresNormalBehaviour() throws Exception {
        context = ChaosManagerTest.newContext(42, true, ChaosManagerTest.ordersNoRedelivery());
        ChaosManager manager = ChaosManager.get(context);
        ProducerTemplate template = context.createProducerTemplate();
        MockEndpoint backend = context.getEndpoint("mock:backend", MockEndpoint.class);

        manager.addRule(ChaosRule.fail("mock:backend", 1.0).id("all"));
        manager.addRule(ChaosRule.latency("mock:backend", 50).id("slow"));
        int failures = 0;
        for (int i = 0; i < 10; i++) {
            try {
                template.sendBody("direct:orders", i);
            } catch (CamelExecutionException e) {
                failures++;
            }
        }
        assertThat(failures).isEqualTo(10);
        assertThat(backend.getReceivedCounter()).isZero();

        assertThat(manager.removeRule("all")).isTrue();
        manager.clear();
        assertThat(manager.getActiveRules()).isEmpty();
        long start = System.nanoTime();
        for (int i = 0; i < 10; i++) {
            template.sendBody("direct:orders", i);
        }
        long tookMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        assertThat(backend.getReceivedCounter()).isEqualTo(10);
        assertThat(tookMs).as("no latency left").isLessThan(500);
        Proof.log(P, "100% fail + 50ms latency: 10/10 failed; after remove/clear: 10/10 routed in " + tookMs
                     + " ms (no latency left)");
    }

    private static JsonObject call(DevConsole console, String... kv) {
        Map<String, Object> options = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            options.put(kv[i], kv[i + 1]);
        }
        return (JsonObject) console.call(DevConsole.MediaType.JSON, options);
    }
}
