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

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.camel.CamelContext;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.console.DevConsole;
import org.apache.camel.console.DevConsoleRegistry;
import org.apache.camel.health.HealthCheckRegistry;
import org.apache.camel.impl.health.RoutesHealthCheckRepository;
import org.apache.camel.util.json.JsonObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * The demo: a timer route calls a backend with a dead letter channel. Everything is driven through the {@code chaos}
 * dev console, the same way Camel CLI and the MCP tool drive it (through the CLI connector, action {@code chaos}).
 * <ol>
 * <li>30% of the next 20 backend calls fail;</li>
 * <li>the consumer is stalled for 600 ms (health goes DOWN, then UP);</li>
 * <li>the report says whether each route recovered.</li>
 * </ol>
 * The text report is written to {@code target/chaos-demo-report.txt}, the JSON one to
 * {@code target/chaos-demo-report.json}.
 */
class ChaosDemoTest {

    private CamelContext context;

    @AfterEach
    void stop() {
        if (context != null) {
            context.stop();
        }
    }

    @Test
    void demo() throws Exception {
        context = ChaosManagerTest.newContext(2026, true, new RouteBuilder() {
            @Override
            public void configure() {
                errorHandler(deadLetterChannel("mock:dead").maximumRedeliveries(2).redeliveryDelay(5));
                from("timer:orders?period=15&includeMetadata=true").routeId("orders")
                        .setBody(simple("order-${exchangeProperty.CamelTimerCounter}"))
                        .to("mock:backend");
            }
        });
        HealthCheckRegistry.get(context).register(new RoutesHealthCheckRepository());
        DevConsole console = context.getCamelContextExtension().getContextPlugin(DevConsoleRegistry.class)
                .resolveById("chaos");
        assertThat(console).as("chaos dev console discovered from the classpath").isNotNull();

        MockEndpoint backend = context.getEndpoint("mock:backend", MockEndpoint.class);
        await().atMost(5, TimeUnit.SECONDS).until(() -> backend.getReceivedCounter() > 5);

        // 1. 30% failures on the next 20 backend calls
        JsonObject added = call(console, Map.of("action", "add", "type", "fail", "pattern", "mock:backend",
                "probability", "30%", "calls", "20", "id", "backend-30pct"));
        assertThat(added.getString("result")).isEqualTo("added rule backend-30pct");
        ChaosManager manager = ChaosManager.get(context);
        await().atMost(10, TimeUnit.SECONDS).until(() -> "ended".equals(manager.getRule("backend-30pct").getState()));

        // 2. stall the consumer for 600 ms
        call(console, Map.of("action", "add", "type", "stall", "routeId", "orders", "duration", "600", "id",
                "stall-orders"));
        await().atMost(10, TimeUnit.SECONDS).until(() -> "ended".equals(manager.getRule("stall-orders").getState()));

        // let the route run a little after the last fault
        int after = backend.getReceivedCounter();
        await().atMost(5, TimeUnit.SECONDS).until(() -> backend.getReceivedCounter() > after + 5);

        // 3. the report
        JsonObject json = call(console, Map.of("action", "report"));
        String text = (String) console.call(DevConsole.MediaType.TEXT, Map.of());
        Files.writeString(Path.of("target/chaos-demo-report.txt"), text);
        Files.writeString(Path.of("target/chaos-demo-report.json"), json.toJson());
        System.out.println(text);

        JsonObject report = (JsonObject) json.get("report");
        JsonObject orders = (JsonObject) ((Map<?, ?>) report.get("routes")).get("orders");
        List<?> rules = (List<?>) report.get("rules");
        JsonObject fail = (JsonObject) rules.get(0);
        JsonObject stall = (JsonObject) rules.get(1);

        assertThat(fail.getLong("matched")).isEqualTo(20L);
        assertThat(fail.getLong("injected")).isPositive();
        assertThat(fail.getString("endReason")).isEqualTo("calls-reached");
        assertThat(orders.getLong("redeliveries")).isPositive();
        assertThat(orders.getLong("failureHandled")).isEqualTo(context.getEndpoint("mock:dead", MockEndpoint.class)
                .getReceivedCounter());

        assertThat(stateOf(stall, "healthBefore")).isEqualTo("UP");
        assertThat(stateOf(stall, "healthDuring")).isEqualTo("DOWN");
        assertThat(stateOf(stall, "healthAfter")).isEqualTo("UP");

        assertThat(orders.getString("verdict")).isEqualTo("RECOVERED");
    }

    private static Object stateOf(JsonObject rule, String key) {
        return ((Map<?, ?>) rule.get(key)).get("state");
    }

    private static JsonObject call(DevConsole console, Map<String, Object> options) {
        Map<String, Object> copy = new HashMap<>(options);
        JsonObject jo = (JsonObject) console.call(DevConsole.MediaType.JSON, copy);
        assertThat(jo.get("error")).as("console error").isNull();
        return jo;
    }
}
