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
import java.util.List;
import java.util.Map;

import org.apache.camel.CamelContext;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.console.DevConsole;
import org.apache.camel.console.DevConsoleRegistry;
import org.apache.camel.util.json.JsonObject;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Invariants: a predicate every exchange of a route must match when it completes; a violation makes the report FAIL.
 */
class ChaosInvariantTest {

    @Test
    void invariantFromTheConsoleCountsViolations() throws Exception {
        CamelContext context = ChaosManagerTest.newContext(3, true, new RouteBuilder() {
            @Override
            public void configure() {
                errorHandler(deadLetterChannel("mock:dead").maximumRedeliveries(0));
                from("direct:orders").routeId("orders")
                        .setHeader("handled-by", constant("route"))
                        .to("mock:backend");
            }
        });
        try {
            DevConsole console = context.getCamelContextExtension().getContextPlugin(DevConsoleRegistry.class)
                    .resolveById("chaos");
            JsonObject added = call(console, Map.of("action", "invariant", "routeId", "orders", "id", "not-dead-lettered",
                    "predicate", "${exchangeProperty.CamelExceptionCaught} == null"));
            assertThat(added.get("error")).isNull();
            call(console, Map.of("action", "add", "type", "fail", "pattern", "mock:backend", "calls", "2"));

            ProducerTemplate template = context.createProducerTemplate();
            for (int i = 0; i < 5; i++) {
                template.sendBody("direct:orders", "order-" + i);
            }
            JsonObject report = ChaosManager.get(context).report();
            JsonObject inv = (JsonObject) ((List<?>) report.get("invariants")).get(0);
            assertThat(inv.getLong("checked")).isEqualTo(5);
            assertThat(inv.getLong("violated")).isEqualTo(2);
            assertThat(inv.getString("result")).isEqualTo("FAIL");
            assertThat(report.get("result")).isEqualTo("FAIL");
            assertThat(ChaosManager.get(context).reportText()).contains("Invariants:").contains("violated=2");

            ChaosManager.get(context).reset();
            assertThat(ChaosManager.get(context).report().get("invariants")).isNull();
        } finally {
            context.stop();
        }
    }

    private static JsonObject call(DevConsole console, Map<String, Object> options) {
        return (JsonObject) console.call(DevConsole.MediaType.JSON, new HashMap<>(options));
    }
}
