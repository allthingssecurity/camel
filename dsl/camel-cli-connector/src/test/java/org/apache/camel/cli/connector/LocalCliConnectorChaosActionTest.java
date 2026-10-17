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
package org.apache.camel.cli.connector;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.camel.CamelContext;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.spi.CliConnectorFactory;
import org.apache.camel.test.junit6.CamelTestSupport;
import org.apache.camel.util.json.JsonObject;
import org.apache.camel.util.json.Jsoner;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * The path Camel CLI ({@code camel cmd chaos}) and the MCP tool ({@code camel_runtime_chaos}) take: an action file with
 * {@code action=chaos} is picked up by the connector, handed to the real {@code chaos} dev console of camel-chaos, and
 * the fault is injected into the running route.
 */
@Isolated
class LocalCliConnectorChaosActionTest extends CamelTestSupport {

    private static final String PID = String.valueOf(ProcessHandle.current().pid());

    @TempDir
    Path home;

    private String oldHome;
    private File dir;
    private LocalCliConnector connector;

    @Override
    protected CamelContext createCamelContext() throws Exception {
        CamelContext context = super.createCamelContext();
        DefaultCliConnectorFactory disabled = new DefaultCliConnectorFactory();
        disabled.setEnabled(false);
        context.getCamelContextExtension().addContextPlugin(CliConnectorFactory.class, disabled);
        Properties props = new Properties();
        props.put("camel.chaos.enabled", "true");
        props.put("camel.chaos.seed", "1");
        context.getPropertiesComponent().setOverrideProperties(props);
        return context;
    }

    @Override
    protected RouteBuilder createRouteBuilder() {
        return new RouteBuilder() {
            @Override
            public void configure() {
                from("direct:orders").routeId("orders").to("mock:backend");
            }
        };
    }

    @BeforeEach
    void useTempHome() {
        oldHome = System.getProperty("user.home");
        System.setProperty("user.home", home.toString());
        dir = home.resolve(".camel").toFile();
    }

    @AfterEach
    void restoreHome() {
        if (connector != null) {
            connector.stop();
        }
        System.setProperty("user.home", oldHome);
    }

    @Test
    void addRuleThroughTheConnectorAndReadTheReport() throws Exception {
        startConnector();

        writeAction(action("chaos", "command", "add", "type", "fail", "pattern", "mock:backend", "calls", "2", "id",
                "cli-rule"), "r1");
        JsonObject added = awaitJson(file(PID + "-output-r1.json"));
        assertThat(added.getString("result")).isEqualTo("added rule cli-rule");

        int failures = 0;
        for (int i = 0; i < 3; i++) {
            try {
                template.sendBody("direct:orders", "order-" + i);
            } catch (Exception e) {
                failures++;
            }
        }
        assertThat(failures).isEqualTo(2);
        assertThat(getMockEndpoint("mock:backend").getReceivedCounter()).isEqualTo(1);

        writeAction(action("chaos"), "r2");
        JsonObject reply = awaitJson(file(PID + "-output-r2.json"));
        JsonObject report = (JsonObject) reply.get("report");
        Map<?, ?> rule = (Map<?, ?>) ((List<?>) report.get("rules")).get(0);
        assertThat(rule.get("id")).isEqualTo("cli-rule");
        assertThat(((Number) rule.get("injected")).longValue()).isEqualTo(2L);
        Map<?, ?> orders = (Map<?, ?>) ((Map<?, ?>) report.get("routes")).get("orders");
        assertThat(((Number) orders.get("failed")).longValue()).isEqualTo(2L);
        assertThat((String) orders.get("verdict")).startsWith("RECOVERED, but 2 exchange(s) failed unhandled");
        MockEndpoint.assertIsSatisfied(context);
    }

    @Test
    void badRuleIsReportedAsFailed() {
        startConnector();

        writeAction(action("chaos", "command", "add", "type", "stall", "routeId", "no-such-route", "duration", "100"),
                "r1");
        JsonObject reply = awaitJson(file(PID + "-output-r1.json"));
        assertThat(reply.getString("status")).isEqualTo("failed");
        assertThat(reply.getString("error")).contains("no-such-route");
    }

    private void startConnector() {
        connector = new LocalCliConnector(new DefaultCliConnectorFactory());
        connector.setCamelContext(context);
        connector.start();
    }

    private File file(String name) {
        return new File(dir, name);
    }

    private static JsonObject action(String name, String... keyValues) {
        JsonObject jo = new JsonObject();
        jo.put("action", name);
        for (int i = 0; i < keyValues.length; i += 2) {
            jo.put(keyValues[i], keyValues[i + 1]);
        }
        return jo;
    }

    private void writeAction(JsonObject action, String requestId) {
        String name = PID + "-action-" + requestId + ".json";
        try {
            Files.createDirectories(dir.toPath());
            Path tmp = dir.toPath().resolve(UUID.randomUUID() + ".tmp");
            Files.writeString(tmp, action.toJson());
            Files.move(tmp, dir.toPath().resolve(name), StandardCopyOption.ATOMIC_MOVE,
                    StandardCopyOption.REPLACE_EXISTING);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static JsonObject awaitJson(File file) {
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> assertThat(file).isNotEmpty());
        return await().atMost(10, TimeUnit.SECONDS).until(() -> readJson(file), jo -> jo != null);
    }

    private static JsonObject readJson(File file) {
        try {
            String text = Files.readString(file.toPath());
            return text.isBlank() ? null : (JsonObject) Jsoner.deserialize(text);
        } catch (Exception e) {
            return null;
        }
    }
}
