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
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.camel.CamelContext;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.util.json.JsonObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * A stall triggered in the middle of a batch: the route of a file consumer is suspended while it routes the second file
 * of a poll of six. The file consumer reads the files it skipped at the next poll after the resume, so the expectation
 * (6 files) passes. The same rule on the slack consumer before CAMEL-25684 loses the rest of the poll (see
 * SlackConsumerChaosTest in camel-slack).
 */
class ChaosMidFlightStallTest {

    @TempDir
    Path dir;

    private CamelContext context;

    @AfterEach
    void stop() {
        if (context != null) {
            context.stop();
        }
    }

    @Test
    void fileConsumerSuspendedMidBatchDeliversEverything() throws Exception {
        for (int i = 1; i <= 6; i++) {
            Files.writeString(dir.resolve("f" + i + ".txt"), "file " + i);
        }
        context = ChaosManagerTest.newContext(3, true, new RouteBuilder() {
            @Override
            public void configure() {
                // no route yet
            }
        });
        ChaosManager manager = ChaosManager.get(context);
        ChaosRule stall = manager.addRule(ChaosRule.stall(null, 300).onCall("mock:sink").atCall(2).id("mid-batch"));
        context.addRoutes(new RouteBuilder() {
            @Override
            public void configure() {
                from("file:" + dir + "?noop=true&delay=50&sortBy=file:name").routeId("files").to("mock:sink");
            }
        });
        manager.expect("files", 6);

        MockEndpoint sink = context.getEndpoint("mock:sink", MockEndpoint.class);
        await().atMost(10, TimeUnit.SECONDS).until(() -> "ended".equals(stall.getState()));
        await().atMost(10, TimeUnit.SECONDS).until(() -> sink.getReceivedCounter() >= 6);

        JsonObject report = manager.report();
        JsonObject rule = (JsonObject) ((List<?>) report.get("rules")).get(0);
        assertThat(rule.getLong("injected")).isEqualTo(1);
        assertThat(rule.get("healthDuring")).isNotNull();
        assertThat(manager.getExpectations().get(0).isMet()).isTrue();
        assertThat(report.get("result")).isEqualTo("PASS");
        assertThat(report.get("timeline").toString()).contains("route-stalled").contains("route-resumed");
    }
}
