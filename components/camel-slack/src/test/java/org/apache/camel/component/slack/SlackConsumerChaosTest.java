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
package org.apache.camel.component.slack;

import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import com.slack.api.model.Message;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.apache.camel.CamelContext;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.chaos.ChaosManager;
import org.apache.camel.chaos.ChaosRule;
import org.apache.camel.test.junit6.CamelTestSupport;
import org.apache.camel.util.json.JsonObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.RepeatedTest;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Proof point (a1) of the camel-chaos proposal, on the real slack consumer: a chaos stall rule suspends the route while
 * the consumer routes the 2nd message of a poll of 4 (as a route policy or an operator would), and resumes it 500 ms
 * later. The chaos report must say whether all 4 messages were routed.
 * <p>
 * On main (with CAMEL-25684) the report says PASS. On the code before CAMEL-25684 (SlackConsumer.java of 68329f0919bc^)
 * it says FAIL with 2 lost messages: run it with {@code -Dchaos.expect=FAIL} there. The text report is appended to
 * {@code target/proof/a1-slack-CAMEL-25684.txt} (or {@code -Dchaos.proof.dir}).
 */
class SlackConsumerChaosTest extends CamelTestSupport {

    private final List<String> channel = new CopyOnWriteArrayList<>();
    private final List<String> routed = new CopyOnWriteArrayList<>();
    private final AtomicInteger historyRequests = new AtomicInteger();
    private HttpServer server;

    @Override
    protected CamelContext createCamelContext() throws Exception {
        CamelContext context = super.createCamelContext();
        Properties props = new Properties();
        props.put("camel.chaos.enabled", "true");
        props.put("camel.chaos.seed", "25684");
        context.getPropertiesComponent().setOverrideProperties(props);
        return context;
    }

    @Override
    protected void doPreSetup() throws Exception {
        server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        server.createContext("/api/conversations.list", this::conversationsList);
        server.createContext("/api/conversations.history", this::conversationsHistory);
        server.start();
    }

    @AfterEach
    void stopServer() {
        context.stop();
        if (server != null) {
            server.stop(0);
        }
    }

    @Override
    public boolean isUseRouteBuilder() {
        return false;
    }

    @RepeatedTest(5)
    void suspendInTheMiddleOfAPollLosesNoMessage() throws Exception {
        channel.add("m1");
        context.addRoutes(new RouteBuilder() {
            @Override
            public void configure() {
                fromF("slack://general?token=RAW(xoxp-test)&serverUrl=http://localhost:%d&maxResults=10&delay=20",
                        server.getAddress().getPort())
                        .routeId("slack")
                        .process(e -> routed.add(e.getMessage().getBody(Message.class).getText()))
                        .to("mock:sink");
            }
        });
        context.start();
        await().atMost(10, TimeUnit.SECONDS).until(() -> routed.contains("m1"));
        routed.clear();

        ChaosManager chaos = ChaosManager.get(context);
        ChaosRule stall = chaos.addRule(ChaosRule.stall(null, 500).onCall("mock:sink").atCall(2).id("suspend-mid-poll"));
        chaos.expect("slack", 4);
        channel.addAll(List.of("m2", "m3", "m4", "m5"));

        await().atMost(20, TimeUnit.SECONDS).until(() -> "ended".equals(stall.getState()));
        // a few polls after the resume
        int before = historyRequests.get();
        await().atMost(10, TimeUnit.SECONDS).until(() -> historyRequests.get() >= before + 5);

        JsonObject report = chaos.report();
        String expected = System.getProperty("chaos.expect", "PASS");
        log("code: " + System.getProperty("chaos.codeLabel", "main") + ", expected report result " + expected);
        log("routed after the burst: " + routed);
        log(chaos.reportText());
        assertEquals(expected, report.get("result"));
        if ("PASS".equals(expected)) {
            assertEquals(List.of("m2", "m3", "m4", "m5"), routed.stream().sorted().toList());
        } else {
            JsonObject expectation = (JsonObject) ((List<?>) report.get("expectations")).get(0);
            assertTrue(expectation.getLong("lostMessages") > 0, "lost messages are counted");
        }
    }

    private static void log(String text) throws IOException {
        System.out.println(text);
        Path dir = Path.of(System.getProperty("chaos.proof.dir", "target/proof"));
        Files.createDirectories(dir);
        Files.writeString(dir.resolve("a1-slack-CAMEL-25684.txt"), text + System.lineSeparator(),
                StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }

    private void conversationsList(HttpExchange http) throws IOException {
        respond(http, "{\"ok\":true,\"channels\":[{\"id\":\"C1\",\"name\":\"general\"}],"
                      + "\"response_metadata\":{\"next_cursor\":\"\"}}");
    }

    /** Answers as Slack does: the messages after oldest (exclusive), the most recent first, limit at a time. */
    private void conversationsHistory(HttpExchange http) throws IOException {
        historyRequests.incrementAndGet();
        Map<String, String> params = form(http);
        String oldest = params.get("oldest");
        int limit = Integer.parseInt(params.get("limit"));
        List<Integer> range = new ArrayList<>();
        for (int i = 0; i < channel.size(); i++) {
            if (oldest == null || new BigDecimal(ts(i)).compareTo(new BigDecimal(oldest)) > 0) {
                range.add(i);
            }
        }
        range.sort(Comparator.reverseOrder());
        List<Integer> page = range.subList(0, Math.min(limit, range.size()));
        String messages = page.stream()
                .map(i -> "{\"type\":\"message\",\"text\":\"" + channel.get(i) + "\",\"ts\":\"" + ts(i) + "\"}")
                .collect(Collectors.joining(","));
        respond(http, "{\"ok\":true,\"messages\":[" + messages + "],\"has_more\":false,"
                      + "\"response_metadata\":{\"next_cursor\":\"\"}}");
    }

    private static String ts(int index) {
        return (1700000000 + index) + ".000100";
    }

    private static Map<String, String> form(HttpExchange http) throws IOException {
        Map<String, String> answer = new HashMap<>();
        String body = new String(http.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
        for (String pair : body.split("&")) {
            int idx = pair.indexOf('=');
            if (idx > 0) {
                answer.put(URLDecoder.decode(pair.substring(0, idx), StandardCharsets.UTF_8),
                        URLDecoder.decode(pair.substring(idx + 1), StandardCharsets.UTF_8));
            }
        }
        return answer;
    }

    private static void respond(HttpExchange http, String json) throws IOException {
        byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
        http.getResponseHeaders().add("Content-Type", "application/json; charset=utf-8");
        http.sendResponseHeaders(200, bytes.length);
        try (OutputStream os = http.getResponseBody()) {
            os.write(bytes);
        }
    }
}
