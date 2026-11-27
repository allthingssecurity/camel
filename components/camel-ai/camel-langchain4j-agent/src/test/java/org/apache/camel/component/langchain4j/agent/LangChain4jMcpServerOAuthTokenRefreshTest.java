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
package org.apache.camel.component.langchain4j.agent;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import dev.langchain4j.mcp.client.McpClient;
import org.apache.camel.test.junit6.CamelTestSupport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * With oauthProfile, the MCP client of an HTTP server asks the profile for the token on each request, so a token that
 * expired after the producer started is replaced.
 */
class LangChain4jMcpServerOAuthTokenRefreshTest extends CamelTestSupport {

    private final ObjectMapper mapper = new ObjectMapper();
    private final List<String> authorizations = new CopyOnWriteArrayList<>();
    private HttpServer server;

    @BeforeEach
    void startMcpServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        server.createContext("/mcp", this::handle);
        server.start();
    }

    @AfterEach
    void stopMcpServer() {
        TestOAuthClientAuthenticationFactory.reset();
        server.stop(0);
    }

    @Test
    void eachRequestAsksTheProfileForTheToken() throws Exception {
        TestOAuthClientAuthenticationFactory.token("first");

        LangChain4jMcpServerDefinition definition = new LangChain4jMcpServerDefinition();
        definition.setServerName("stub");
        definition.setTransportType("http");
        definition.setUrl("http://localhost:" + server.getAddress().getPort() + "/mcp");
        definition.setOauthProfile("mcp");

        McpClient client = definition.buildClient(context);
        try {
            // building the client initialized the session with the first token
            assertThat(authorizations).isNotEmpty().allMatch("Bearer first"::equals);

            // the first token expired, and the profile gives another one
            TestOAuthClientAuthenticationFactory.token("second");
            client.listTools();

            assertThat(authorizations.get(authorizations.size() - 1)).isEqualTo("Bearer second");
        } finally {
            client.close();
        }
    }

    /**
     * A minimal MCP server of the Streamable HTTP transport: it answers initialize, tools/list and any other request
     * with a JSON response, and accepts notifications.
     */
    private void handle(HttpExchange exchange) throws IOException {
        try (exchange) {
            if (!"POST".equals(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(405, -1);
                return;
            }
            authorizations.add(exchange.getRequestHeaders().getFirst("Authorization"));
            JsonNode request;
            try (InputStream in = exchange.getRequestBody()) {
                request = mapper.readTree(in);
            }
            if (!request.has("id")) {
                exchange.sendResponseHeaders(202, -1);
                return;
            }
            ObjectNode response = mapper.createObjectNode();
            response.put("jsonrpc", "2.0");
            response.set("id", request.get("id"));
            ObjectNode result = response.putObject("result");
            String method = request.path("method").asText();
            if ("initialize".equals(method)) {
                result.put("protocolVersion", request.path("params").path("protocolVersion").asText("2025-06-18"));
                result.putObject("capabilities").putObject("tools");
                result.putObject("serverInfo").put("name", "stub").put("version", "1.0");
            } else if ("tools/list".equals(method)) {
                result.putArray("tools");
            }
            byte[] body = mapper.writeValueAsBytes(response);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, body.length);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(body);
            }
        }
    }
}
