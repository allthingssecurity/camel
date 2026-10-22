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
package org.apache.camel.component.graphql;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.http.BaseHttpTest;
import org.apache.camel.util.json.JsonObject;
import org.apache.hc.core5.http.ClassicHttpRequest;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.config.Http1Config;
import org.apache.hc.core5.http.impl.bootstrap.HttpServer;
import org.apache.hc.core5.http.impl.bootstrap.ServerBootstrap;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The query and the variables of a GraphQL request travel in its JSON body: the headers named by queryHeader and
 * variablesHeader must not be sent as HTTP headers as well, and the body is sent as application/json whatever
 * Content-Type the message has.
 */
class GraphqlProducerRequestHeadersTest extends BaseHttpTest {

    private static final String RESPONSE = "{\"data\":{}}";

    private final List<ClassicHttpRequest> requests = new CopyOnWriteArrayList<>();
    private final List<String> bodies = new CopyOnWriteArrayList<>();
    private HttpServer localServer;

    @Override
    public void setupResources() throws Exception {
        localServer = ServerBootstrap.bootstrap()
                .setCanonicalHostName("localhost")
                // like the default request header limit of Jetty, Tomcat and Spring Boot (8 KB): the server does
                // not accept a header line longer than this
                .setHttp1Config(Http1Config.custom().setMaxLineLength(8192).build())
                .register("/graphql", (request, response, context) -> {
                    requests.add(request);
                    bodies.add(EntityUtils.toString(request.getEntity(), StandardCharsets.UTF_8));
                    response.setEntity(new StringEntity(RESPONSE, ContentType.APPLICATION_JSON));
                })
                .create();
        localServer.start();
    }

    @Override
    public void cleanupResources() {
        if (localServer != null) {
            localServer.stop();
        }
    }

    @Test
    void queryHeaderIsNotSentAsHttpHeader() {
        Exchange exchange = template.request("direct:queryHeader",
                e -> e.getIn().setHeader("myQuery", "{books{id name}}"));

        assertNull(exchange.getException());
        assertEquals(1, requests.size());
        assertFalse(requests.get(0).containsHeader("myQuery"), "the query must only be sent in the body");
        assertTrue(bodies.get(0).contains("{books{id name}}"), bodies.get(0));
        assertEquals("{books{id name}}", exchange.getMessage().getHeader("myQuery"));
    }

    @Test
    void variablesHeaderIsNotSentAsHttpHeader() {
        JsonObject variables = new JsonObject();
        variables.put("id", "book-1");
        Exchange exchange = template.request("direct:variablesHeader",
                e -> e.getIn().setHeader("myVariables", variables));

        assertNull(exchange.getException());
        assertEquals(1, requests.size());
        assertFalse(requests.get(0).containsHeader("myVariables"), "the variables must only be sent in the body");
        assertTrue(bodies.get(0).contains("\"book-1\""), bodies.get(0));
    }

    @Test
    void queryLongerThanTheServerHeaderLimitIsAnswered() {
        StringBuilder query = new StringBuilder("{books{id name");
        while (query.length() < 10000) {
            query.append(" name");
        }
        query.append("}}");
        Exchange exchange = template.request("direct:queryHeader",
                e -> e.getIn().setHeader("myQuery", query.toString()));

        assertNull(exchange.getException());
        assertEquals(RESPONSE, exchange.getMessage().getBody(String.class));
    }

    @Test
    void bodyIsSentAsJsonWhenTheMessageHasAnotherContentType() {
        // such as the Content-Type of a request received by an HTTP consumer, or of the response of an earlier call
        Exchange exchange = template.request("direct:queryHeader", e -> {
            e.getIn().setHeader("myQuery", "{books{id name}}");
            e.getIn().setHeader(Exchange.CONTENT_TYPE, "text/plain");
        });

        assertNull(exchange.getException());
        assertEquals(1, requests.size());
        assertEquals(ContentType.APPLICATION_JSON.getMimeType(),
                ContentType.parse(requests.get(0).getFirstHeader("Content-Type").getValue()).getMimeType());
    }

    @Override
    protected RouteBuilder createRouteBuilder() {
        return new RouteBuilder() {
            @Override
            public void configure() {
                from("direct:queryHeader")
                        .to("graphql://http://localhost:" + localServer.getLocalPort() + "/graphql?queryHeader=myQuery");

                from("direct:variablesHeader")
                        .to("graphql://http://localhost:" + localServer.getLocalPort()
                            + "/graphql?query={books{id name}}&variablesHeader=myVariables");
            }
        };
    }
}
