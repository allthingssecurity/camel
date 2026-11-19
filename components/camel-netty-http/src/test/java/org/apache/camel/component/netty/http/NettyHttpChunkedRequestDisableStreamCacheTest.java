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
package org.apache.camel.component.netty.http;

import java.io.InputStream;
import java.util.concurrent.TimeUnit;

import org.apache.camel.StreamCache;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.component.netty.http.RawHttpClient.Response;
import org.apache.camel.test.AvailablePortFinder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;

import static org.apache.camel.component.netty.http.RawHttpClient.chunkedPost;
import static org.apache.camel.component.netty.http.RawHttpClient.get;
import static org.apache.camel.component.netty.http.RawHttpClient.send;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * With disableStreamCache=true, the server of a netty-http endpoint streams the body of a chunked request to the route.
 * The request is answered whether the route reads the body or not, and whether a route matches or not, and the
 * connection can be used for the next request. The server has one I/O thread, so a request that is not answered would
 * also hold the requests after it.
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS)
class NettyHttpChunkedRequestDisableStreamCacheTest extends BaseNettyTestSupport {

    // 200 chunks of 1000 bytes: more than the server keeps waiting to be read
    private static final int CHUNKS = 200;

    @RegisterExtension
    AvailablePortFinder.Port port2 = AvailablePortFinder.find();

    @Test
    void testChunkedRequestRouteReadsBody() throws Exception {
        MockEndpoint mock = getMockEndpoint("mock:read");
        mock.expectedMessageCount(2);
        mock.allMessages().header("streamed").isEqualTo(true);

        try (RawHttpClient client = new RawHttpClient(getPort())) {
            // two requests on the same connection
            for (int i = 0; i < 2; i++) {
                Response response = client.send(chunkedPost("/read", CHUNKS));
                assertEquals("HTTP/1.1 200 OK", response.status());
                assertEquals(String.valueOf(CHUNKS * 1000), response.body());
            }
        }

        MockEndpoint.assertIsSatisfied(context);
    }

    @Test
    void testChunkedRequestRouteDoesNotReadBody() throws Exception {
        try (RawHttpClient client = new RawHttpClient(getPort())) {
            Response response = client.send(chunkedPost("/noread", CHUNKS));
            assertEquals("HTTP/1.1 200 OK", response.status());
            assertEquals("ok", response.body());

            // the rest of the body is discarded, and the next request on the connection is answered
            assertEquals("HTTP/1.1 200 OK", client.send(get("/hello")).status());
        }

        assertEquals("HTTP/1.1 200 OK", send(getPort(), get("/hello")).status());
    }

    @Test
    void testChunkedRequestUnknownPath() throws Exception {
        assertEquals("HTTP/1.1 404 Not Found", send(getPort(), chunkedPost("/unknown", 4)).status());

        assertEquals("HTTP/1.1 200 OK", send(getPort(), get("/hello")).status());
    }

    @Test
    void testChunkedRequestWithoutExecutorService() throws Exception {
        Response response = send(port2.getPort(), chunkedPost("/read", 4));
        assertEquals("HTTP/1.1 200 OK", response.status());
        assertEquals("4000", response.body());

        assertEquals("HTTP/1.1 200 OK", send(port2.getPort(), get("/hello")).status());
    }

    @Override
    protected RouteBuilder createRouteBuilder() {
        return new RouteBuilder() {
            @Override
            public void configure() {
                String options = "?disableStreamCache=true&workerCount=1";

                // the routes do not use stream caching, which would read the body before the route
                from("netty-http:http://localhost:" + getPort() + "/read" + options)
                        .noStreamCaching()
                        .process(e -> e.getMessage().setHeader("streamed",
                                e.getMessage().getBody() instanceof InputStream
                                        && !(e.getMessage().getBody() instanceof StreamCache)))
                        .to("mock:read")
                        .convertBodyTo(String.class)
                        .setBody(simple("${body.length()}"));

                from("netty-http:http://localhost:" + getPort() + "/noread" + options)
                        .noStreamCaching()
                        .transform().constant("ok");

                from("netty-http:http://localhost:" + getPort() + "/hello" + options)
                        .transform().constant("hello");

                // the routes run on the I/O thread
                String noExecutor = options + "&usingExecutorService=false";

                from("netty-http:http://localhost:" + port2.getPort() + "/read" + noExecutor)
                        .convertBodyTo(String.class)
                        .setBody(simple("${body.length()}"));

                from("netty-http:http://localhost:" + port2.getPort() + "/hello" + noExecutor)
                        .transform().constant("hello");
            }
        };
    }
}
