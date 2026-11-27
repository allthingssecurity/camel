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
package org.apache.camel.component.stream;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.sun.net.httpserver.HttpServer;
import org.apache.camel.BindToRegistry;
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.spi.ExceptionHandler;
import org.apache.camel.test.junit6.CamelTestSupport;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

/**
 * The httpHeaders, encoding and readTimeout options of kind http.
 */
public class StreamHttpOptionsTest extends CamelTestSupport {

    private static final Map<String, String> REQUEST_HEADERS = new ConcurrentHashMap<>();
    private static final CountDownLatch RELEASE = new CountDownLatch(1);
    private static ExecutorService executor;
    private static HttpServer server;

    @BindToRegistry("recordingHandler")
    private final RecordingExceptionHandler handler = new RecordingExceptionHandler();

    @BeforeAll
    public static void startServer() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/headers", exchange -> {
            exchange.getRequestHeaders().forEach((name, values) -> REQUEST_HEADERS.put(name, values.get(0)));
            byte[] data = "ok\n".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, data.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(data);
            }
        });
        server.createContext("/latin1", exchange -> {
            byte[] data = "café\n".getBytes(StandardCharsets.ISO_8859_1);
            exchange.sendResponseHeaders(200, data.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(data);
            }
        });
        server.createContext("/stall", exchange -> {
            // one line, then no more data until the test is done
            exchange.sendResponseHeaders(200, 0);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write("Hello\n".getBytes(StandardCharsets.UTF_8));
                os.flush();
                RELEASE.await(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        // the stalled request must not hold the other requests
        executor = Executors.newCachedThreadPool();
        server.setExecutor(executor);
        server.start();
    }

    @AfterAll
    public static void stopServer() {
        RELEASE.countDown();
        if (server != null) {
            server.stop(0);
        }
        if (executor != null) {
            executor.shutdownNow();
        }
    }

    @Override
    public boolean isUseRouteBuilder() {
        return false;
    }

    @Test
    public void testHttpHeadersKeepSeparatorsInValues() throws Exception {
        MockEndpoint mock = getMockEndpoint("mock:result");
        mock.expectedBodiesReceived("ok");

        startRoute("/headers", "&httpHeaders=RAW(X-Referer:http://host/path,X-Token=abc==,not-a-pair)");

        mock.assertIsSatisfied();
        assertEquals("http://host/path", REQUEST_HEADERS.get("X-referer"));
        assertEquals("abc==", REQUEST_HEADERS.get("X-token"));
    }

    @Test
    public void testEncodingIsUsed() throws Exception {
        MockEndpoint mock = getMockEndpoint("mock:result");
        mock.expectedBodiesReceived("café");

        startRoute("/latin1", "&encoding=ISO-8859-1");

        mock.assertIsSatisfied();
    }

    @Test
    public void testReadTimeout() throws Exception {
        startRoute("/stall", "&readTimeout=500&exceptionHandler=#recordingHandler");

        await().atMost(10, TimeUnit.SECONDS).until(() -> !handler.exceptions.isEmpty());
        assertInstanceOf(SocketTimeoutException.class, handler.exceptions.get(0));
    }

    private void startRoute(String path, String options) throws Exception {
        String url = "http://127.0.0.1:" + server.getAddress().getPort() + path;
        context.addRoutes(new RouteBuilder() {
            @Override
            public void configure() {
                from("stream:http?httpUrl=" + url + options).to("mock:result");
            }
        });
        context.start();
    }

    private static final class RecordingExceptionHandler implements ExceptionHandler {
        private final List<Throwable> exceptions = new CopyOnWriteArrayList<>();

        @Override
        public void handleException(Throwable exception) {
            exceptions.add(exception);
        }

        @Override
        public void handleException(String message, Throwable exception) {
            exceptions.add(exception);
        }

        @Override
        public void handleException(String message, Exchange exchange, Throwable exception) {
            exceptions.add(exception);
        }
    }
}
