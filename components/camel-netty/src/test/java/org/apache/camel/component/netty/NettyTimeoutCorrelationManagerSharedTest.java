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
package org.apache.camel.component.netty;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import io.netty.channel.embedded.EmbeddedChannel;
import org.apache.camel.AsyncProducer;
import org.apache.camel.BindToRegistry;
import org.apache.camel.Endpoint;
import org.apache.camel.Exchange;
import org.apache.camel.ExchangePattern;
import org.apache.camel.ExchangeTimedOutException;
import org.apache.camel.support.DefaultExchange;
import org.apache.camel.support.service.ServiceHelper;
import org.apache.camel.test.junit6.CamelTestSupport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A correlation manager bean extending TimeoutCorrelationManagerSupport can be used by several producers (the producers
 * of one endpoint, or of endpoints that refer to the same bean). A producer that stops must not stop it under the
 * others: it is stopped by the last producer that stops.
 */
class NettyTimeoutCorrelationManagerSharedTest extends CamelTestSupport {

    @BindToRegistry("slowManager")
    private final MyCorrelationManager slowManager = new MyCorrelationManager(60000);

    @BindToRegistry("fastManager")
    private final MyCorrelationManager fastManager = new MyCorrelationManager(300);

    private final List<Socket> accepted = new CopyOnWriteArrayList<>();
    private final List<AsyncProducer> producers = new CopyOnWriteArrayList<>();
    private final BlockingQueue<Request> received = new LinkedBlockingQueue<>();
    private ServerSocket server;

    private record Request(String line, Socket socket) {
    }

    private static final class Sent {
        final Exchange exchange;
        final CountDownLatch done = new CountDownLatch(1);

        Sent(Exchange exchange) {
            this.exchange = exchange;
        }
    }

    @AfterEach
    void cleanup() throws Exception {
        for (AsyncProducer producer : producers) {
            ServiceHelper.stopService(producer);
        }
        for (Socket socket : accepted) {
            socket.close();
        }
        if (server != null) {
            server.close();
        }
    }

    @Test
    void testStoppingAndRestartingOneProducerKeepsTheRequestOfTheOther() throws Exception {
        Endpoint endpoint = context.getEndpoint(uri("slowManager"));
        AsyncProducer p1 = start(endpoint.createAsyncProducer());
        AsyncProducer p2 = start(endpoint.createAsyncProducer());

        Sent sent = send(p2, "abc-hello");
        Request request = received.poll(5, TimeUnit.SECONDS);
        assertNotNull(request, "the server should get the request");

        // p1 stops (its route is stopped, for example) and starts again while the request of p2 waits for its reply
        ServiceHelper.stopService(p1);
        ServiceHelper.startService(p1);
        reply(request, "abc-world");

        assertTrue(sent.done.await(10, TimeUnit.SECONDS), "the request should get its reply");
        assertNull(sent.exchange.getException(), "the request should not be completed by the stop of the other producer");
        assertEquals("abc-world", sent.exchange.getMessage().getBody(String.class));
    }

    @Test
    void testRequestOfTheRunningProducerTimesOutAfterTheOtherStopped() throws Exception {
        Endpoint endpoint = context.getEndpoint(uri("fastManager"));
        AsyncProducer p1 = start(endpoint.createAsyncProducer());
        AsyncProducer p2 = start(endpoint.createAsyncProducer());

        ServiceHelper.stopService(p1);
        assertTrue(fastManager.isStarted(), "the manager should still run for p2");

        // the server never replies
        Sent sent = send(p2, "abc-hello");
        assertTrue(sent.done.await(10, TimeUnit.SECONDS), "the request should time out");
        assertInstanceOf(ExchangeTimedOutException.class, sent.exchange.getException());
    }

    @Test
    void testLastProducerThatStopsCompletesThePendingRequests() throws Exception {
        Endpoint endpoint = context.getEndpoint(uri("slowManager"));
        AsyncProducer p1 = start(endpoint.createAsyncProducer());
        AsyncProducer p2 = start(endpoint.createAsyncProducer());

        Sent first = send(p1, "abc-hello");
        Sent second = send(p2, "def-hello");
        assertNotNull(received.poll(5, TimeUnit.SECONDS), "the server should get the first request");
        assertNotNull(received.poll(5, TimeUnit.SECONDS), "the server should get the second request");

        ServiceHelper.stopService(p1);
        assertTrue(slowManager.isStarted(), "the manager should still run for p2");
        ServiceHelper.stopService(p2);

        // the server never replies, and the timeout of the manager is 60 seconds
        assertTrue(first.done.await(10, TimeUnit.SECONDS), "the stop of the last producer should complete the request");
        assertTrue(second.done.await(10, TimeUnit.SECONDS), "the stop of the last producer should complete the request");
        assertInstanceOf(ExchangeTimedOutException.class, first.exchange.getException());
        assertInstanceOf(ExchangeTimedOutException.class, second.exchange.getException());
        assertTrue(slowManager.isStopped(), "the last producer should stop the manager");
    }

    @Test
    void testProducerThatFailsToStartAgainDoesNotStopTheManager() throws Exception {
        try (ServerSocket other = new ServerSocket(0)) {
            // lazyChannelCreation=false: the producer connects when it starts, and fails to start without a server
            AsyncProducer p1 = start(context.getEndpoint("netty:tcp://localhost:" + other.getLocalPort()
                                                         + "?textline=true&sync=true&lazyChannelCreation=false"
                                                         + "&correlationManager=#slowManager")
                    .createAsyncProducer());
            AsyncProducer p2 = start(context.getEndpoint(uri("slowManager")).createAsyncProducer());

            ServiceHelper.stopService(p1);
            other.close();
            assertThrows(Exception.class, () -> ServiceHelper.startService(p1));
            assertTrue(slowManager.isStarted(), "the producer that failed to start should not stop the manager of p2");

            Sent sent = send(p2, "abc-hello");
            Request request = received.poll(5, TimeUnit.SECONDS);
            assertNotNull(request, "the server should get the request");
            reply(request, "abc-world");
            assertTrue(sent.done.await(10, TimeUnit.SECONDS), "the request should get its reply");
            assertNull(sent.exchange.getException());
            assertEquals("abc-world", sent.exchange.getMessage().getBody(String.class));
        }
    }

    @Test
    void testRequestFailsWhenTheManagerIsStopped() throws Exception {
        AsyncProducer producer = start(context.getEndpoint(uri("slowManager")).createAsyncProducer());

        // the bean is stopped by something else than the producer while the producer runs
        ServiceHelper.stopService(slowManager);

        // the server never replies: the request must fail rather than wait for a timeout that never fires
        Sent sent = send(producer, "abc-hello");
        assertTrue(sent.done.await(10, TimeUnit.SECONDS), "the request should fail");
        assertInstanceOf(IllegalStateException.class, sent.exchange.getException());
    }

    @Test
    void testStateFailsWhenTheManagerStopsWhilePutting() {
        // the manager stops after putState checked that it runs, and before the state is in its map: the drain of the
        // stop does not see the state, so putState must fail it rather than leave it in the map of a stopped manager
        StoppingCorrelationManager manager = new StoppingCorrelationManager();
        manager.setCamelContext(context);
        manager.retain();

        Exchange exchange = new DefaultExchange(context, ExchangePattern.InOut);
        exchange.getMessage().setBody("abc-hello");
        NettyCamelState state = new NettyCamelState(doneSync -> {
        }, exchange);
        EmbeddedChannel channel = new EmbeddedChannel();
        try {
            assertThrows(IllegalStateException.class, () -> manager.putState(channel, state));
            assertTrue(manager.isStopped());
            // the producer completes the exchange with the exception of putState
            assertFalse(state.isDone());
        } finally {
            channel.close();
        }
    }

    private String uri(String manager) throws Exception {
        if (server == null) {
            startServer();
        }
        return "netty:tcp://localhost:" + server.getLocalPort() + "?textline=true&sync=true&correlationManager=#" + manager;
    }

    private AsyncProducer start(AsyncProducer producer) {
        producers.add(producer);
        ServiceHelper.startService(producer);
        return producer;
    }

    private Sent send(AsyncProducer producer, String body) {
        Exchange exchange = new DefaultExchange(context, ExchangePattern.InOut);
        exchange.getMessage().setBody(body);
        Sent sent = new Sent(exchange);
        producer.process(exchange, doneSync -> sent.done.countDown());
        return sent;
    }

    private static void reply(Request request, String line) throws Exception {
        OutputStream out = request.socket().getOutputStream();
        out.write((line + "\n").getBytes(StandardCharsets.UTF_8));
        out.flush();
    }

    private void startServer() throws Exception {
        server = new ServerSocket(0);
        Thread thread = new Thread(() -> {
            try {
                while (!server.isClosed()) {
                    Socket socket = server.accept();
                    accepted.add(socket);
                    Thread reader = new Thread(() -> {
                        try {
                            BufferedReader in = new BufferedReader(
                                    new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
                            String line;
                            while ((line = in.readLine()) != null) {
                                received.add(new Request(line, socket));
                            }
                        } catch (Exception e) {
                            // the socket was closed
                        }
                    }, "NettyTimeoutCorrelationManagerSharedTestReader");
                    reader.setDaemon(true);
                    reader.start();
                }
            } catch (Exception e) {
                // the server socket was closed
            }
        }, "NettyTimeoutCorrelationManagerSharedTestServer");
        thread.setDaemon(true);
        thread.start();
    }

    private static class MyCorrelationManager extends TimeoutCorrelationManagerSupport {
        MyCorrelationManager(long timeout) {
            setTimeout(timeout);
            setTimeoutChecker(100);
        }

        @Override
        public String getRequestCorrelationId(Object request) {
            return request.toString().substring(0, 3);
        }

        @Override
        public String getResponseCorrelationId(Object response) {
            return response.toString().substring(0, 3);
        }
    }

    /**
     * Stops itself while it reads the correlation id of a request, which is after putState checked that it runs and
     * before it puts the state in its map.
     */
    private static final class StoppingCorrelationManager extends MyCorrelationManager {
        StoppingCorrelationManager() {
            super(60000);
        }

        @Override
        public String getRequestCorrelationId(Object request) {
            ServiceHelper.stopService(this);
            return super.getRequestCorrelationId(request);
        }
    }
}
