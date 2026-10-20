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
package org.apache.camel.component.activemq6;

import java.io.IOException;
import java.net.URI;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.command.Command;
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.RemoveInfo;
import org.apache.activemq.command.Response;
import org.apache.activemq.command.ShutdownInfo;
import org.apache.activemq.transport.DefaultTransportListener;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportAcceptListener;
import org.apache.activemq.transport.TransportFactory;
import org.apache.activemq.transport.TransportServer;
import org.apache.camel.CamelContext;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.test.junit6.CamelTestSupport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * With only a brokerURL, every endpoint creates its own pooled connection factory, which opens a broker connection. An
 * endpoint that is discarded after use (toD or recipientList with cacheSize=-1, an endpoint evicted from the endpoint
 * cache) must close it; otherwise every such endpoint leaves one broker connection open until the component stops.
 */
class ActiveMQDynamicEndpointConnectionTest extends CamelTestSupport {

    private final FakeBroker broker = new FakeBroker();

    @Override
    protected CamelContext createCamelContext() throws Exception {
        CamelContext context = super.createCamelContext();
        ActiveMQComponent activemq = new ActiveMQComponent();
        activemq.setBrokerURL(broker.start() + "?jms.watchTopicAdvisories=false");
        context.addComponent("activemq", activemq);
        return context;
    }

    @Override
    protected RouteBuilder createRouteBuilder() {
        return new RouteBuilder() {
            @Override
            public void configure() {
                from("direct:start").toD("activemq:queue:${header.queue}", -1);
                from("direct:static").to("activemq:queue:orders");
            }
        };
    }

    @AfterEach
    void stopBroker() throws Exception {
        broker.stop();
    }

    @Test
    void endpointDiscardedAfterUseClosesItsConnection() {
        for (int i = 0; i < 10; i++) {
            template.sendBodyAndHeader("direct:start", "order-" + i, "queue", "orders-" + i);
        }
        assertEquals(10, broker.messages.get(), "all messages sent");
        await().atMost(10, TimeUnit.SECONDS).untilAsserted(
                () -> assertEquals(0, broker.openConnections(), "broker connections left open"));
    }

    @Test
    void endpointKeptInUseKeepsItsConnection() throws Exception {
        for (int i = 0; i < 10; i++) {
            template.sendBody("direct:static", "order-" + i);
        }
        assertEquals(10, broker.messages.get(), "all messages sent");
        assertEquals(1, broker.openConnections(), "one pooled connection for the endpoint");

        // a route that is stopped and started again sends with the same endpoint
        context.getRouteController().stopRoute(context.getRoutes().get(1).getRouteId());
        context.getRouteController().startRoute(context.getRoutes().get(1).getRouteId());
        template.sendBody("direct:static", "order-10");
        assertEquals(11, broker.messages.get(), "all messages sent");
    }

    /**
     * Just enough of an OpenWire broker (the transport server of activemq-client): it answers every command that wants
     * a response, and counts the connections that are open (connection info received, not removed, transport not
     * closed) and the messages received.
     */
    static final class FakeBroker {
        final Set<Object> connections = ConcurrentHashMap.newKeySet();
        final AtomicInteger messages = new AtomicInteger();
        TransportServer server;

        String start() throws Exception {
            server = TransportFactory.bind(new URI("tcp://localhost:0"));
            server.setAcceptListener(new TransportAcceptListener() {
                @Override
                public void onAccept(Transport transport) {
                    try {
                        transport.setTransportListener(new DefaultTransportListener() {
                            @Override
                            public void onCommand(Object o) {
                                Command command = (Command) o;
                                if (command instanceof ConnectionInfo info) {
                                    connections.add(info.getConnectionId());
                                } else if (command instanceof RemoveInfo remove
                                        && remove.getObjectId() instanceof ConnectionId id) {
                                    connections.remove(id);
                                } else if (command instanceof Message) {
                                    messages.incrementAndGet();
                                } else if (command instanceof ShutdownInfo) {
                                    return;
                                }
                                if (command.isResponseRequired()) {
                                    Response response = new Response();
                                    response.setCorrelationId(command.getCommandId());
                                    try {
                                        transport.oneway(response);
                                    } catch (IOException e) {
                                        // the client went away
                                    }
                                }
                            }
                        });
                        transport.start();
                    } catch (Exception e) {
                        throw new IllegalStateException(e);
                    }
                }

                @Override
                public void onAcceptError(Exception error) {
                    // ignore
                }
            });
            server.start();
            return "tcp://localhost:" + server.getSocketAddress().getPort();
        }

        int openConnections() {
            return connections.size();
        }

        void stop() throws Exception {
            if (server != null) {
                server.stop();
            }
        }
    }
}
