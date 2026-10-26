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
package org.apache.camel.impl;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.camel.Consumer;
import org.apache.camel.ContextTestSupport;
import org.apache.camel.Endpoint;
import org.apache.camel.Exchange;
import org.apache.camel.MultipleConsumersSupport;
import org.apache.camel.Processor;
import org.apache.camel.Producer;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.support.DefaultComponent;
import org.apache.camel.support.DefaultConsumer;
import org.apache.camel.support.DefaultEndpoint;
import org.apache.camel.support.DefaultProducer;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Removing a route must not stop the endpoint it consumes from while other routes still use that endpoint: many
 * endpoints close their client (connection, session, cache) when they are stopped.
 */
class RemoveRouteKeepsSharedEndpointTest extends ContextTestSupport {

    @Test
    void removeConsumerRouteKeepsTheEndpointOfTheProducerRoute() throws Exception {
        ClientEndpoint endpoint = context.getEndpoint("client:a", ClientEndpoint.class);

        context.getRouteController().stopRoute("consumerA");
        context.removeRoute("consumerA");

        assertTrue(endpoint.isStarted(), "the producer route still uses the endpoint");
        assertTrue(endpoint.clientOpen);

        // the producer route still sends with the client of the endpoint
        template.sendBody("direct:a", "Hello");
        assertEquals(List.of("Hello"), endpoint.sent);
    }

    @Test
    void removeOneOfTwoConsumerRoutesKeepsTheOther() throws Exception {
        ClientEndpoint endpoint = context.getEndpoint("client:b", ClientEndpoint.class);

        context.getRouteController().stopRoute("consumerB1");
        context.removeRoute("consumerB1");

        assertTrue(endpoint.isStarted(), "the other consumer route still uses the endpoint");

        MockEndpoint b2 = getMockEndpoint("mock:b2");
        b2.expectedBodiesReceived("World");
        endpoint.receive("World");
        b2.assertIsSatisfied();
    }

    @Test
    void removeLastRouteOfTheEndpointStopsAndShutsItDown() throws Exception {
        ClientEndpoint endpoint = context.getEndpoint("client:a", ClientEndpoint.class);

        context.getRouteController().stopRoute("consumerA");
        context.removeRoute("consumerA");
        context.getRouteController().stopRoute("producerA");
        context.removeRoute("producerA");

        // shut down as when the consumer route is removed last, as it is no longer in the endpoint registry and so
        // would never be shut down
        assertTrue(endpoint.isShutdown());
        assertFalse(endpoint.clientOpen);
        assertTrue(context.getEndpoints().stream().noneMatch(e -> e == endpoint));
    }

    @Test
    void removeConsumerRouteThenAddItAgain() throws Exception {
        ClientEndpoint endpoint = context.getEndpoint("client:a", ClientEndpoint.class);

        context.getRouteController().stopRoute("consumerA");
        context.removeRoute("consumerA");
        context.addRoutes(new RouteBuilder() {
            @Override
            public void configure() {
                from("client:a").routeId("consumerA").to("mock:a");
            }
        });

        assertSame(endpoint, context.getRoute("consumerA").getEndpoint());
        MockEndpoint a = getMockEndpoint("mock:a");
        a.expectedBodiesReceived("Again");
        endpoint.receive("Again");
        a.assertIsSatisfied();
        template.sendBody("direct:a", "Hello");
        assertEquals(List.of("Hello"), endpoint.sent);
    }

    @Test
    void removeConsumerRouteThatIsTheOnlyUserStopsAndShutsDownTheEndpoint() throws Exception {
        ClientEndpoint endpoint = context.getEndpoint("client:c", ClientEndpoint.class);

        context.getRouteController().stopRoute("consumerC");
        context.removeRoute("consumerC");

        assertTrue(endpoint.isShutdown());
        assertFalse(endpoint.clientOpen);
    }

    @Test
    void removeRouteStopsItsOwnInstanceOfAnEndpointThatIsNotASingleton() throws Exception {
        // each route gets its own instance of an endpoint that is not a singleton, equal to the others (same uri)
        ClientEndpoint d1 = (ClientEndpoint) context.getRoute("consumerD1").getEndpoint();
        ClientEndpoint d2 = (ClientEndpoint) context.getRoute("consumerD2").getEndpoint();
        assertNotSame(d1, d2);

        context.getRouteController().stopRoute("consumerD1");
        context.removeRoute("consumerD1");

        assertFalse(d1.clientOpen, "the endpoint of the removed route is stopped");
        assertTrue(d2.clientOpen, "the endpoint of the other route is not");
    }

    @Override
    protected RouteBuilder createRouteBuilder() {
        return new RouteBuilder() {
            @Override
            public void configure() {
                context.addComponent("client", new ClientComponent());

                from("client:a").routeId("consumerA").to("mock:a");
                from("direct:a").routeId("producerA").to("client:a");

                from("client:b").routeId("consumerB1").to("mock:b1");
                from("client:b").routeId("consumerB2").to("mock:b2");

                from("client:c").routeId("consumerC").to("mock:c");

                from("client:d?prototype=true").routeId("consumerD1").to("mock:d1");
                from("client:d?prototype=true").routeId("consumerD2").to("mock:d2");
            }
        };
    }

    private static final class ClientComponent extends DefaultComponent {
        @Override
        protected Endpoint createEndpoint(String uri, String remaining, Map<String, Object> parameters) {
            boolean prototype = "true".equals(parameters.remove("prototype"));
            return new ClientEndpoint(uri, this, !prototype);
        }
    }

    /**
     * An endpoint that opens a client when it starts and closes it when it stops, as endpoints that hold a connection
     * do; its producers and consumers use that client.
     */
    private static final class ClientEndpoint extends DefaultEndpoint implements MultipleConsumersSupport {
        private final List<Consumer> consumers = new CopyOnWriteArrayList<>();
        private final List<Object> sent = new CopyOnWriteArrayList<>();
        private final boolean singleton;
        private volatile boolean clientOpen;

        ClientEndpoint(String uri, ClientComponent component, boolean singleton) {
            super(uri, component);
            this.singleton = singleton;
        }

        @Override
        public boolean isSingleton() {
            return singleton;
        }

        @Override
        public boolean isMultipleConsumersSupported() {
            return true;
        }

        @Override
        public Producer createProducer() {
            return new DefaultProducer(this) {
                @Override
                public void process(Exchange exchange) {
                    checkClientOpen();
                    sent.add(exchange.getIn().getBody());
                }
            };
        }

        @Override
        public Consumer createConsumer(Processor processor) throws Exception {
            DefaultConsumer consumer = new DefaultConsumer(this, processor) {
                @Override
                protected void doStart() throws Exception {
                    super.doStart();
                    consumers.add(this);
                }

                @Override
                protected void doStop() throws Exception {
                    consumers.remove(this);
                    super.doStop();
                }
            };
            configureConsumer(consumer);
            return consumer;
        }

        void receive(Object body) throws Exception {
            checkClientOpen();
            for (Consumer consumer : consumers) {
                Exchange exchange = consumer.createExchange(false);
                exchange.getIn().setBody(body);
                consumer.getProcessor().process(exchange);
                consumer.releaseExchange(exchange, false);
            }
        }

        private void checkClientOpen() {
            if (!clientOpen) {
                throw new IllegalStateException("client is closed");
            }
        }

        @Override
        protected void doStart() throws Exception {
            super.doStart();
            clientOpen = true;
        }

        @Override
        protected void doStop() throws Exception {
            clientOpen = false;
            super.doStop();
        }
    }
}
