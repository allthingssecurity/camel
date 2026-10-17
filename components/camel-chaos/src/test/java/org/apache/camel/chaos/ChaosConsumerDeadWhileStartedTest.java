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

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.camel.CamelContext;
import org.apache.camel.Consumer;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.Producer;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.ServiceStatus;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.health.HealthCheckRegistry;
import org.apache.camel.impl.health.RoutesHealthCheckRepository;
import org.apache.camel.support.DefaultConsumer;
import org.apache.camel.support.DefaultEndpoint;
import org.apache.camel.util.json.JsonObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Proof point (a2), a <b>minimal reproduction</b> (not the real component) of the "consumer dead while the route is
 * Started" class of bugs, with the shape of CAMEL-25679 (camel-mongodb tailable consumer, fix on the fork branch
 * camel-mongodb-consumer-retries-cursor-regeneration, not on main yet): the consumer reads on its own thread; when a
 * read fails it opens a new cursor <i>outside</i> the try of its loop, so a second failure (the server is still down)
 * ends the thread. The route stays Started and its health check stays UP.
 * <p>
 * The "server" is a route calling {@code mock:server}; chaos fails the next 3 calls to it. The report must flag the old
 * shape (expectation FAIL, server route NOT-RECOVERED, while health is UP) and pass the fixed shape.
 */
class ChaosConsumerDeadWhileStartedTest {

    private static final String P = "a2-consumer-dead-while-started";
    private CamelContext context;

    @BeforeAll
    static void title() {
        Proof.start(P, "Proof point (a2): minimal reproduction of the CAMEL-25679 mechanism (consumer thread ends,"
                       + " route stays Started) - ChaosConsumerDeadWhileStartedTest");
    }

    @AfterEach
    void stop() {
        if (context != null) {
            context.stop();
        }
    }

    @Test
    void oldShapeIsFlagged() throws Exception {
        JsonObject report = scenario(false);
        JsonObject expectation = (JsonObject) ((List<?>) report.get("expectations")).get(0);
        JsonObject server = route(report, "server");
        assertThat(context.getRouteController().getRouteStatus("tail")).isEqualTo(ServiceStatus.Started);
        assertThat(((Map<?, ?>) report.get("healthNow")).get("state")).isEqualTo("UP");
        assertThat(expectation.getString("result")).isEqualTo("FAIL");
        assertThat(expectation.getLong("completedExchanges")).isZero();
        assertThat(server.getString("verdict")).startsWith("NOT-RECOVERED");
        assertThat(report.get("result")).isEqualTo("FAIL");
        Proof.log(P, "OLD shape (cursor reopened outside the try): route tail=Started, health="
                     + ((Map<?, ?>) report.get("healthNow")).get("state") + ", expectation " + expectation.toJson()
                     + ", server verdict=" + server.getString("verdict") + " => report result=" + report.get("result"));
    }

    @Test
    void fixedShapePasses() throws Exception {
        JsonObject report = scenario(true);
        JsonObject expectation = (JsonObject) ((List<?>) report.get("expectations")).get(0);
        JsonObject server = route(report, "server");
        assertThat(expectation.getString("result")).isEqualTo("PASS");
        assertThat(server.getString("verdict")).startsWith("RECOVERED (3 request/reply failure(s) returned to the caller)");
        assertThat(report.get("result")).isEqualTo("PASS");
        Proof.log(P, "FIXED shape (reopen inside the try, retried): expectation " + expectation.toJson()
                     + ", server verdict=" + server.getString("verdict") + " => report result=" + report.get("result"));
    }

    private JsonObject scenario(boolean fixed) throws Exception {
        context = ChaosManagerTest.newContext(5, true, new RouteBuilder() {
            @Override
            public void configure() {
                from("direct:server").routeId("server").to("mock:server");
            }
        });
        HealthCheckRegistry.get(context).register(new RoutesHealthCheckRepository());
        TailingEndpoint endpoint = new TailingEndpoint(context, fixed);
        context.addEndpoint("tailing:orders", endpoint);
        context.addRoutes(new RouteBuilder() {
            @Override
            public void configure() {
                from("tailing:orders").routeId("tail").to("mock:out");
            }
        });
        MockEndpoint out = context.getEndpoint("mock:out", MockEndpoint.class);
        await().atMost(5, TimeUnit.SECONDS).until(() -> out.getReceivedCounter() >= 5);

        ChaosManager manager = ChaosManager.get(context);
        ChaosRule rule = manager.addRule(ChaosRule.fail("mock:server", 1.0).calls(3).duration(300).id("server-down"));
        await().atMost(5, TimeUnit.SECONDS).until(() -> "ended".equals(rule.getState()));
        // the server works again: the consumer must deliver again
        manager.expect("tail", 20);
        // give a dead consumer the same time a live one needs (about 20 x 10 ms), with a wide margin
        await().pollDelay(1500, TimeUnit.MILLISECONDS).atMost(5, TimeUnit.SECONDS)
                .until(() -> manager.getExpectations().get(0).isMet() || endpoint.threadEnded);
        if (endpoint.threadEnded) {
            // it will never deliver again: wait the margin anyway, so the comparison with the fixed shape is fair
            await().pollDelay(1, TimeUnit.SECONDS).atMost(2, TimeUnit.SECONDS).until(() -> true);
        }
        Proof.log(P, (fixed ? "fixed" : "old") + " consumer thread ended=" + endpoint.threadEnded + ", text report:"
                     + System.lineSeparator() + manager.reportText());
        return manager.report();
    }

    private static JsonObject route(JsonObject report, String id) {
        return (JsonObject) ((Map<?, ?>) report.get("routes")).get(id);
    }

    /**
     * Stands in for a tailable cursor consumer: one thread, reads through the "server" route.
     */
    static final class TailingEndpoint extends DefaultEndpoint {
        final boolean fixed;
        volatile boolean threadEnded;

        TailingEndpoint(CamelContext context, boolean fixed) {
            this.fixed = fixed;
            setCamelContext(context);
        }

        @Override
        protected String createEndpointUri() {
            return "tailing:orders";
        }

        @Override
        public Producer createProducer() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Consumer createConsumer(Processor processor) {
            return new TailingConsumer(this, processor);
        }
    }

    static final class TailingConsumer extends DefaultConsumer {
        private final TailingEndpoint endpoint;
        private volatile boolean running;
        private Thread thread;

        TailingConsumer(TailingEndpoint endpoint, Processor processor) {
            super(endpoint, processor);
            this.endpoint = endpoint;
        }

        @Override
        protected void doStart() throws Exception {
            super.doStart();
            running = true;
            ProducerTemplate server = getEndpoint().getCamelContext().createProducerTemplate();
            thread = new Thread(() -> {
                try {
                    if (endpoint.fixed) {
                        runFixed(server);
                    } else {
                        runOld(server);
                    }
                } catch (Exception e) {
                    // the old shape: the exception ends the thread; only stderr would show it
                } finally {
                    endpoint.threadEnded = running;
                }
            }, "tailing-consumer");
            thread.setDaemon(true);
            thread.start();
        }

        @Override
        protected void doStop() throws Exception {
            running = false;
            super.doStop();
        }

        /** Before the fix: the new cursor is opened outside the try of the loop. */
        private void runOld(ProducerTemplate server) throws Exception {
            server.requestBody("direct:server", "open");
            while (running) {
                try {
                    readOne(server);
                } catch (Exception e) {
                    pause();
                    server.requestBody("direct:server", "open"); // a failure here ends the thread
                }
            }
        }

        /** After the fix: opening the cursor is retried inside the try. */
        private void runFixed(ProducerTemplate server) {
            boolean open = false;
            while (running) {
                try {
                    if (!open) {
                        server.requestBody("direct:server", "open");
                        open = true;
                    }
                    readOne(server);
                } catch (Exception e) {
                    open = false;
                    pause();
                }
            }
        }

        private void readOne(ProducerTemplate server) throws Exception {
            Object doc = server.requestBody("direct:server", "next");
            Exchange exchange = createExchange(false);
            exchange.getMessage().setBody(doc);
            try {
                getProcessor().process(exchange);
            } finally {
                releaseExchange(exchange, false);
            }
            pause();
        }

        private static void pause() {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
