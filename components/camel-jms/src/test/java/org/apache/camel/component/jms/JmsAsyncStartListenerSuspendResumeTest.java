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
package org.apache.camel.component.jms;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSException;

import org.apache.camel.CamelContext;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.RuntimeCamelException;
import org.apache.camel.ServiceStatus;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.test.infra.core.CamelContextExtension;
import org.apache.camel.test.infra.core.DefaultCamelContextExtension;
import org.apache.camel.util.function.ThrowingRunnable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.springframework.jms.listener.DefaultMessageListenerContainer;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Suspending and resuming a JMS consumer with asyncStartListener=true (as route policies, JMX and the route controller
 * do) while the asynchronous start of its listener container is pending, or after it failed, must leave a suspended
 * consumer idle and a resumed consumer with one listener container, initialized once and counted once.
 * <p/>
 * The tasks the consumer submits to the component's async start/stop thread pool are held, and the test runs them
 * itself, so the interleaving is deterministic.
 */
@TestInstance(TestInstance.Lifecycle.PER_METHOD)
@Timeout(60)
class JmsAsyncStartListenerSuspendResumeTest extends AbstractJMSTest {

    @Order(2)
    @RegisterExtension
    public static CamelContextExtension camelContextExtension = new DefaultCamelContextExtension();

    private static final String QUEUE = "JmsAsyncStartListenerSuspendResumeTest";

    // static: the CamelContext (and so the component) is created once for the class
    private static final JmsAsyncStopListenerRestartTest.HeldExecutorService HELD
            = new JmsAsyncStopListenerRestartTest.HeldExecutorService();
    // when true every connection the consumer creates fails, as if the broker was unreachable
    private static volatile boolean brokerDown;
    private volatile ThrowingRunnable<Exception> initializeHook;
    private CamelContext context;
    private ProducerTemplate template;
    private int listenersBefore;

    @Test
    void testSuspendWhileAsyncStartIsPending() throws Exception {
        startRoute();
        assertEquals(1, HELD.pending(), "the asynchronous start is pending");
        context.getRouteController().suspendRoute("jms");

        HELD.runPending();

        assertSuspendedAndIdle();
        resumeAndAssertConsumes();
    }

    @Test
    void testSuspendWhileAsyncStartIsInitializingTheListenerContainer() throws Exception {
        useListenerContainerWithInitializeHook();
        startRoute();
        // the route is suspended after the start task checked that the consumer is not suspended, and before the task
        // starts the listener container
        initializeHook = () -> context.getRouteController().suspendRoute("jms");

        HELD.runPending();

        assertSuspendedAndIdle();
        resumeAndAssertConsumes();
    }

    @Test
    void testResumeWhileAsyncStartIsPending() throws Exception {
        startRoute();
        context.getRouteController().suspendRoute("jms");
        context.getRouteController().resumeRoute("jms");
        assertEquals(1, HELD.pending(), "the resume must not submit a second start of the same listener container");

        HELD.runPending();

        assertOneListenerContainerConsumes();
        assertStopReleasesTheListener();
    }

    @Test
    void testResumeWhileSkippedAsyncStartIsInitializingTheListenerContainer() throws Exception {
        useListenerContainerWithInitializeHook();
        startRoute();
        context.getRouteController().suspendRoute("jms");
        // the start task saw the suspended consumer and does not start the listener container; the route is resumed
        // before the task finished
        initializeHook = () -> context.getRouteController().resumeRoute("jms");

        HELD.runPending();

        assertEquals(0, HELD.pending(), "the resume must not submit a second start of the same listener container");
        assertOneListenerContainerConsumes();
        assertStopReleasesTheListener();
    }

    @Test
    void testResumeWhileAsyncStartIsPendingOnTopicProcessesEachMessageOnce() throws Exception {
        topicEndpoint().getConfiguration().setAsyncStartListener(true);
        context.getRouteController().startRoute("topic");
        context.getRouteController().suspendRoute("topic");
        context.getRouteController().resumeRoute("topic");
        HELD.runPending();

        MockEndpoint topic = getMockEndpoint("mock:topic");
        topic.reset();
        AtomicInteger sent = new AtomicInteger();
        // the subscriptions are not durable: publish until the consumer received a few messages, so it subscribed
        await().atMost(20, TimeUnit.SECONDS).pollInterval(200, TimeUnit.MILLISECONDS).until(() -> {
            template.sendBody("activemq:topic:" + QUEUE, "m" + sent.incrementAndGet());
            return countByBody(topic).size() >= 5;
        });
        template.sendBody("activemq:topic:" + QUEUE, "last");
        await().atMost(10, TimeUnit.SECONDS).until(() -> countByBody(topic).containsKey("last"));
        await().atMost(5, TimeUnit.SECONDS).during(1, TimeUnit.SECONDS).untilAsserted(
                () -> assertTrue(countByBody(topic).values().stream().allMatch(c -> c == 1),
                        "a topic consumer with concurrentConsumers=1 must process each message once: "
                                                                                             + countByBody(topic)));

        DefaultMessageListenerContainer container = (DefaultMessageListenerContainer) ((JmsConsumer) context
                .getRoute("topic").getConsumer()).getListenerContainer();
        assertEquals(container.getConcurrentConsumers(), container.getScheduledConsumerCount(),
                "the listener container must run concurrentConsumers invokers, not one set per start task");
    }

    @Test
    void testSuspendAndResumeAfterFailedAsyncStart() throws Exception {
        endpoint().getConfiguration().setTestConnectionOnStartup(true);
        startRoute();
        brokerDown = true;
        try {
            // the start task logs the failed connection test and returns
            HELD.runPending();
        } finally {
            brokerDown = false;
        }
        assertFalse(consumer().getListenerContainer().isRunning(), "the asynchronous start failed");

        context.getRouteController().suspendRoute("jms");
        context.getRouteController().resumeRoute("jms");
        assertEquals(1, HELD.pending(), "the resume starts the listener container again");
        HELD.runPending();

        assertOneListenerContainerConsumes();
        assertStopReleasesTheListener();
    }

    @Test
    void testSuspendAndResumeWithSynchronousStart() throws Exception {
        // control: asyncStartListener=false is not affected by the asynchronous start
        endpoint().getConfiguration().setAsyncStartListener(false);
        listenersBefore = endpoint().getRunningMessageListeners();
        context.getRouteController().startRoute("jms");
        assertEquals(0, HELD.pending(), "the listener container is started synchronously");
        context.getRouteController().suspendRoute("jms");
        assertSuspendedAndIdle();

        resumeAndAssertConsumes();
        assertStopReleasesTheListener();
    }

    private void startRoute() throws Exception {
        endpoint().getConfiguration().setAsyncStartListener(true);
        listenersBefore = endpoint().getRunningMessageListeners();
        context.getRouteController().startRoute("jms");
    }

    private void useListenerContainerWithInitializeHook() {
        // the listener containers run the hook (if any) when they are initialized
        endpoint().getConfiguration().setMessageListenerContainerFactory(e -> new DefaultJmsMessageListenerContainer(e) {
            @Override
            public void afterPropertiesSet() {
                ThrowingRunnable<Exception> hook = initializeHook;
                initializeHook = null;
                if (hook != null) {
                    try {
                        hook.run();
                    } catch (Exception e) {
                        throw new RuntimeCamelException(e);
                    }
                }
                super.afterPropertiesSet();
            }
        });
        endpoint().getConfiguration().setConsumerType(ConsumerType.Custom);
    }

    private void assertSuspendedAndIdle() throws Exception {
        assertEquals(ServiceStatus.Suspended, context.getRouteController().getRouteStatus("jms"));
        assertFalse(consumer().getListenerContainer().isRunning(),
                "a suspended consumer must not have a running listener container");
        MockEndpoint result = getMockEndpoint("mock:result");
        result.reset();
        result.expectedMessageCount(0);
        result.setAssertPeriod(1000);
        template.sendBody("activemq:queue:" + QUEUE, "sent while suspended");
        result.assertIsSatisfied();
    }

    private void resumeAndAssertConsumes() throws Exception {
        MockEndpoint result = getMockEndpoint("mock:result");
        result.reset();
        result.expectedBodiesReceived("sent while suspended");
        context.getRouteController().resumeRoute("jms");
        result.assertIsSatisfied();
        assertOneListenerContainerConsumes();
    }

    private void assertOneListenerContainerConsumes() throws Exception {
        assertEquals(ServiceStatus.Started, context.getRouteController().getRouteStatus("jms"));
        DefaultMessageListenerContainer container = (DefaultMessageListenerContainer) consumer().getListenerContainer();
        assertTrue(container.isRunning(), "the resumed consumer must have a running listener container");
        assertEquals(container.getConcurrentConsumers(), container.getScheduledConsumerCount(),
                "the listener container must run concurrentConsumers invokers, not one set per start task");
        assertEquals(1, endpoint().getRunningMessageListeners() - listenersBefore,
                "one listener container is counted once on the endpoint");

        MockEndpoint result = getMockEndpoint("mock:result");
        result.reset();
        result.expectedBodiesReceived("Hello World");
        template.sendBody("activemq:queue:" + QUEUE, "Hello World");
        result.assertIsSatisfied();
    }

    private void assertStopReleasesTheListener() throws Exception {
        context.getRouteController().stopRoute("jms");
        HELD.runPending();
        assertEquals(listenersBefore, endpoint().getRunningMessageListeners(),
                "a stopped route leaves no running message listener counted on the endpoint");
    }

    private static Map<Object, Long> countByBody(MockEndpoint mock) {
        return mock.getReceivedExchanges().stream()
                .collect(Collectors.groupingBy(e -> e.getMessage().getBody(), Collectors.counting()));
    }

    private JmsEndpoint topicEndpoint() {
        return context.getEndpoint("activemq:topic:" + QUEUE, JmsEndpoint.class);
    }

    private JmsEndpoint endpoint() {
        return context.getEndpoint("activemq:queue:" + QUEUE, JmsEndpoint.class);
    }

    private JmsConsumer consumer() {
        return (JmsConsumer) context.getRoute("jms").getConsumer();
    }

    @Override
    protected String getComponentName() {
        return "activemq";
    }

    @Override
    protected JmsComponent buildComponent(ConnectionFactory connectionFactory) {
        ConnectionFactory failing = (ConnectionFactory) Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[] { ConnectionFactory.class }, (proxy, method, args) -> {
                    if (brokerDown && method.getName().startsWith("create")) {
                        throw new JMSException("The broker is down (test)");
                    }
                    try {
                        return method.invoke(connectionFactory, args);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
        JmsComponent jms = new JmsComponent() {
            @Override
            protected ExecutorService getAsyncStartStopExecutorService() {
                return HELD;
            }
        };
        jms.getConfiguration().setConnectionFactory(failing);
        return jms;
    }

    @Override
    protected RouteBuilder createRouteBuilder() {
        return new RouteBuilder() {
            public void configure() {
                from("activemq:queue:" + QUEUE).routeId("jms").autoStartup(false).to("mock:result");
                from("activemq:topic:" + QUEUE).routeId("topic").autoStartup(false).to("mock:topic");
            }
        };
    }

    @Override
    public CamelContextExtension getCamelContextExtension() {
        return camelContextExtension;
    }

    @BeforeEach
    void setUpRequirements() {
        context = camelContextExtension.getContext();
        template = camelContextExtension.getProducerTemplate();
        HELD.hold = true;
        brokerDown = false;
        initializeHook = null;
    }

    @AfterEach
    void stopRoutes() throws Exception {
        // run what is left, and stop the routes with the tasks running inline, for the next test
        HELD.hold = false;
        HELD.runPending();
        context.getRouteController().stopRoute("jms");
        context.getRouteController().stopRoute("topic");
        for (JmsEndpoint endpoint : new JmsEndpoint[] { endpoint(), topicEndpoint() }) {
            endpoint.getConfiguration().setAsyncStartListener(false);
            endpoint.getConfiguration().setTestConnectionOnStartup(false);
            endpoint.getConfiguration().setMessageListenerContainerFactory(null);
            endpoint.getConfiguration().setConsumerType(ConsumerType.Default);
        }
        // drop what a failed test left in the queue, so it does not reach the next test
        while (camelContextExtension.getConsumerTemplate().receiveBody("activemq:queue:" + QUEUE, 100) != null) {
            // drained
        }
    }
}
