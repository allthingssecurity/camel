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
package org.apache.camel.support.component;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.camel.CamelContext;
import org.apache.camel.Component;
import org.apache.camel.Exchange;
import org.apache.camel.Producer;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.RuntimeCamelException;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.component.AbstractApiProducerTest.TestApiComponent;
import org.apache.camel.support.component.AbstractApiProducerTest.TestApiEndpoint;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The thread pool of an API endpoint belongs to the CamelContext of the endpoint: stopping another CamelContext does
 * not shut down the pool that the producers of this CamelContext use.
 */
public class AbstractApiEndpointExecutorServiceTest {

    private CamelContext context1;
    private CamelContext context2;

    @AfterEach
    public void stopContexts() {
        if (context1 != null) {
            context1.stop();
        }
        if (context2 != null) {
            context2.stop();
        }
    }

    @Test
    public void testStoppingAnotherCamelContextDoesNotStopThePool() throws Exception {
        context1 = new DefaultCamelContext();
        context1.start();
        context2 = new DefaultCamelContext();
        context2.start();

        TestApiEndpoint endpoint1 = createEndpoint(context1);
        TestApiEndpoint endpoint2 = createEndpoint(context2);

        // the first CamelContext to call the API creates the pool, the second one uses it too
        assertGreeting(context1, endpoint1);
        assertGreeting(context2, endpoint2);

        context1.stop();

        // the second CamelContext is still running, so its producer must still work
        assertGreeting(context2, endpoint2);
    }

    @Test
    public void testStoppingAnotherCamelContextDoesNotInterruptItsCalls() throws Exception {
        context1 = new DefaultCamelContext();
        // do not wait long for the tasks still running in the pools when the CamelContext stops
        context1.getExecutorServiceManager().setShutdownAwaitTermination(100);
        context1.start();
        context2 = new DefaultCamelContext();
        context2.start();

        BlockingApiEndpoint endpoint1 = new BlockingApiEndpoint(new TestApiComponent(context1));
        BlockingApiEndpoint endpoint2 = new BlockingApiEndpoint(new TestApiComponent(context2));
        assertGreeting(context1, endpoint1);

        // a call of the second CamelContext is running when the first CamelContext stops
        endpoint2.block = new CountDownLatch(1);
        try (ProducerTemplate template = context2.createProducerTemplate()) {
            CompletableFuture<Exchange> future = template.asyncSend(endpoint2, e -> e.getMessage().setBody("World"));
            assertTrue(endpoint2.running.await(10, TimeUnit.SECONDS), "The call should be running");

            context1.stop();

            endpoint2.block.countDown();
            assertCompleted(future);
        }
    }

    private static TestApiEndpoint createEndpoint(CamelContext context) throws Exception {
        TestApiComponent component = new TestApiComponent(context);
        TestApiEndpoint endpoint = new TestApiEndpoint("test-api:greetMe", component, "greetMe");
        endpoint.setInBody("name");
        endpoint.configureProperties(new HashMap<>());
        return endpoint;
    }

    private static void assertGreeting(CamelContext context, TestApiEndpoint endpoint) throws Exception {
        try (ProducerTemplate template = context.createProducerTemplate()) {
            // a call that is lost never completes, so do not wait for good
            assertCompleted(template.asyncSend(endpoint, e -> e.getMessage().setBody("World")));
        }
    }

    private static void assertCompleted(CompletableFuture<Exchange> future) throws Exception {
        Exchange out;
        try {
            out = future.get(10, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            throw new AssertionError("The call did not complete", e);
        }
        assertNull(out.getException(), "Should not fail: " + out.getException());
        assertEquals("Greetings World", out.getMessage().getBody());
    }

    /**
     * An API endpoint whose calls wait until they are released.
     */
    static class BlockingApiEndpoint extends TestApiEndpoint {

        final CountDownLatch running = new CountDownLatch(1);
        volatile CountDownLatch block;

        BlockingApiEndpoint(Component component) throws Exception {
            super("test-api:greetMe", component, "greetMe");
            setInBody("name");
            configureProperties(new HashMap<>());
        }

        @Override
        public Producer createProducer() {
            return new AbstractApiProducer<>(this, getPropertiesHelper()) {
                @Override
                protected Object doInvokeMethod(ApiMethod method, Map<String, Object> properties) {
                    CountDownLatch latch = block;
                    if (latch != null) {
                        running.countDown();
                        try {
                            latch.await();
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new RuntimeCamelException("The call was interrupted", e);
                        }
                    }
                    return super.doInvokeMethod(method, properties);
                }
            };
        }
    }
}
