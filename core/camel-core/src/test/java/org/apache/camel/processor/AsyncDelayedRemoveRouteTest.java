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
package org.apache.camel.processor;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.camel.ContextTestSupport;
import org.apache.camel.Exchange;
import org.apache.camel.RouteStoppingException;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.builder.ThreadPoolProfileBuilder;
import org.apache.camel.spi.ThreadPoolProfile;
import org.junit.jupiter.api.Test;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * An exchange that waits in the thread pool of a Delay, Throttle (asyncDelayed) or Threads EIP when its route is
 * stopped after the shutdown timeout and then removed must be completed (failed), not dropped with the thread pool.
 */
public class AsyncDelayedRemoveRouteTest extends ContextTestSupport {

    private final CountDownLatch latch = new CountDownLatch(1);

    @Override
    public boolean isUseRouteBuilder() {
        return false;
    }

    @Test
    public void testDelayRemoveRoute() throws Exception {
        context.addRoutes(new RouteBuilder() {
            @Override
            public void configure() {
                from("direct:delay").routeId("delay").delay(60000).asyncDelayed().to("mock:result");
            }
        });
        context.start();
        assertExchangeCompletedOnRemove("direct:delay", "delay");
    }

    @Test
    public void testThrottleRemoveRoute() throws Exception {
        context.addRoutes(new RouteBuilder() {
            @Override
            public void configure() {
                from("direct:throttle").routeId("throttle")
                        .throttle(1).totalRequestsMode().timePeriodMillis(60000).asyncDelayed()
                        .to("mock:result");
            }
        });
        context.start();
        // the first exchange takes the only permit of the period
        template.sendBody("direct:throttle", "first");
        assertExchangeCompletedOnRemove("direct:throttle", "throttle");
    }

    @Test
    public void testConcurrentThrottleRemoveRoute() throws Exception {
        // one thread in the thread pool of the throttler
        ThreadPoolProfile one = new ThreadPoolProfileBuilder("one").poolSize(1).maxPoolSize(1).build();
        context.getExecutorServiceManager().registerThreadPoolProfile(one);
        context.addRoutes(new RouteBuilder() {
            @Override
            public void configure() {
                from("direct:concurrent").routeId("concurrent")
                        .throttle(1).concurrentRequestsMode().asyncDelayed().executorService("one")
                        .process(e -> {
                            if ("first".equals(e.getMessage().getBody())) {
                                latch.await(20, TimeUnit.SECONDS);
                            }
                        });
            }
        });
        context.start();
        // the first exchange keeps the only permit, the second one waits for it on the only thread of the pool
        CompletableFuture<Exchange> first = template.asyncSend("direct:concurrent", e -> e.getMessage().setBody("first"));
        await().atMost(5, TimeUnit.SECONDS).until(() -> context.getInflightRepository().size("concurrent") == 1);
        CompletableFuture<Exchange> second = template.asyncSend("direct:concurrent", e -> e.getMessage().setBody("second"));
        await().atMost(5, TimeUnit.SECONDS).until(() -> context.getInflightRepository().size("concurrent") == 2);
        try {
            // the third one is queued in the thread pool
            assertExchangeCompletedOnRemove("direct:concurrent", "concurrent", 3);
            assertNotNull(second.get(10, TimeUnit.SECONDS).getException());
        } finally {
            latch.countDown();
        }
        // the first one completes too, cut off by the forced stop
        assertInstanceOf(RouteStoppingException.class, first.get(10, TimeUnit.SECONDS).getException());
        assertEquals(0, context.getInflightRepository().size());
    }

    @Test
    public void testThreadsRemoveRoute() throws Exception {
        context.addRoutes(new RouteBuilder() {
            @Override
            public void configure() {
                from("direct:threads").routeId("threads")
                        .threads(1, 1).maxQueueSize(10)
                        .process(e -> {
                            if ("first".equals(e.getMessage().getBody())) {
                                latch.await(20, TimeUnit.SECONDS);
                            }
                        });
            }
        });
        context.start();
        // the first exchange keeps the only thread of the pool busy
        CompletableFuture<Exchange> first = template.asyncSend("direct:threads", e -> e.getMessage().setBody("first"));
        await().atMost(5, TimeUnit.SECONDS).until(() -> context.getInflightRepository().size("threads") == 1);
        try {
            // the second one is queued in the thread pool
            assertExchangeCompletedOnRemove("direct:threads", "threads", 2);
        } finally {
            latch.countDown();
        }
        // the first one was interrupted by the shutdown of the thread pool
        assertNotNull(first.get(10, TimeUnit.SECONDS).getException());
        assertEquals(0, context.getInflightRepository().size());
    }

    private void assertExchangeCompletedOnRemove(String uri, String routeId) throws Exception {
        assertExchangeCompletedOnRemove(uri, routeId, 1);
        assertEquals(0, context.getInflightRepository().size());
    }

    private void assertExchangeCompletedOnRemove(String uri, String routeId, int inflight) throws Exception {
        CompletableFuture<Exchange> future = template.asyncSend(uri, e -> e.getMessage().setBody("waiting"));
        await().atMost(5, TimeUnit.SECONDS).until(() -> context.getInflightRepository().size(routeId) == inflight);

        // the shutdown timeout runs out, the route is stopped by force, then removed
        context.getRouteController().stopRoute(routeId, 100, TimeUnit.MILLISECONDS);
        context.removeRoute(routeId);

        Exchange out = future.get(10, TimeUnit.SECONDS);
        assertInstanceOf(RouteStoppingException.class, out.getException());
    }
}
