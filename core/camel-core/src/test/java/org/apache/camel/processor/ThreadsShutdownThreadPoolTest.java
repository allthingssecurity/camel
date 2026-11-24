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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.camel.AsyncCallback;
import org.apache.camel.ContextTestSupport;
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.support.DefaultExchange;
import org.apache.camel.support.service.ServiceHelper;
import org.apache.camel.util.concurrent.RejectableThreadPoolExecutor;
import org.apache.camel.util.concurrent.ThreadPoolRejectedPolicy;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The Threads EIP with the CallerRuns rejected policy (the default) on a thread pool that is shut down: the policy
 * drops the task without an exception, so the exchange must be failed with a {@link RejectedExecutionException} instead
 * of never being completed.
 */
class ThreadsShutdownThreadPoolTest extends ContextTestSupport {

    @Override
    public boolean isUseRouteBuilder() {
        return false;
    }

    @Test
    void testSharedThreadPoolShutDownByItsOwner() throws Exception {
        // a thread pool of the default profile (CallerRuns), which the route uses but does not own
        ExecutorService shared = context.getExecutorServiceManager().newDefaultThreadPool(this, "shared");
        context.getRegistry().bind("sharedPool", shared);
        context.addRoutes(new RouteBuilder() {
            @Override
            public void configure() {
                from("direct:start").routeId("threads").threads().executorService("sharedPool").to("mock:result");
            }
        });
        context.start();
        assertEquals("CallerRuns", ((ThreadPoolExecutor) shared).getRejectedExecutionHandler().toString());

        MockEndpoint mock = getMockEndpoint("mock:result");
        mock.expectedBodiesReceived("before");
        template.sendBody("direct:start", "before");

        // the owner of the thread pool shuts it down
        shared.shutdown();

        Future<Exchange> future = template.asyncSend("direct:start", e -> e.getMessage().setBody("after"));
        // the sender gets the failure (on main it waited for ever)
        Exchange out = future.get(10, TimeUnit.SECONDS);
        assertInstanceOf(RejectedExecutionException.class, out.getException());
        assertEquals(0, context.getInflightRepository().size("threads"));
        mock.assertIsSatisfied();
    }

    @Test
    void testThreadPoolShutDownWhileTheTaskIsSubmitted() throws Exception {
        // the only thread of the pool is busy, so a new task goes to the queue, and the pool is shut down while the task
        // is offered to its queue: the thread pool takes the task out again and the CallerRuns policy drops it
        CountDownLatch release = new CountDownLatch(1);
        ShutdownOnOfferQueue queue = new ShutdownOnOfferQueue();
        RejectableThreadPoolExecutor pool = new RejectableThreadPoolExecutor(
                1, 1, 0, TimeUnit.SECONDS, queue, ThreadPoolRejectedPolicy.CallerRuns.asRejectedExecutionHandler());
        queue.pool = pool;
        ThreadsProcessor processor = new ThreadsProcessor(context, pool, true, ThreadPoolRejectedPolicy.CallerRuns);
        ServiceHelper.startService(processor);
        try {
            CountDownLatch busy = new CountDownLatch(1);
            pool.execute(() -> {
                busy.countDown();
                await(release);
            });
            assertTrue(busy.await(5, TimeUnit.SECONDS));

            Exchange exchange = new DefaultExchange(context);
            CountingCallback callback = new CountingCallback();
            queue.shutdownOnOffer = true;
            boolean doneSync = processor.process(exchange, callback);

            assertTrue(pool.isShutdown());
            assertTrue(doneSync, "the exchange is completed in the calling thread");
            assertEquals(1, callback.calls.get());
            assertTrue(callback.doneSync);
            assertInstanceOf(RejectedExecutionException.class, exchange.getException());
        } finally {
            release.countDown();
            ServiceHelper.stopAndShutdownService(processor);
        }
    }

    @Test
    void testTaskQueuedBeforeTheShutdownCompletesTheExchangeOnce() throws Exception {
        // control: the task is queued, then the thread pool is shut down gracefully before process looks at it, so the
        // task is still run once the busy thread is free. Whichever completes the exchange, it is completed once
        CountDownLatch release = new CountDownLatch(1);
        RejectableThreadPoolExecutor pool = new RejectableThreadPoolExecutor(
                1, 1, 0, TimeUnit.SECONDS, new LinkedBlockingQueue<>(),
                ThreadPoolRejectedPolicy.CallerRuns.asRejectedExecutionHandler()) {
            @Override
            public Future<?> submit(Runnable task) {
                Future<?> answer = super.submit(task);
                shutdown();
                return answer;
            }
        };
        ThreadsProcessor processor = new ThreadsProcessor(context, pool, true, ThreadPoolRejectedPolicy.CallerRuns);
        ServiceHelper.startService(processor);
        try {
            CountDownLatch busy = new CountDownLatch(1);
            pool.execute(() -> {
                busy.countDown();
                await(release);
            });
            assertTrue(busy.await(5, TimeUnit.SECONDS));

            Exchange exchange = new DefaultExchange(context);
            CountingCallback callback = new CountingCallback();
            boolean doneSync = processor.process(exchange, callback);
            release.countDown();
            assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));

            assertEquals(1, callback.calls.get());
            assertEquals(doneSync, callback.doneSync);
        } finally {
            release.countDown();
            ServiceHelper.stopAndShutdownService(processor);
        }
    }

    @Test
    void testThreadPoolOfTheUserShutDown() throws Exception {
        // a thread pool of the JDK throws when it is shut down, then the Threads EIP applies its own CallerRuns policy
        ExecutorService pool = Executors.newFixedThreadPool(1);
        ThreadsProcessor processor = new ThreadsProcessor(context, pool, false, ThreadPoolRejectedPolicy.CallerRuns);
        ServiceHelper.startService(processor);
        try {
            pool.shutdown();

            Exchange exchange = new DefaultExchange(context);
            CountingCallback callback = new CountingCallback();
            boolean doneSync = processor.process(exchange, callback);

            assertTrue(doneSync);
            assertEquals(1, callback.calls.get());
            assertInstanceOf(RejectedExecutionException.class, exchange.getException());
        } finally {
            ServiceHelper.stopAndShutdownService(processor);
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await(20, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static final class CountingCallback implements AsyncCallback {
        private final AtomicInteger calls = new AtomicInteger();
        private volatile boolean doneSync;

        @Override
        public void done(boolean doneSync) {
            this.doneSync = doneSync;
            calls.incrementAndGet();
        }
    }

    private static final class ShutdownOnOfferQueue extends LinkedBlockingQueue<Runnable> {
        private volatile ThreadPoolExecutor pool;
        private volatile boolean shutdownOnOffer;

        @Override
        public boolean offer(Runnable task) {
            boolean answer = super.offer(task);
            if (shutdownOnOffer) {
                pool.shutdown();
            }
            return answer;
        }
    }
}
