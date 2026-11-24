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

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.camel.ContextTestSupport;
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.builder.ThreadPoolProfileBuilder;
import org.apache.camel.component.log.ConsumingAppender;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.util.concurrent.ThreadPoolRejectedPolicy;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * A wire tap whose thread pool is shut down: the tap is lost (the CallerRuns policy drops it, the Block policy may put
 * it into the queue of a thread pool that no longer runs it). It must be logged at WARN and must not stay counted as a
 * pending exchange, which a graceful shutdown waits for.
 */
class WireTapShutdownThreadPoolTest extends ContextTestSupport {

    private final List<String> warnings = new CopyOnWriteArrayList<>();
    private final CountDownLatch release = new CountDownLatch(1);

    @Override
    public boolean isUseRouteBuilder() {
        return false;
    }

    @Override
    @BeforeEach
    public void setUp() throws Exception {
        ConsumingAppender.newAppender(WireTapProcessor.class.getName(), "WireTapShutdownThreadPoolTest", Level.WARN,
                e -> warnings.add(e.getMessage().getFormattedMessage()));
        super.setUp();
    }

    @Override
    @AfterEach
    public void tearDown() throws Exception {
        release.countDown();
        super.tearDown();
        LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
        ctx.getConfiguration().removeLogger(WireTapProcessor.class.getName());
        ctx.updateLoggers();
    }

    @Test
    void testSharedThreadPoolShutDownByItsOwner() throws Exception {
        // a thread pool of the default profile (CallerRuns), which the route uses but does not own
        ExecutorService shared = context.getExecutorServiceManager().newDefaultThreadPool(this, "shared");
        context.addRoutes(new RouteBuilder() {
            @Override
            public void configure() {
                from("direct:start").routeId("tapRoute")
                        .wireTap("mock:tapped").executorService(shared).id("tap")
                        .to("mock:main");
            }
        });
        context.start();
        assertEquals("CallerRuns", ((ThreadPoolExecutor) shared).getRejectedExecutionHandler().toString());
        WireTapProcessor tap = context.getProcessor("tap", WireTapProcessor.class);

        MockEndpoint tapped = getMockEndpoint("mock:tapped");
        tapped.expectedBodiesReceived("before");
        template.sendBody("direct:start", "before");
        tapped.assertIsSatisfied();
        await().atMost(5, TimeUnit.SECONDS).until(() -> tap.getPendingExchangesSize() == 0);

        // the owner of the thread pool shuts it down
        shared.shutdown();

        MockEndpoint main = getMockEndpoint("mock:main");
        main.expectedMessageCount(4);
        for (int i = 0; i < 3; i++) {
            Exchange out = template.send("direct:start", e -> e.getMessage().setBody("after"));
            // the original exchange is not failed nor held by the lost tap
            assertNull(out.getException());
        }
        main.assertIsSatisfied();

        // the lost taps are not pending (a graceful shutdown would wait for them until its timeout), and are logged
        assertEquals(0, tap.getPendingExchangesSize());
        assertEquals(3, warnings.stream().filter(m -> m.contains("The wire tap to mock:tapped is lost")).count(),
                warnings.toString());
        tapped.assertIsSatisfied();
    }

    @Test
    void testBlockedTapPutIntoTheQueueOfTheShutDownThreadPool() throws Exception {
        // the route owns its thread pool: one thread, room for one task in the queue, the Block policy
        context.getExecutorServiceManager().registerThreadPoolProfile(new ThreadPoolProfileBuilder("block")
                .poolSize(1).maxPoolSize(1).maxQueueSize(1).rejectedPolicy(ThreadPoolRejectedPolicy.Block).build());
        context.addRoutes(new RouteBuilder() {
            @Override
            public void configure() {
                from("direct:start").routeId("tapRoute")
                        .wireTap("direct:tap").executorService("block").id("tap")
                        .to("mock:main");
                from("direct:tap").routeId("tapped")
                        .process(e -> {
                            if ("first".equals(e.getMessage().getBody())) {
                                release.await(20, TimeUnit.SECONDS);
                            }
                        });
            }
        });
        context.start();
        WireTapProcessor tap = context.getProcessor("tap", WireTapProcessor.class);

        // the first tap keeps the only thread busy, the second one waits in the queue, the third caller waits for space
        template.sendBody("direct:start", "first");
        await().atMost(5, TimeUnit.SECONDS).until(() -> context.getInflightRepository().size("tapped") == 1);
        template.sendBody("direct:start", "second");
        Thread[] caller = new Thread[1];
        CompletableFuture<Exchange> third = CompletableFuture.supplyAsync(() -> {
            caller[0] = Thread.currentThread();
            return template.send("direct:start", e -> e.getMessage().setBody("third"));
        });
        await().atMost(5, TimeUnit.SECONDS).until(() -> caller[0] != null && waitsForSpace(caller[0]));
        assertEquals(3, tap.getPendingExchangesSize());

        // the route is stopped after the shutdown timeout and removed: the thread pool is shut down, which takes the
        // second tap out of the queue and makes space for the third one
        context.getRouteController().stopRoute("tapRoute", 100, TimeUnit.MILLISECONDS);
        context.removeRoute("tapRoute");
        release.countDown();

        // the caller of the third exchange returns (its exchange is cut off by the forced stop of the route)
        assertNotNull(third.get(10, TimeUnit.SECONDS));
        // the first tap completed, the second and the third are lost: none is left pending
        await().atMost(5, TimeUnit.SECONDS).until(() -> tap.getPendingExchangesSize() == 0);
        assertEquals(0, context.getInflightRepository().size("tapped"));
    }

    private static boolean waitsForSpace(Thread thread) {
        Thread.State state = thread.getState();
        return (state == Thread.State.WAITING || state == Thread.State.TIMED_WAITING)
                && Arrays.stream(thread.getStackTrace())
                        .anyMatch(f -> f.getClassName().startsWith(ThreadPoolRejectedPolicy.class.getName()));
    }
}
