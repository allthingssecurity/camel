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
package org.apache.camel.component.hazelcast.policy;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import org.apache.camel.CamelContext;
import org.apache.camel.Route;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.service.ServiceHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The leadership taken by the policy's thread against the route start and the policy stop, with an embedded member.
 */
class HazelcastRoutePolicyLeadershipTest {

    private static final String LOCK_MAP = "route-policy-locks";
    private static final String LOCK_KEY = "route-lock";

    private HazelcastInstance member;
    private CamelContext context;

    @BeforeEach
    void startMember() {
        Config config = new Config();
        config.setClusterName("route-policy-" + UUID.randomUUID());
        config.getMetricsConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(false);
        member = Hazelcast.newHazelcastInstance(config);
    }

    @AfterEach
    void stop() {
        if (context != null) {
            context.stop();
        }
        if (member != null) {
            member.shutdown();
        }
    }

    @Test
    void leadershipTakenWhileTheRouteStartsIsNotLost() throws Exception {
        // another node holds the cluster lock, so the policy's own thread waits in tryLock during the test
        IMap<String, String> locks = member.getMap(LOCK_MAP);
        locks.lock(LOCK_KEY);

        TestPolicy policy = newPolicy(TimeUnit.MINUTES.toMillis(1));
        startContext(policy);

        // holds the lock that onStart on main waited for after it read the flag, and takes the leadership meanwhile.
        // The fixed onStart does not take this lock, so this checks the ordered case; the race test covers the atomicity
        ReentrantLock held = (ReentrantLock) policy.lifecycleLock();
        held.lock();
        CompletableFuture<Void> routeStart;
        try {
            routeStart = CompletableFuture.runAsync(() -> {
                try {
                    context.getRouteController().startRoute("route");
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            });
            await().atMost(20, TimeUnit.SECONDS).until(() -> held.hasQueuedThreads() || routeStart.isDone());

            // meanwhile this node takes the leadership: acquireLeadership calls setLeader(true) once it holds the
            // cluster lock, which starts the consumers the policy has stopped (none yet)
            policy.setLeader(true);
        } finally {
            held.unlock();
        }
        routeStart.get(20, TimeUnit.SECONDS);

        assertTrue(policy.isLeader());
        assertTrue(ServiceHelper.isStarted(context.getRoute("route").getConsumer()),
                "The node that holds the leadership must consume");
    }

    @Test
    void leadershipTakenWhileRoutesStartNeverLeavesTheLeaderIdle() throws Exception {
        IMap<String, String> locks = member.getMap(LOCK_MAP);
        locks.lock(LOCK_KEY);

        TestPolicy policy = newPolicy(TimeUnit.MINUTES.toMillis(1));
        startContext(policy);
        Route route = context.getRoute("route");

        // the route start (onStart) and the leadership thread (setLeader(true)) race, no hooks
        for (int i = 0; i < 1000; i++) {
            ServiceHelper.startService(route.getConsumer());
            policy.onStop(route);
            policy.setLeader(false);

            CyclicBarrier barrier = new CyclicBarrier(2);
            long spinNanos = ThreadLocalRandom.current().nextLong(2000);
            CompletableFuture<Void> routeStart = CompletableFuture.runAsync(() -> {
                awaitBarrier(barrier);
                policy.onStart(route);
            });
            CompletableFuture<Void> leadership = CompletableFuture.runAsync(() -> {
                awaitBarrier(barrier);
                long end = System.nanoTime() + spinNanos;
                while (System.nanoTime() < end) {
                    Thread.onSpinWait();
                }
                policy.setLeader(true);
            });
            CompletableFuture.allOf(routeStart, leadership).get(20, TimeUnit.SECONDS);

            assertTrue(ServiceHelper.isStarted(route.getConsumer()),
                    "The node that holds the leadership must consume (round " + i + ")");
        }
    }

    @Test
    void stopWhileTakingTheLeadershipDoesNotWaitForTheTimeout() throws Exception {

        // the policy's thread takes the cluster lock and is held before it sets the leader flag
        CountDownLatch elected = new CountDownLatch(1);
        CountDownLatch gate = new CountDownLatch(1);
        TestPolicy policy = newPolicy(100);
        policy.beforeLeadership = () -> {
            elected.countDown();
            awaitUninterruptibly(gate);
        };
        startContext(policy);
        assertTrue(elected.await(20, TimeUnit.SECONDS));

        // the policy stops (the CamelContext shuts down) when this node becomes the leader
        CompletableFuture<Void> stop = CompletableFuture.runAsync(policy::stop);
        try {
            await().atMost(20, TimeUnit.SECONDS).until(() -> policy.isStopping() || stop.isDone());
        } finally {
            gate.countDown();
        }

        // the stop interrupts the leadership thread and waits for it: it must not wait for the timeout (10 seconds)
        await().atMost(5, TimeUnit.SECONDS).until(stop::isDone);
        stop.get(20, TimeUnit.SECONDS);
    }

    private TestPolicy newPolicy(long tryLockTimeoutMillis) {
        TestPolicy policy = new TestPolicy(member);
        policy.setLockMapName(LOCK_MAP);
        policy.setLockKey(LOCK_KEY);
        policy.setLockValue("this-node");
        policy.setTryLockTimeout(tryLockTimeoutMillis, TimeUnit.MILLISECONDS);
        return policy;
    }

    private void startContext(TestPolicy policy) throws Exception {
        if (context == null) {
            context = new DefaultCamelContext();
        }
        context.addRoutes(new RouteBuilder() {
            @Override
            public void configure() {
                from("direct:start").routeId("route").autoStartup(false).routePolicy(policy).to("log:route");
            }
        });
        context.start();
    }

    private static void awaitBarrier(CyclicBarrier barrier) {
        try {
            barrier.await(20, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static void awaitUninterruptibly(CountDownLatch latch) {
        boolean interrupted = false;
        while (true) {
            try {
                latch.await();
                break;
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private static final class TestPolicy extends HazelcastRoutePolicy {
        volatile Runnable beforeLeadership;

        TestPolicy(HazelcastInstance instance) {
            super(instance);
        }

        Lock lifecycleLock() {
            return lock;
        }

        @Override
        protected void setLeader(boolean isLeader) {
            Runnable hook = beforeLeadership;
            if (isLeader && hook != null) {
                beforeLeadership = null;
                hook.run();
            }
            super.setLeader(isLeader);
        }
    }
}
