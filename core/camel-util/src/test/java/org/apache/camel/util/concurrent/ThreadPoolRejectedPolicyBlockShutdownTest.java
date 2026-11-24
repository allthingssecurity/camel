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
package org.apache.camel.util.concurrent;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The Block rejected policy: a caller that waits for space in the queue when the thread pool is shut down must get its
 * task rejected, not put it into the queue of a thread pool that never runs it (nor wait for ever).
 */
class ThreadPoolRejectedPolicyBlockShutdownTest {

    private final CountDownLatch release = new CountDownLatch(1);

    @Test
    void testWaitingCallerIsRejectedWhenShutdownNowDrainsTheQueue() throws Exception {
        SignalingQueue queue = new SignalingQueue(1);
        ThreadPoolExecutor pool = newPool(queue);
        try {
            CountingTask running = new CountingTask();
            pool.execute(running);
            assertTrue(running.started.await(5, TimeUnit.SECONDS));
            CountingTask queued = new CountingTask();
            pool.execute(queued);

            // the queue is full: the caller waits for space
            CountingTask waiting = new CountingTask();
            CompletableFuture<Void> caller = execute(pool, waiting);
            assertTrue(queue.waiting.await(5, TimeUnit.SECONDS));

            // shutdownNow takes the queued task out of the queue, which makes space for the waiting caller
            List<Runnable> drained = pool.shutdownNow();
            assertEquals(List.of(queued), drained);

            ExecutionException e = assertThrows(ExecutionException.class, () -> caller.get(10, TimeUnit.SECONDS));
            assertInstanceOf(RejectedExecutionException.class, e.getCause());
            assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
            assertTrue(pool.getQueue().isEmpty(), "no task is left in the queue of the terminated thread pool");
            assertEquals(0, waiting.runs.get());
        } finally {
            release.countDown();
            pool.shutdownNow();
        }
    }

    @Test
    void testWaitingRejectableTaskIsRejectedWhenShutdownNowDrainsTheQueue() throws Exception {
        SignalingQueue queue = new SignalingQueue(1);
        ThreadPoolExecutor pool = newPool(queue);
        try {
            CountingTask running = new CountingTask();
            pool.execute(running);
            assertTrue(running.started.await(5, TimeUnit.SECONDS));
            pool.execute(new CountingTask());

            RejectableCountingTask waiting = new RejectableCountingTask();
            CompletableFuture<Void> caller = execute(pool, waiting);
            assertTrue(queue.waiting.await(5, TimeUnit.SECONDS));

            pool.shutdownNow();

            // a Rejectable task is rejected (as when the thread pool is already shut down), the caller returns
            assertNull(caller.get(10, TimeUnit.SECONDS));
            assertEquals(1, waiting.rejects.get());
            assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
            assertTrue(pool.getQueue().isEmpty());
            assertEquals(0, waiting.runs.get());
        } finally {
            release.countDown();
            pool.shutdownNow();
        }
    }

    @Test
    void testWaitingCallerIsRejectedWhenThePoolWithoutQueueIsShutDown() throws Exception {
        // a SynchronousQueue (maxQueueSize 0): the caller waits for a thread to take its task, and once the thread pool
        // is shut down (gracefully) no thread takes from the queue any more, nor wakes up the caller
        SignalingSynchronousQueue queue = new SignalingSynchronousQueue();
        ThreadPoolExecutor pool = newPool(queue);
        CountingTask running = new CountingTask();
        pool.execute(running);
        assertTrue(running.started.await(5, TimeUnit.SECONDS));
        CountingTask waiting = new CountingTask();
        CompletableFuture<Void> caller = execute(pool, waiting);
        try {
            assertTrue(queue.waiting.await(5, TimeUnit.SECONDS));

            pool.shutdown();

            ExecutionException e = assertThrows(ExecutionException.class, () -> caller.get(10, TimeUnit.SECONDS));
            assertInstanceOf(RejectedExecutionException.class, e.getCause());
            release.countDown();
            assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
            assertEquals(0, waiting.runs.get());
        } finally {
            release.countDown();
            pool.shutdownNow();
            caller.cancel(true);
        }
    }

    private static ThreadPoolExecutor newPool(BlockingQueue<Runnable> queue) {
        return new RejectableThreadPoolExecutor(
                1, 1, 0, TimeUnit.SECONDS, queue, ThreadPoolRejectedPolicy.Block.asRejectedExecutionHandler());
    }

    private static CompletableFuture<Void> execute(ThreadPoolExecutor pool, Runnable task) {
        CompletableFuture<Void> answer = new CompletableFuture<>();
        Thread caller = new Thread(() -> {
            try {
                pool.execute(task);
                answer.complete(null);
            } catch (Throwable e) {
                answer.completeExceptionally(e);
            }
        }, "caller");
        caller.setDaemon(true);
        caller.start();
        // cancel interrupts a caller that would otherwise wait for ever
        answer.whenComplete((r, e) -> {
            if (answer.isCancelled()) {
                caller.interrupt();
            }
        });
        return answer;
    }

    private class CountingTask implements Runnable {
        final CountDownLatch started = new CountDownLatch(1);
        final AtomicInteger runs = new AtomicInteger();

        @Override
        public void run() {
            runs.incrementAndGet();
            started.countDown();
            try {
                release.await(20, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private final class RejectableCountingTask extends CountingTask implements Rejectable {
        private final AtomicInteger rejects = new AtomicInteger();

        @Override
        public void reject() {
            rejects.incrementAndGet();
        }
    }

    /**
     * A queue that counts down a latch when a caller starts to wait for space in it (put, or offer with a timeout).
     */
    private static final class SignalingQueue extends LinkedBlockingQueue<Runnable> {
        private final CountDownLatch waiting = new CountDownLatch(1);

        SignalingQueue(int capacity) {
            super(capacity);
        }

        @Override
        public void put(Runnable task) throws InterruptedException {
            waiting.countDown();
            super.put(task);
        }

        @Override
        public boolean offer(Runnable task, long timeout, TimeUnit unit) throws InterruptedException {
            waiting.countDown();
            return super.offer(task, timeout, unit);
        }
    }

    /**
     * The same for a queue without capacity, as the thread pools of Camel use for maxQueueSize 0.
     */
    private static final class SignalingSynchronousQueue extends SynchronousQueue<Runnable> {
        private final CountDownLatch waiting = new CountDownLatch(1);

        @Override
        public void put(Runnable task) throws InterruptedException {
            waiting.countDown();
            super.put(task);
        }

        @Override
        public boolean offer(Runnable task, long timeout, TimeUnit unit) throws InterruptedException {
            waiting.countDown();
            return super.offer(task, timeout, unit);
        }
    }
}
