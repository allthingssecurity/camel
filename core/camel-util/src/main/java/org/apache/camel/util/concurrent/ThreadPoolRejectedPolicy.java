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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Represent the kinds of options for rejection handlers for thread pools.
 * <p>
 * These options are used for fine-grained thread pool settings, where you want to control which handler to use when a
 * thread pool cannot execute a new task.
 * <p>
 * Camel will by default use <code>CallerRuns</code>.
 */
public enum ThreadPoolRejectedPolicy {

    Abort,
    CallerRuns,
    /**
     * The caller waits until there is space in the queue of the thread pool. When the thread pool is shut down while
     * the caller waits, or before the task is taken from the queue, the task is rejected.
     */
    Block;

    // how often a caller waiting for space in the queue looks whether the thread pool was shut down
    private static final long BLOCK_SHUTDOWN_CHECK_MILLIS = 100;

    public RejectedExecutionHandler asRejectedExecutionHandler() {
        if (this == Abort) {
            return new RejectedExecutionHandler() {
                @Override
                public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                    if (r instanceof Rejectable rejectable) {
                        rejectable.reject();
                    } else {
                        throw new RejectedExecutionException("Task " + r.toString() + " rejected from " + executor.toString());
                    }
                }

                @Override
                public String toString() {
                    return "Abort";
                }
            };
        } else if (this == CallerRuns) {
            return new ThreadPoolExecutor.CallerRunsPolicy() {
                @Override
                public String toString() {
                    return "CallerRuns";
                }
            };
        } else if (this == Block) {
            return new RejectedExecutionHandler() {
                @Override
                public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
                    if (executor.isShutdown()) {
                        rejectShutdown(r, executor);
                        return;
                    }
                    BlockingQueue<Runnable> queue = executor.getQueue();
                    try {
                        // a shutdown does not always wake up a caller that waits for space (a SynchronousQueue has no
                        // taker any more), so the caller looks whether the thread pool was shut down meanwhile
                        while (!queue.offer(r, BLOCK_SHUTDOWN_CHECK_MILLIS, TimeUnit.MILLISECONDS)) {
                            if (executor.isShutdown()) {
                                rejectShutdown(r, executor);
                                return;
                            }
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RejectedExecutionException("Interrupted while waiting for queue space", e);
                    }
                    // as ThreadPoolExecutor.execute does after it queued a task: when the thread pool was shut down
                    // meanwhile, the task may never run (shutdownNow drained the queue before the task was added, or
                    // the last thread of the pool has exited), so it is taken out of the queue again and rejected.
                    // When it cannot be taken out, a thread of the pool or shutdownNow has it
                    if (executor.isShutdown() && executor.remove(r)) {
                        rejectShutdown(r, executor);
                    }
                }

                private void rejectShutdown(Runnable r, ThreadPoolExecutor executor) {
                    if (r instanceof Rejectable rejectable) {
                        // the task cannot run as the pool is shutdown
                        rejectable.reject();
                    } else {
                        throw new RejectedExecutionException("Task " + r.toString() + " rejected from " + executor.toString());
                    }
                }

                @Override
                public String toString() {
                    return "Block";
                }
            };
        }
        throw new IllegalArgumentException("Unknown ThreadPoolRejectedPolicy: " + this);
    }

}
