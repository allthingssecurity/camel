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

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.camel.AsyncCallback;
import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.RouteStoppingException;
import org.apache.camel.spi.IdAware;
import org.apache.camel.spi.RouteIdAware;
import org.apache.camel.spi.StepIdAware;
import org.apache.camel.util.ObjectHelper;
import org.apache.camel.util.concurrent.Rejectable;
import org.apache.camel.util.concurrent.ThreadPoolRejectedPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Threads processor that leverages a thread pool for continue processing the {@link Exchange}s using the asynchronous
 * routing engine.
 * <p>
 * <b>Notice:</b> For transacted routes then this {@link ThreadsProcessor} is not in use, as we want to process messages
 * using the same thread to support all work done in the same transaction. The reason is that the transaction manager
 * that orchestrates the transaction, requires all the work to be done on the same thread.
 * <p>
 * Pay attention to how this processor handles rejected tasks.
 * <ul>
 * <li>Abort - The current exchange will be set with a {@link RejectedExecutionException} exception, and marked to stop
 * continue routing. The {@link org.apache.camel.spi.UnitOfWork} will be regarded as <b>failed</b>, due to the
 * exception.</li>
 * <li>CallerRuns - The current exchange will be processed by the current thread. Which means the current thread will
 * not be free to process a new exchange, as it's processing the current exchange.</li>
 * </ul>
 * When the thread pool is shut down (such as a shared thread pool shut down by its owner), the exchange cannot be
 * processed with any policy: it is failed with a {@link RejectedExecutionException} (a {@link RouteStoppingException}
 * when this processor is stopped).
 */
public class ThreadsProcessor extends BaseProcessorSupport implements IdAware, RouteIdAware, StepIdAware {

    private static final Logger LOG = LoggerFactory.getLogger(ThreadsProcessor.class);

    private String id;
    private String routeId;
    private String stepId;
    private final CamelContext camelContext;
    private final ExecutorService executorService;
    private final ThreadPoolRejectedPolicy rejectedPolicy;
    private final boolean shutdownExecutorService;
    private final AtomicBoolean shutdown = new AtomicBoolean(true);

    private final class ProcessCall implements Runnable, Rejectable {
        private final Exchange exchange;
        private final AsyncCallback callback;
        private final boolean done;
        // the exchange is completed once: set by the task when it runs, by a reject, or by process when the thread
        // pool was shut down while the task was handed to it
        private final AtomicBoolean claimed = new AtomicBoolean();

        ProcessCall(Exchange exchange, AsyncCallback callback, boolean done) {
            this.exchange = exchange;
            this.callback = callback;
            this.done = done;
        }

        boolean claim() {
            return claimed.compareAndSet(false, true);
        }

        @Override
        public void run() {
            if (!claim()) {
                // the exchange was already failed as the thread pool was shut down
                return;
            }
            LOG.trace("Continue routing exchange {}", exchange);
            if (shutdown.get()) {
                exchange.setException(new RouteStoppingException("ThreadsProcessor is not running."));
            }
            callback.done(done);
        }

        @Override
        public void reject() {
            if (!claim()) {
                // the task has already run, or the exchange was already failed
                return;
            }
            // reject should mark the exchange with an rejected exception and mark not to route anymore
            exchange.setException(new RejectedExecutionException());
            LOG.trace("Rejected routing exchange {}", exchange);
            if (shutdown.get()) {
                exchange.setException(new RouteStoppingException("ThreadsProcessor is not running."));
            }
            callback.done(done);
        }

        /**
         * Fails the exchange in the calling thread when the thread pool is shut down and the task has not run: the
         * CallerRuns policy (the default) drops the task of a shut down thread pool without an exception, so nothing
         * else would complete the exchange.
         *
         * @return true when this call failed the exchange (and called the callback), false when the task runs (or ran,
         *         or was rejected) and completes the exchange itself
         */
        boolean failIfNotRun() {
            if (!claim()) {
                return false;
            }
            LOG.trace("Thread pool is shut down, rejected exchange {}", exchange);
            exchange.setException(new RejectedExecutionException(
                    "The thread pool of the Threads EIP is shut down: the exchange is not processed"));
            if (shutdown.get()) {
                exchange.setException(new RouteStoppingException("ThreadsProcessor is not running."));
            }
            callback.done(true);
            return true;
        }

        @Override
        public String toString() {
            return "ProcessCall[" + exchange + "]";
        }
    }

    public ThreadsProcessor(CamelContext camelContext, ExecutorService executorService, boolean shutdownExecutorService,
                            ThreadPoolRejectedPolicy rejectedPolicy) {
        ObjectHelper.notNull(camelContext, "camelContext");
        ObjectHelper.notNull(executorService, "executorService");
        ObjectHelper.notNull(rejectedPolicy, "rejectedPolicy");
        this.camelContext = camelContext;
        this.executorService = executorService;
        this.shutdownExecutorService = shutdownExecutorService;
        this.rejectedPolicy = rejectedPolicy;
    }

    @Override
    public boolean process(Exchange exchange, AsyncCallback callback) {
        if (shutdown.get()) {
            throw new IllegalStateException("ThreadsProcessor is not running.");
        }

        // we cannot execute this asynchronously for transacted exchanges, as the transaction manager doesn't support
        // using different threads in the same transaction
        if (exchange.isTransacted()) {
            LOG.trace("Transacted Exchange must be routed synchronously for exchangeId: {} -> {}", exchange.getExchangeId(),
                    exchange);
            callback.done(true);
            return true;
        }

        // process the call in asynchronous mode
        ProcessCall call = new ProcessCall(exchange, callback, false);
        try {
            LOG.trace("Submitting task {}", call);
            executorService.submit(call);
        } catch (Exception e) {
            return handleException(exchange, callback, e);
        }
        if (executorService.isShutdown() && call.failIfNotRun()) {
            // the thread pool was shut down before or while the task was handed to it (such as a shared thread pool
            // shut down by its owner, or the route removed meanwhile), so the task may have been dropped
            return true;
        }
        // tell Camel routing engine we continue routing asynchronous
        return false;
    }

    private boolean handleException(Exchange exchange, AsyncCallback callback, Exception e) {
        if (executorService instanceof ThreadPoolExecutor tpe) {
            // process the call in synchronous mode
            ProcessCall call = new ProcessCall(exchange, callback, true);
            rejectedPolicy.asRejectedExecutionHandler().rejectedExecution(call, tpe);
            if (tpe.isShutdown()) {
                // the CallerRuns policy drops the call of a shut down thread pool
                call.failIfNotRun();
            }
            return true;
        } else {
            exchange.setException(e);
            callback.done(true);
            return true;
        }
    }

    public ExecutorService getExecutorService() {
        return executorService;
    }

    @Override
    public String toString() {
        return id;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public void setId(String id) {
        this.id = id;
    }

    @Override
    public String getRouteId() {
        return routeId;
    }

    @Override
    public void setRouteId(String routeId) {
        this.routeId = routeId;
    }

    @Override
    public String getStepId() {
        return stepId;
    }

    @Override
    public void setStepId(String stepId) {
        this.stepId = stepId;
    }

    public ThreadPoolRejectedPolicy getRejectedPolicy() {
        return rejectedPolicy;
    }

    @Override
    protected void doStart() throws Exception {
        shutdown.set(false);
    }

    @Override
    protected void doStop() throws Exception {
        shutdown.set(true);
    }

    @Override
    protected void doShutdown() throws Exception {
        if (shutdownExecutorService) {
            List<Runnable> dropped = camelContext.getExecutorServiceManager().shutdownNow(executorService);
            if (dropped != null) {
                // the exchanges still queued in the thread pool are never processed: reject them so their callers
                // do not wait for ever. A thread pool factory that wraps the tasks (such as the instrumented thread
                // pools of camel-opentelemetry or camel-micrometer) hands back its own wrappers here, which are not
                // Rejectable, so those exchanges are not failed here
                for (Runnable task : dropped) {
                    if (task instanceof Rejectable rejectable) {
                        rejectable.reject();
                    }
                }
            }
        }
        super.doShutdown();
    }

}
