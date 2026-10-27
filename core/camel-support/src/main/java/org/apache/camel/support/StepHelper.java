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
package org.apache.camel.support;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.camel.AsyncCallback;
import org.apache.camel.AsyncProcessor;
import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.spi.ReactiveExecutor;
import org.apache.camel.spi.StepHook;
import org.apache.camel.util.TimeSource;

/**
 * Helper for the places in Camel that call the processor of a step of a route: the error handler of the step, and the
 * {@link org.apache.camel.Channel} of a step that has no error handler that does it. It asks the {@link StepHook} of
 * the {@link CamelContext}, if one is installed, before it calls the processor.
 *
 * @since 4.24
 */
public final class StepHelper {

    // returned by beforeStep when the hook failed the step
    private static final long FAILED = -1;

    private StepHelper() {
    }

    /**
     * Calls the processor of a step with the {@link StepHook} asked first. Without a hook this is
     * {@code processor.process(exchange, callback)}.
     * <p/>
     * With a hook, see {@link StepHook} for what the hook can do. A step that the hook failed completes right away
     * without calling the processor. A delayed step returns {@code false} and the processor is called later from the
     * scheduled thread pool of the error handler, unless the exchange is transacted: then this thread waits. An
     * exception the processor throws, instead of calling the callback, is thrown to the caller, as without a hook.
     *
     * @param  exchange  the exchange
     * @param  nodeId    the id of the step
     * @param  processor the processor of the step
     * @param  callback  the callback, called exactly once when the step is done
     * @return           whether the step completed synchronously (the same value the callback was called with)
     */
    public static boolean process(Exchange exchange, String nodeId, AsyncProcessor processor, AsyncCallback callback) {
        final StepHook hook = exchange.getContext().getCamelContextExtension().getStepHook();
        if (hook == null) {
            return processor.process(exchange, callback);
        }

        final long delay = beforeStep(hook, exchange, nodeId);
        if (delay == FAILED) {
            // the hook failed the step, so the processor is not called
            callback.done(true);
            return true;
        }
        if (delay > 0) {
            if (!exchange.isTransacted()) {
                return scheduleStep(exchange, processor, callback, delay);
            }
            // a transacted exchange must stay on its thread, so wait here, as redelivery does for it
            if (!sleep(exchange, delay)) {
                callback.done(true);
                return true;
            }
        }
        return processor.process(exchange, callback);
    }

    /**
     * Calls the processor of a step synchronously, with the {@link StepHook} asked first. Without a hook this is
     * {@code processor.process(exchange)}.
     * <p/>
     * A step that the hook failed returns without calling the processor, with the exception on the exchange. A delay
     * waits on this thread.
     *
     * @param  exchange  the exchange
     * @param  nodeId    the id of the step
     * @param  processor the processor of the step
     * @throws Exception the exception the processor throws
     */
    public static void process(Exchange exchange, String nodeId, Processor processor) throws Exception {
        final StepHook hook = exchange.getContext().getCamelContextExtension().getStepHook();
        if (hook != null) {
            final long delay = beforeStep(hook, exchange, nodeId);
            if (delay == FAILED || delay > 0 && !sleep(exchange, delay)) {
                // the hook failed the step, or the wait was interrupted, so the processor is not called
                return;
            }
        }
        processor.process(exchange);
    }

    /**
     * Asks the hook.
     *
     * @return the delay in millis, 0 for none, or {@code FAILED} when the step failed (the exception is on the
     *         exchange)
     */
    private static long beforeStep(StepHook hook, Exchange exchange, String nodeId) {
        // an exchange can reach a step with an exception on it already, which does not fail the step
        final Exception existing = exchange.getException();
        final long delay;
        try {
            delay = hook.beforeStep(exchange, nodeId);
        } catch (Exception e) {
            exchange.setException(e);
            return FAILED;
        }
        final Exception exception = exchange.getException();
        if (exception != null && exception != existing) {
            return FAILED;
        }
        return Math.max(delay, 0);
    }

    /**
     * Waits for the delay on this thread.
     *
     * @return false when the wait was interrupted (the exception is on the exchange)
     */
    private static boolean sleep(Exchange exchange, long delay) {
        try {
            TimeSource.sleep(delay);
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            exchange.setException(e);
            return false;
        }
    }

    private static boolean scheduleStep(Exchange exchange, AsyncProcessor processor, AsyncCallback callback, long delay) {
        CamelContext context = exchange.getContext();
        ReactiveExecutor reactiveExecutor = context.getCamelContextExtension().getReactiveExecutor();
        // the same scheduled thread pool as the asynchronous redelivery of the error handler
        ScheduledExecutorService scheduler = PluginHelper.getErrorHandlerExecutorService(context);
        try {
            // the pool of a stopped camel context drops a task without an exception (its rejection policy is
            // CallerRuns, which does nothing once the pool is shut down), so check first
            if (scheduler.isShutdown()) {
                throw new RejectedExecutionException("Cannot delay the step as CamelContext is stopping");
            }
            scheduler.schedule(() -> reactiveExecutor.schedule(() -> processDelayed(exchange, processor, callback)), delay,
                    TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException e) {
            // the processor is not called
            exchange.setException(e);
            callback.done(true);
            return true;
        }
        return false;
    }

    /**
     * The delayed call of the processor, after {@link #process(Exchange, String, AsyncProcessor, AsyncCallback)}
     * returned false.
     */
    private static void processDelayed(Exchange exchange, AsyncProcessor processor, AsyncCallback callback) {
        try {
            // process returned false to its caller, so the step completes asynchronously
            processor.process(exchange, doneSync -> callback.done(false));
        } catch (RuntimeException e) {
            // there is no caller to throw to, so this is the end of the step
            exchange.setException(e);
            callback.done(false);
        }
    }
}
