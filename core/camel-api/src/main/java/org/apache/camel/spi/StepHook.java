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
package org.apache.camel.spi;

import org.apache.camel.Exchange;

/**
 * A hook that is asked before every processing step of a route, and can delay the step or fail it. A step is any node
 * of a route, with its node id: {@code to}, {@code process}, {@code log}, {@code split}, {@code choice} and each step
 * inside them, not only the {@code step} EIP.
 * <p/>
 * The hook is installed with {@link org.apache.camel.ExtendedCamelContext#setStepHook(StepHook)}. There is one hook per
 * {@link org.apache.camel.CamelContext}; to use more than one, combine them in one hook. Without a hook, a step costs
 * one check whether a hook is installed.
 * <p/>
 * <b>Where it is asked.</b> Each step runs through its {@link org.apache.camel.Channel}, which runs its advices (such
 * as tracing, the debuggers and JMX) and calls the error handler of the step, which calls the processor of the step.
 * The hook is asked right before the processor is called, once for each attempt:
 * <ul>
 * <li>By the error handler of the step ({@code DefaultErrorHandler}, {@code DeadLetterChannel} or a transaction error
 * handler that extends them). So a failure from the hook is handled as a failure of the processor: {@code onException},
 * redelivery and the dead letter channel apply to it, and each redelivery asks the hook again.</li>
 * <li>By the channel, for a step that has no such error handler, such as the steps inside {@code doTry},
 * {@code doCatch}, {@code doFinally} and {@code onException}, a {@code multicast} and its outputs, and every step of a
 * route with {@code noErrorHandler()}. A failure from the hook is then the exception of the exchange, as when the
 * processor fails: inside {@code doTry}, {@code doCatch} gets it; an output of a {@code multicast} is handled (and
 * redelivered) by the error handler that the multicast keeps for each output; otherwise the exchange fails with it,
 * without {@code onException}, redelivery or dead letter channel.</li>
 * </ul>
 * A step that is disabled at runtime ({@link org.apache.camel.DisabledAware}) is skipped before the hook is asked.
 * <p/>
 * <b>Threads.</b> The hook is called by many threads at the same time, and must be thread-safe. It is called on the
 * thread that runs the step.
 *
 * @since 4.24
 */
public interface StepHook {

    /**
     * Returned by {@link #beforeStep(Exchange, String)} to call the processor of the step right away.
     */
    long PROCEED = 0;

    /**
     * Called before the processor of a step is called, and again before each redelivery of the step.
     * <p/>
     * To fail the step, the hook sets an exception on the exchange, or throws one: the processor is not called, and the
     * step fails with that exception. (An exchange that already has an exception when the step is called, which is
     * rare, is processed as usual unless the hook sets another one.)
     * <p/>
     * A positive return value delays the call of the processor by that many milliseconds. The delay does not block the
     * thread: the processor is called from the scheduled thread pool that the error handler uses for asynchronous
     * redelivery, and the step completes asynchronously. A transacted exchange must stay on its thread, so for a
     * transacted exchange the thread waits, as a redelivery of a transacted exchange does.
     *
     * @param  exchange the exchange
     * @param  nodeId   the id of the step: the id of its node in the route, such as {@code to1}, or the id set with
     *                  {@code id("myStep")}
     * @return          {@link #PROCEED} to call the processor now, or a delay in milliseconds to call it later.
     *                  Negative values are the same as {@link #PROCEED}. The value is ignored when the hook sets an
     *                  exception on the exchange.
     */
    long beforeStep(Exchange exchange, String nodeId);

}
