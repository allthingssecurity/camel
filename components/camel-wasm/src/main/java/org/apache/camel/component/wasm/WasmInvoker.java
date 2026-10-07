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
package org.apache.camel.component.wasm;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.camel.Exchange;
import org.apache.camel.ExchangeTimedOutException;

/**
 * Runs a guest call, bounded by a timeout when one is configured.
 * <p/>
 * The guest runs on a worker thread and the route thread waits for it. On timeout the worker is interrupted: Endive
 * checks the interrupt flag on every call and every backward branch, in the interpreter and in compiled code, so the
 * guest stops at its next loop iteration. Interrupting the worker rather than the route thread keeps the interrupt flag
 * away from Camel's own threads. The caller must treat the instance the guest ran on as unusable afterwards.
 */
final class WasmInvoker {

    private WasmInvoker() {
    }

    static <T> T call(ExecutorService executor, long timeout, Exchange exchange, Callable<T> task) throws Exception {
        if (executor == null || timeout <= 0) {
            return task.call();
        }
        Future<T> future = executor.submit(task);
        try {
            return future.get(timeout, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            future.cancel(true);
            throw new ExchangeTimedOutException(
                    exchange, timeout, "The Wasm module did not complete within " + timeout + " ms and was interrupted");
        } catch (InterruptedException e) {
            future.cancel(true);
            Thread.currentThread().interrupt();
            throw e;
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof Exception ex) {
                throw ex;
            }
            if (cause instanceof Error err) {
                throw err;
            }
            throw e;
        }
    }
}
