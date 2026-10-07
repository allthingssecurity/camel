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
package org.apache.camel.wasm;

import java.util.Objects;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import run.endive.runtime.ExportFunction;
import run.endive.runtime.Instance;
import run.endive.wasm.WasmModule;

public class WasmFunction implements AutoCloseable {
    private final Lock lock;

    private final WasmRuntime runtime;
    private final String functionName;

    private Instance instance;
    private ExportFunction function;
    private ExportFunction alloc;
    private ExportFunction dealloc;

    public WasmFunction(WasmModule module, String functionName) {
        this(new WasmRuntime(module, 0, false), functionName);
    }

    public WasmFunction(WasmRuntime runtime, String functionName) {
        this.lock = new ReentrantLock();

        this.runtime = Objects.requireNonNull(runtime);
        this.functionName = Objects.requireNonNull(functionName);

        // fail fast on a missing export
        bind();
    }

    public byte[] run(byte[] in) throws Exception {
        Objects.requireNonNull(in);

        //
        // Wasm execution is not thread safe so we must put a
        // synchronization guard around the function execution
        //
        lock.lock();
        try {
            if (instance == null) {
                bind();
            }

            final int inSize = in.length;
            final byte[] out;
            String guestError = null;

            try {
                int inPtr = (int) alloc.apply(inSize)[0];
                instance.memory().write(inPtr, in);

                long[] results = function.apply(inPtr, inSize);
                long ptrAndSize = results[0];

                int outPtr = (int) (ptrAndSize >> 32);
                int outSize = (int) ptrAndSize;

                // assume the max output is 31 bit, leverage the first bit for
                // error detection
                if (isError(outSize)) {
                    outSize = errSize(outSize);
                    guestError = instance.memory().readString(outPtr, outSize);
                    out = null;
                } else {
                    out = instance.memory().readBytes(outPtr, outSize);
                }

                dealloc.apply(inPtr, inSize);
                dealloc.apply(outPtr, outSize);
            } catch (RuntimeException | Error e) {
                // A trap, an interruption (timeout) or a failed memory growth leaves the guest's heap in a state
                // the host cannot repair: whatever the guest allocated before it stopped is unknown here. Throw
                // the instance away; the next call gets a fresh one.
                discard();
                throw e;
            }

            if (guestError != null) {
                throw new RuntimeException(guestError);
            }
            return out;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() throws Exception {
        lock.lock();
        try {
            discard();
        } finally {
            lock.unlock();
        }
    }

    private void bind() {
        this.instance = runtime.instanceBuilder().build();
        this.function = this.instance.export(this.functionName);
        this.alloc = this.instance.export(Wasm.FN_ALLOC);
        this.dealloc = this.instance.export(Wasm.FN_DEALLOC);
    }

    private void discard() {
        this.instance = null;
        this.function = null;
        this.alloc = null;
        this.dealloc = null;
    }

    private static boolean isError(int number) {
        return (number & (1 << 31)) != 0;
    }

    private static int errSize(int number) {
        return number & (~(1 << 31));
    }
}
