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
import java.util.Optional;
import java.util.function.Function;

import run.endive.compiler.MachineFactoryCompiler;
import run.endive.runtime.Instance;
import run.endive.runtime.Machine;
import run.endive.wasm.WasmModule;
import run.endive.wasm.types.MemoryLimits;
import run.endive.wasm.types.MemorySection;

/**
 * A parsed module plus the per-endpoint settings that apply to every instance created from it: the memory limit and,
 * optionally, the module translated to JVM bytecode. Immutable and safe to share; the instances it builds are not.
 */
public final class WasmRuntime {

    private final WasmModule module;
    private final MemoryLimits memoryLimits;
    private final Function<Instance, Machine> machineFactory;

    /**
     * @param module         the parsed module
     * @param maxMemoryPages the maximum number of 64 KiB pages the module's memory may grow to, or 0 to keep the
     *                       maximum the module declares
     * @param compile        whether to translate the module to JVM bytecode now, instead of interpreting it
     */
    public WasmRuntime(WasmModule module, int maxMemoryPages, boolean compile) {
        this.module = Objects.requireNonNull(module);
        this.memoryLimits = memoryLimits(module, maxMemoryPages);
        this.machineFactory = compile ? MachineFactoryCompiler.compile(module) : null;
    }

    public WasmModule module() {
        return module;
    }

    public boolean isCompiled() {
        return machineFactory != null;
    }

    /**
     * A builder for a new instance of the module, with the memory limit and machine already applied.
     */
    public Instance.Builder instanceBuilder() {
        Instance.Builder builder = Instance.builder(module);
        if (memoryLimits != null) {
            builder.withMemoryLimits(memoryLimits);
        }
        if (machineFactory != null) {
            builder.withMachineFactory(machineFactory);
        }
        return builder;
    }

    /**
     * A hint to append to the message of a trap when the instance's memory is at its maximum, which is the usual reason
     * a module traps after maxMemoryPages was lowered: its allocator could not grow the memory and aborted.
     */
    public static String memoryHint(Instance instance) {
        try {
            if (instance != null && instance.memory() != null
                    && instance.memory().pages() >= instance.memory().maximumPages()) {
                return " (the module's memory is at its maximum of " + instance.memory().maximumPages()
                       + " pages; see the maxMemoryPages option)";
            }
        } catch (RuntimeException e) {
            // no memory exported or defined: no hint
        }
        return "";
    }

    static MemoryLimits memoryLimits(WasmModule module, int maxMemoryPages) {
        if (maxMemoryPages <= 0) {
            return null;
        }
        Optional<MemorySection> section = module.memorySection();
        if (section.isEmpty() || section.get().memoryCount() == 0) {
            // the module defines no memory of its own, so there is nothing to limit
            return null;
        }
        MemoryLimits declared = section.get().getMemory(0).limits();
        if (maxMemoryPages < declared.initialPages()) {
            throw new IllegalArgumentException(
                    "maxMemoryPages (" + maxMemoryPages + ") is lower than the " + declared.initialPages()
                                               + " pages of memory the module declares as its initial size");
        }
        return new MemoryLimits(declared.initialPages(), Math.min(maxMemoryPages, declared.maximumPages()));
    }
}
