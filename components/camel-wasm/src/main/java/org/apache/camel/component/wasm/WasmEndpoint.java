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

import java.io.InputStream;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.camel.Category;
import org.apache.camel.Component;
import org.apache.camel.Consumer;
import org.apache.camel.Processor;
import org.apache.camel.Producer;
import org.apache.camel.spi.Metadata;
import org.apache.camel.spi.Resource;
import org.apache.camel.spi.ResourceLoader;
import org.apache.camel.spi.UriEndpoint;
import org.apache.camel.spi.UriParam;
import org.apache.camel.spi.UriPath;
import org.apache.camel.support.DefaultEndpoint;
import org.apache.camel.support.PluginHelper;
import org.apache.camel.wasm.Wasm;
import org.apache.camel.wasm.WasmRuntime;
import run.endive.wasm.Parser;

/**
 * Invoke Wasm functions.
 */
@UriEndpoint(firstVersion = "4.4.0", scheme = Wasm.SCHEME, title = "Wasm", syntax = "wasm:functionName",
             producerOnly = true, remote = false, category = { Category.CORE, Category.SCRIPT },
             headersClass = Wasm.Headers.class)
public class WasmEndpoint extends DefaultEndpoint {

    @Metadata(required = true)
    @UriPath(description = "The name of the exported function to call. In wasi mode, the name passed to the program as"
                           + " argument zero.")
    private final String functionName;
    @UriParam
    private WasmConfiguration configuration;

    private final Lock runtimeLock = new ReentrantLock();
    private WasmRuntime runtime;

    public WasmEndpoint(String endpointUri, Component component, String functionName,
                        WasmConfiguration configuration) {
        super(endpointUri, component);
        this.functionName = functionName;
        this.configuration = configuration;
    }

    @Override
    public boolean isRemote() {
        return false;
    }

    public WasmConfiguration getConfiguration() {
        return configuration;
    }

    public String getFunctionName() {
        return functionName;
    }

    @Override
    public Producer createProducer() throws Exception {
        String mode = configuration.getMode();
        if (mode == null || Wasm.MODE_FUNCTION.equalsIgnoreCase(mode)) {
            if (configuration.getArgs() != null || configuration.getEnvironment() != null
                    || configuration.getEnvironmentHeaders() != null) {
                throw new IllegalArgumentException(
                        "The args, environment and environmentHeaders options only apply to mode=wasi");
            }
            return new WasmProducer(this, configuration.getModule(), functionName);
        }
        if (configuration.isWasi()) {
            return new WasiProducer(this);
        }
        throw new IllegalArgumentException(
                "Unknown mode " + mode + ", expected " + Wasm.MODE_FUNCTION + " or " + Wasm.MODE_WASI);
    }

    @Override
    public Consumer createConsumer(Processor processor) throws Exception {
        throw new UnsupportedOperationException("You cannot consume from a wasm endpoint");
    }

    /**
     * The parsed (and, with compile=true, compiled) module, loaded once per endpoint and shared by its producers.
     */
    WasmRuntime runtime() throws Exception {
        runtimeLock.lock();
        try {
            if (runtime == null) {
                final ResourceLoader rl = PluginHelper.getResourceLoader(getCamelContext());
                final Resource res = rl.resolveResource(configuration.getModule());
                try (InputStream is = res.getInputStream()) {
                    runtime = new WasmRuntime(
                            Parser.parse(is), configuration.getMaxMemoryPages(), configuration.isCompile());
                }
            }
            return runtime;
        } finally {
            runtimeLock.unlock();
        }
    }

    @Override
    protected void doShutdown() throws Exception {
        super.doShutdown();
        runtimeLock.lock();
        try {
            runtime = null;
        } finally {
            runtimeLock.unlock();
        }
    }
}
