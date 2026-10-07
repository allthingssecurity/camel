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

import java.util.concurrent.ExecutorService;

import org.apache.camel.Endpoint;
import org.apache.camel.Exchange;
import org.apache.camel.support.DefaultProducer;
import org.apache.camel.wasm.WasmFunction;
import org.apache.camel.wasm.WasmRuntime;
import org.apache.camel.wasm.WasmSupport;

/**
 * Calls an exported function of the module using the camel-wasm memory ABI (mode=function).
 */
public class WasmProducer extends DefaultProducer {

    private final String functionModule;
    private final String functionName;

    private WasmRuntime runtime;
    private WasmFunction function;
    private ExecutorService executor;

    public WasmProducer(Endpoint endpoint, String functionModule, String functionName) throws Exception {
        super(endpoint);

        this.functionModule = functionModule;
        this.functionName = functionName;
    }

    @Override
    public WasmEndpoint getEndpoint() {
        return (WasmEndpoint) super.getEndpoint();
    }

    @Override
    public void doInit() throws Exception {
        this.runtime = getEndpoint().runtime();
    }

    @Override
    public void doStart() throws Exception {
        super.doStart();

        if (this.runtime != null && this.function == null) {
            this.function = new WasmFunction(this.runtime, this.functionName);
        }
        if (getEndpoint().getConfiguration().getTimeout() > 0 && executor == null) {
            executor = getEndpoint().getCamelContext().getExecutorServiceManager()
                    .newCachedThreadPool(this, "WasmGuest[" + functionName + "]");
        }
    }

    @Override
    public void doStop() throws Exception {
        super.doStop();

        if (executor != null) {
            getEndpoint().getCamelContext().getExecutorServiceManager().shutdownNow(executor);
            executor = null;
        }
        this.function = null;
    }

    @Override
    public void doShutdown() throws Exception {
        super.doShutdown();

        this.function = null;
        this.runtime = null;
    }

    @Override
    public void process(Exchange exchange) throws Exception {
        byte[] in = WasmSupport.serialize(exchange);
        WasmFunction fn = this.function;
        byte[] result = WasmInvoker.call(executor, getEndpoint().getConfiguration().getTimeout(), exchange,
                () -> fn.run(in));

        WasmSupport.deserialize(result, exchange);
    }

    public String getFunctionModule() {
        return functionModule;
    }
}
