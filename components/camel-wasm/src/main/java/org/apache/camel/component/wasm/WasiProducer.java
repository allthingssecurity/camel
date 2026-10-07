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

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import org.apache.camel.Exchange;
import org.apache.camel.Message;
import org.apache.camel.support.DefaultProducer;
import org.apache.camel.util.ObjectHelper;
import org.apache.camel.wasm.WasiCommand;
import org.apache.camel.wasm.WasiExitCodeException;
import org.apache.camel.wasm.Wasm;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs a WASI preview1 command module once per exchange (mode=wasi): the body is the program's standard input, its
 * standard output becomes the body.
 * <p/>
 * Every exchange gets a fresh instance, so exchanges never share guest state, need no lock, and a trap or timeout
 * leaves nothing behind. The parsed (and optionally compiled) module is shared.
 */
public class WasiProducer extends DefaultProducer {

    private static final Logger LOG = LoggerFactory.getLogger(WasiProducer.class);

    private WasiCommand command;
    private Map<String, String> staticEnvironment = Collections.emptyMap();
    private List<String> environmentHeaders = Collections.emptyList();
    private ExecutorService executor;

    public WasiProducer(WasmEndpoint endpoint) {
        super(endpoint);
    }

    @Override
    public WasmEndpoint getEndpoint() {
        return (WasmEndpoint) super.getEndpoint();
    }

    @Override
    protected void doInit() throws Exception {
        super.doInit();

        WasmConfiguration conf = getEndpoint().getConfiguration();

        Map<String, String> env = new LinkedHashMap<>();
        if (conf.getEnvironment() != null) {
            for (Map.Entry<String, Object> e : conf.getEnvironment().entrySet()) {
                env.put(checkName(e.getKey()), e.getValue() != null ? e.getValue().toString() : "");
            }
        }
        List<String> headers = new ArrayList<>();
        if (ObjectHelper.isNotEmpty(conf.getEnvironmentHeaders())) {
            for (String h : conf.getEnvironmentHeaders().split(",")) {
                String name = checkName(h.trim());
                if (env.containsKey(name)) {
                    throw new IllegalArgumentException(
                            "Environment variable " + name + " is configured both in environment and environmentHeaders");
                }
                headers.add(name);
            }
        }
        this.staticEnvironment = Collections.unmodifiableMap(env);
        this.environmentHeaders = List.copyOf(headers);
        this.command = new WasiCommand(
                getEndpoint().runtime(), WasiCommand.arguments(getEndpoint().getFunctionName(), conf.getArgs()));
    }

    @Override
    protected void doStart() throws Exception {
        super.doStart();
        if (getEndpoint().getConfiguration().getTimeout() > 0 && executor == null) {
            executor = getEndpoint().getCamelContext().getExecutorServiceManager()
                    .newCachedThreadPool(this, "WasmGuest[" + getEndpoint().getFunctionName() + "]");
        }
    }

    @Override
    protected void doStop() throws Exception {
        super.doStop();
        if (executor != null) {
            getEndpoint().getCamelContext().getExecutorServiceManager().shutdownNow(executor);
            executor = null;
        }
    }

    @Override
    public void process(Exchange exchange) throws Exception {
        WasmConfiguration conf = getEndpoint().getConfiguration();
        Message message = exchange.getMessage();

        InputStream stdin = message.getBody(InputStream.class);
        if (stdin == null) {
            stdin = new ByteArrayInputStream(new byte[0]);
        }
        Map<String, String> env = environment(message);

        final InputStream in = stdin;
        WasiCommand.Result result = WasmInvoker.call(executor, conf.getTimeout(), exchange, () -> command.run(in, env));

        message.setHeader(Wasm.Headers.EXIT_CODE, result.exitCode());
        if (result.stderr().isEmpty()) {
            message.removeHeader(Wasm.Headers.STDERR);
        } else {
            message.setHeader(Wasm.Headers.STDERR, result.stderr());
            if (LOG.isDebugEnabled()) {
                LOG.debug("WASI program {} wrote to stderr{}: {}", getEndpoint().getFunctionName(),
                        result.stderrTruncated() ? " (truncated)" : "", result.stderr());
            }
        }

        if (result.exitCode() != 0 && conf.isFailOnNonZeroExit()) {
            throw new WasiExitCodeException(
                    getEndpoint().getFunctionName(), result.exitCode(), result.stderr(), exchange);
        }
        message.setBody(result.stdout());
    }

    private Map<String, String> environment(Message message) {
        if (environmentHeaders.isEmpty()) {
            return staticEnvironment;
        }
        Map<String, String> env = new LinkedHashMap<>(staticEnvironment);
        for (String name : environmentHeaders) {
            String value = message.getHeader(name, String.class);
            if (value != null) {
                if (value.indexOf('\0') >= 0) {
                    throw new IllegalArgumentException(
                            "Header " + name + " contains a NUL character and cannot be passed as an environment variable");
                }
                env.put(name, value);
            }
        }
        return env;
    }

    private static String checkName(String name) {
        if (ObjectHelper.isEmpty(name) || name.indexOf('=') >= 0 || name.indexOf('\0') >= 0) {
            throw new IllegalArgumentException("Invalid environment variable name: '" + name + "'");
        }
        return name;
    }
}
