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

import java.util.Map;

import org.apache.camel.RuntimeCamelException;
import org.apache.camel.spi.Configurer;
import org.apache.camel.spi.Metadata;
import org.apache.camel.spi.UriParam;
import org.apache.camel.spi.UriParams;
import org.apache.camel.wasm.Wasm;

@Configurer
@UriParams
public class WasmConfiguration implements Cloneable {

    @Metadata(required = true)
    @UriParam
    private String module;

    @UriParam(defaultValue = Wasm.MODE_FUNCTION, enums = Wasm.MODE_FUNCTION + "," + Wasm.MODE_WASI)
    private String mode = Wasm.MODE_FUNCTION;

    @UriParam(label = "wasi")
    private String args;
    @UriParam(label = "wasi", prefix = "environment.", multiValue = true)
    private Map<String, Object> environment;
    @UriParam(label = "wasi")
    private String environmentHeaders;
    @UriParam(label = "wasi", defaultValue = "true")
    private boolean failOnNonZeroExit = true;

    @UriParam(label = "advanced")
    private int maxMemoryPages;
    @UriParam(label = "advanced", javaType = "java.time.Duration")
    private long timeout;
    @UriParam(label = "advanced")
    private boolean compile;

    public String getModule() {
        return module;
    }

    /**
     * Set the module (the distributable, loadable, and executable unit of code in WebAssembly) resource that provides
     * the producer function.
     */
    public void setModule(String module) {
        this.module = module;
    }

    public String getMode() {
        return mode;
    }

    /**
     * How the module is invoked. In function mode (the default) the endpoint calls the exported function named in the
     * URI path using the camel-wasm memory ABI (alloc/dealloc and a JSON envelope of headers and body). In wasi mode
     * the module is a WASI preview1 command (for example a Rust or Go program compiled to wasm32-wasip1): it runs once
     * per exchange with the message body as standard input, and its standard output becomes the message body. The URI
     * path is then passed to the program as its name (argument zero).
     */
    public void setMode(String mode) {
        this.mode = mode;
    }

    public boolean isWasi() {
        return Wasm.MODE_WASI.equalsIgnoreCase(mode);
    }

    public String getArgs() {
        return args;
    }

    /**
     * The command line arguments passed to a WASI program (wasi mode only), separated by whitespace. Use double quotes
     * around an argument that contains whitespace. The arguments are fixed by the route; they cannot be set from
     * message headers.
     */
    public void setArgs(String args) {
        this.args = args;
    }

    public Map<String, Object> getEnvironment() {
        return environment;
    }

    /**
     * Environment variables for a WASI program (wasi mode only), for example environment.LOG_LEVEL=debug. The program
     * sees no environment variables other than these and the ones listed in environmentHeaders; the environment of the
     * JVM is never passed through.
     */
    public void setEnvironment(Map<String, Object> environment) {
        this.environment = environment;
    }

    public String getEnvironmentHeaders() {
        return environmentHeaders;
    }

    /**
     * A comma separated allow-list of message headers whose values are passed to a WASI program as environment
     * variables of the same name (wasi mode only). Headers usually come from whoever sent the message, so no header is
     * passed unless it is listed here. A listed header that is absent is left unset; a header name may not also be
     * configured in environment.
     */
    public void setEnvironmentHeaders(String environmentHeaders) {
        this.environmentHeaders = environmentHeaders;
    }

    public boolean isFailOnNonZeroExit() {
        return failOnNonZeroExit;
    }

    /**
     * Whether a WASI program that exits with a non-zero exit code fails the exchange (wasi mode only). When false, the
     * standard output still replaces the body and the exit code is available in the CamelWasmExitCode header.
     */
    public void setFailOnNonZeroExit(boolean failOnNonZeroExit) {
        this.failOnNonZeroExit = failOnNonZeroExit;
    }

    public int getMaxMemoryPages() {
        return maxMemoryPages;
    }

    /**
     * The maximum size of the module's linear memory, in WebAssembly pages of 64 KiB. Compilers such as Rust and Go
     * declare no maximum by default, which allows a module to grow to 4 GiB of JVM heap. When the limit is reached,
     * memory growth fails inside the module, which then usually traps or exits with an error. 0 (the default) keeps the
     * limit declared by the module. The value must not be lower than the initial size the module declares.
     */
    public void setMaxMemoryPages(int maxMemoryPages) {
        this.maxMemoryPages = maxMemoryPages;
    }

    public long getTimeout() {
        return timeout;
    }

    /**
     * The maximum time an invocation of the module may run. When it is exceeded the module is interrupted, the exchange
     * fails with an ExchangeTimedOutException and the instance is discarded. The module runs on a separate thread when
     * a timeout is set. 0 (the default) means no timeout.
     */
    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }

    public boolean isCompile() {
        return compile;
    }

    /**
     * Whether to translate the module to JVM bytecode when the endpoint starts, instead of interpreting it. Compiled
     * modules typically run an order of magnitude faster, at the cost of a slower start (the translation runs once per
     * endpoint) and more metaspace.
     */
    public void setCompile(boolean compile) {
        this.compile = compile;
    }

    // ************************
    //
    // Clone
    //
    // ************************

    public WasmConfiguration copy() {
        try {
            return (WasmConfiguration) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeCamelException(e);
        }
    }
}
