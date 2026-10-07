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

import org.apache.camel.spi.Metadata;

public final class Wasm {
    public static final String SCHEME = "wasm";
    public static final String FN_ALLOC = "alloc";
    public static final String FN_DEALLOC = "dealloc";

    public static final String MODE_FUNCTION = "function";
    public static final String MODE_WASI = "wasi";

    private Wasm() {
    }

    public static class Headers {

        @Metadata(label = "producer",
                  description = "The exit code of the WASI program (wasi mode only). Always set by the component, so a"
                                + " value carried by an inbound message never survives the call.",
                  javaType = "Integer")
        public static final String EXIT_CODE = "CamelWasmExitCode";

        @Metadata(label = "producer",
                  description = "What the WASI program wrote to standard error, decoded as UTF-8 and truncated to 8 KiB"
                                + " (wasi mode only). Removed when the program wrote nothing.",
                  javaType = "String")
        public static final String STDERR = "CamelWasmStderr";
    }

}
