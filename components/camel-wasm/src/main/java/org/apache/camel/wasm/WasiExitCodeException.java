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

import org.apache.camel.CamelExchangeException;
import org.apache.camel.Exchange;

/**
 * Thrown when a WASI program exits with a non-zero exit code and the endpoint is configured to fail on that
 * (failOnNonZeroExit, the default). The message body is left as it was before the call.
 */
public class WasiExitCodeException extends CamelExchangeException {

    private static final int MAX_STDERR_IN_MESSAGE = 256;

    private final int exitCode;
    private final String stderr;

    public WasiExitCodeException(String program, int exitCode, String stderr, Exchange exchange) {
        super(message(program, exitCode, stderr), exchange);
        this.exitCode = exitCode;
        this.stderr = stderr;
    }

    public int getExitCode() {
        return exitCode;
    }

    /**
     * What the program wrote to standard error (at most 8 KiB).
     */
    public String getStderr() {
        return stderr;
    }

    private static String message(String program, int exitCode, String stderr) {
        StringBuilder sb = new StringBuilder("WASI program ").append(program).append(" exited with code ").append(exitCode);
        if (stderr != null && !stderr.isBlank()) {
            String s = stderr.strip();
            sb.append(": ").append(s.length() > MAX_STDERR_IN_MESSAGE ? s.substring(0, MAX_STDERR_IN_MESSAGE) + "..." : s);
        }
        return sb.toString();
    }
}
