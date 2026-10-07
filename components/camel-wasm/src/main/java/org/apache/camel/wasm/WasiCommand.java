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

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import run.endive.runtime.ImportValues;
import run.endive.runtime.Instance;
import run.endive.runtime.TrapException;
import run.endive.wasi.WasiExitException;
import run.endive.wasi.WasiOptions;
import run.endive.wasi.WasiPreview1;
import run.endive.wasm.types.Export;
import run.endive.wasm.types.ExportSection;
import run.endive.wasm.types.ExternalType;

/**
 * Runs a WASI preview1 command module (a program with a {@code _start} export, such as Rust or Go compiled to
 * {@code wasm32-wasip1}) to completion, once per call, on a fresh instance.
 * <p/>
 * The program gets exactly what is passed in: the given standard input, arguments and environment variables. It gets no
 * preopened directories, so it cannot open files, and no sockets. Clock and random numbers are the host's. Each call
 * creates its own instance, so calls never share guest state and need no lock.
 */
public final class WasiCommand {

    public static final String START = "_start";
    public static final String WASI_MODULE = "wasi_snapshot_preview1";
    /** At most this many bytes of standard error are kept. */
    public static final int MAX_STDERR = 8 * 1024;

    private final WasmRuntime runtime;
    private final List<String> arguments;

    public WasiCommand(WasmRuntime runtime, List<String> arguments) {
        this.runtime = Objects.requireNonNull(runtime);
        this.arguments = List.copyOf(arguments);
        validate();
    }

    private void validate() {
        ExportSection exports = runtime.module().exportSection();
        boolean hasStart = false;
        for (int i = 0; i < exports.exportCount(); i++) {
            Export e = exports.getExport(i);
            if (START.equals(e.name()) && e.exportType() == ExternalType.FUNCTION) {
                hasStart = true;
                break;
            }
        }
        if (!hasStart) {
            throw new IllegalArgumentException(
                    "The module has no " + START + " function, so it is not a WASI command module."
                                               + " Build it as a program (for example cargo build --target wasm32-wasip1"
                                               + " of a binary crate, or GOOS=wasip1 go build), or use mode=function");
        }
        String unsupported = runtime.module().importSection().stream()
                .filter(i -> !WASI_MODULE.equals(i.module()))
                .map(i -> i.module() + "." + i.name())
                .distinct()
                .collect(Collectors.joining(", "));
        if (!unsupported.isEmpty()) {
            throw new IllegalArgumentException(
                    "The module imports functions that are not part of WASI preview1 (" + WASI_MODULE
                                               + "), which cannot be provided: " + unsupported);
        }
    }

    /**
     * Runs the program once.
     *
     * @param  stdin                                       the program's standard input
     * @param  environment                                 the program's complete environment
     * @return                                             the exit code and the captured output
     * @throws TrapException                               if the program trapped (for example a Rust panic)
     * @throws run.endive.runtime.WasmInterruptedException if the calling thread was interrupted
     */
    public Result run(InputStream stdin, Map<String, String> environment) {
        ByteArrayOutputStream stdout = new ByteArrayOutputStream();
        BoundedOutputStream stderr = new BoundedOutputStream(MAX_STDERR);

        WasiOptions.Builder options = WasiOptions.builder()
                .withStdin(stdin)
                .withStdout(stdout)
                .withStderr(stderr)
                .withArguments(arguments);
        environment.forEach(options::withEnvironment);

        int exitCode = 0;
        try (WasiPreview1 wasi = WasiPreview1.builder().withOptions(options.build()).build()) {
            Instance instance = runtime.instanceBuilder()
                    .withImportValues(ImportValues.builder().addFunction(wasi.toHostFunctions()).build())
                    .withStart(false)
                    .build();
            try {
                instance.export(START).apply();
            } catch (WasiExitException e) {
                exitCode = e.exitCode();
            } catch (TrapException e) {
                TrapException t = new TrapException(e.getMessage() + WasmRuntime.memoryHint(instance));
                t.initCause(e);
                throw t;
            }
        } catch (WasiExitException e) {
            // proc_exit from a start section or _initialize
            exitCode = e.exitCode();
        }
        return new Result(exitCode, stdout.toByteArray(), stderr.toString(), stderr.truncated);
    }

    public List<String> getArguments() {
        return arguments;
    }

    /**
     * The outcome of one run.
     *
     * @param exitCode        the exit code, 0 when {@code _start} returned normally
     * @param stdout          everything written to standard output
     * @param stderr          what was written to standard error, decoded as UTF-8, at most {@link #MAX_STDERR} bytes
     * @param stderrTruncated whether more was written to standard error than was kept
     */
    public record Result(int exitCode, byte[] stdout, String stderr, boolean stderrTruncated) {
    }

    /**
     * Keeps the first {@code max} bytes written and silently drops the rest, so a chatty or hostile program cannot make
     * the host buffer an unbounded amount of diagnostics.
     */
    static final class BoundedOutputStream extends OutputStream {
        private final ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        private final int max;
        private boolean truncated;

        BoundedOutputStream(int max) {
            this.max = max;
        }

        @Override
        public void write(int b) {
            if (buffer.size() < max) {
                buffer.write(b);
            } else {
                truncated = true;
            }
        }

        @Override
        public void write(byte[] b, int off, int len) {
            int room = max - buffer.size();
            if (len > room) {
                truncated = true;
            }
            if (room > 0) {
                buffer.write(b, off, Math.min(len, room));
            }
        }

        @Override
        public String toString() {
            return buffer.toString(StandardCharsets.UTF_8);
        }
    }

    /**
     * Splits the args option of a WASI endpoint into the program's argument list, with the program name first.
     * Arguments are separated by whitespace; double quotes group an argument that contains whitespace.
     */
    public static List<String> arguments(String programName, String args) {
        List<String> answer = new ArrayList<>();
        answer.add(programName);
        if (args == null) {
            return answer;
        }
        StringBuilder current = null;
        boolean quoted = false;
        for (int i = 0; i < args.length(); i++) {
            char c = args.charAt(i);
            if (c == '"') {
                quoted = !quoted;
                if (current == null) {
                    current = new StringBuilder();
                }
            } else if (Character.isWhitespace(c) && !quoted) {
                if (current != null) {
                    answer.add(current.toString());
                    current = null;
                }
            } else {
                if (current == null) {
                    current = new StringBuilder();
                }
                current.append(c);
            }
        }
        if (quoted) {
            throw new IllegalArgumentException("Unbalanced double quote in args: " + args);
        }
        if (current != null) {
            answer.add(current.toString());
        }
        return answer;
    }

}
