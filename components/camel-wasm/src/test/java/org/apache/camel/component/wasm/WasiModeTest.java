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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.ExchangeTimedOutException;
import org.apache.camel.FluentProducerTemplate;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.wasm.WasiExitCodeException;
import org.apache.camel.wasm.Wasm;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import run.endive.runtime.TrapException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * mode=wasi with wasi_guest.wasm, a WASI preview1 command module built from src/test/rust-wasi.
 */
public class WasiModeTest {

    private static final String GUEST = "wasm:guest?module=wasi_guest.wasm&mode=wasi";

    private CamelContext context;

    @AfterEach
    public void stop() throws Exception {
        if (context != null) {
            context.close();
        }
    }

    private FluentProducerTemplate template;

    /** A template aimed at the route; a fluent template forgets its endpoint after each request. */
    private FluentProducerTemplate pt() {
        return template.to("direct:in");
    }

    private void start(String uri) throws Exception {
        context = new DefaultCamelContext();
        context.addRoutes(new RouteBuilder() {
            @Override
            public void configure() {
                from("direct:in").to(uri);
            }
        });
        context.start();
        template = context.createFluentProducerTemplate();
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    public void bodyIsStdinAndStdoutIsBody(boolean compile) throws Exception {
        start(GUEST + "&compile=" + compile);

        Exchange out = pt().withBody("hello camel")
                .withHeader("foo", "bar")
                .withHeader(Wasm.Headers.STDERR, "forged by the sender")
                .request(Exchange.class);

        assertThat(out.getException()).isNull();
        assertThat(out.getMessage().getBody(String.class)).isEqualTo("HELLO CAMEL");
        assertThat(out.getMessage().getHeader(Wasm.Headers.EXIT_CODE)).isEqualTo(0);
        assertThat(out.getMessage().getHeaders())
                .containsEntry("foo", "bar")
                .doesNotContainKey(Wasm.Headers.STDERR);
    }

    @Test
    public void emptyBodyIsEmptyStdin() throws Exception {
        start(GUEST);

        Exchange out = pt().withBody(null).request(Exchange.class);

        assertThat(out.getException()).isNull();
        assertThat(out.getMessage().getBody(byte[].class)).isEmpty();
    }

    @Test
    public void largeBodyIsStreamed() throws Exception {
        start(GUEST);
        byte[] body = new byte[1024 * 1024];
        Arrays.fill(body, (byte) 'a');

        Exchange out = pt().withBody(body).request(Exchange.class);

        byte[] expected = new byte[body.length];
        Arrays.fill(expected, (byte) 'A');
        assertThat(out.getMessage().getBody(byte[].class)).isEqualTo(expected);
    }

    @Test
    public void argumentsComeFromTheRouteOnly() throws Exception {
        start(GUEST + "&args=RAW(args \"two words\" x)");

        Exchange out = pt().withBody("").withHeader("CamelWasmArgs", "injected").request(Exchange.class);

        assertThat(out.getMessage().getBody(String.class)).isEqualTo("guest\nargs\ntwo words\nx\n");
    }

    @Test
    public void environmentIsOnlyWhatIsConfiguredAndAllowListed() throws Exception {
        start(GUEST + "&args=env&environment.STATIC=one&environmentHeaders=TENANT,REGION");

        Exchange out = pt().withBody("")
                .withHeader("TENANT", "acme")
                .withHeader("SECRET", "not for the guest")
                .request(Exchange.class);

        // no JVM environment, no SECRET, no REGION (absent header)
        assertThat(out.getMessage().getBody(String.class)).isEqualTo("STATIC=one\nTENANT=acme\n");
    }

    @Test
    public void headerWithNulIsRejected() throws Exception {
        start(GUEST + "&args=env&environmentHeaders=TENANT");

        Exchange out = pt().withBody("").withHeader("TENANT", "a\u0000b").request(Exchange.class);

        assertThat(out.getException()).isInstanceOf(IllegalArgumentException.class).hasMessageContaining("NUL");
    }

    @Test
    public void environmentAndEnvironmentHeadersMustNotOverlap() {
        assertThatThrownBy(() -> start(GUEST + "&environment.TENANT=x&environmentHeaders=TENANT"))
                .rootCause().hasMessageContaining("both in environment and environmentHeaders");
    }

    @Test
    public void nonZeroExitFailsAndKeepsTheBody() throws Exception {
        start(GUEST + "&args=RAW(exit 3)");

        Exchange out = pt().withBody("hello").request(Exchange.class);

        assertThat(out.getException()).isInstanceOf(WasiExitCodeException.class)
                .hasMessageContaining("exited with code 3: exiting");
        WasiExitCodeException e = (WasiExitCodeException) out.getException();
        assertThat(e.getExitCode()).isEqualTo(3);
        assertThat(e.getStderr()).isEqualTo("exiting");
        assertThat(out.getMessage().getBody(String.class)).isEqualTo("hello");
        assertThat(out.getMessage().getHeader(Wasm.Headers.EXIT_CODE)).isEqualTo(3);
    }

    @Test
    public void nonZeroExitCanBeTolerated() throws Exception {
        start(GUEST + "&args=RAW(exit 3)&failOnNonZeroExit=false");

        Exchange out = pt().withBody("hello").request(Exchange.class);

        assertThat(out.getException()).isNull();
        assertThat(out.getMessage().getHeader(Wasm.Headers.EXIT_CODE)).isEqualTo(3);
        assertThat(out.getMessage().getHeader(Wasm.Headers.STDERR)).isEqualTo("exiting");
        assertThat(out.getMessage().getBody(byte[].class)).isEmpty();
    }

    @Test
    public void stderrIsCapturedAndBounded() throws Exception {
        start(GUEST + "&args=stderr");

        Exchange out = pt().withBody("diagnostics").request(Exchange.class);
        assertThat(out.getMessage().getBody(String.class)).isEqualTo("ok");
        assertThat(out.getMessage().getHeader(Wasm.Headers.STDERR)).isEqualTo("diagnostics");

        out = pt().withBody("x".repeat(100_000)).request(Exchange.class);
        assertThat(out.getMessage().getHeader(Wasm.Headers.STDERR, String.class)).hasSize(8 * 1024);
    }

    @Test
    public void noFilesystemAccess() throws Exception {
        start(GUEST + "&args=fs");

        Exchange out = pt().withBody("").request(Exchange.class);

        // 8 = EBADF: there is no preopened directory at file descriptor 3
        assertThat(out.getMessage().getBody(String.class)).isEqualTo("no preopened directory, errno 8");
    }

    @Test
    public void trapFailsTheExchange() throws Exception {
        start(GUEST + "&args=trap");

        Exchange out = pt().withBody("").request(Exchange.class);

        assertThat(out.getException()).isInstanceOf(TrapException.class);
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    public void timeoutInterruptsARunawayProgram(boolean compile) throws Exception {
        start(GUEST + "&args=spin&timeout=300&compile=" + compile);

        for (int i = 0; i < 2; i++) {
            long t0 = System.nanoTime();
            Exchange out = pt().withBody("").request(Exchange.class);
            long ms = (System.nanoTime() - t0) / 1_000_000;

            assertThat(out.getException()).isInstanceOf(ExchangeTimedOutException.class)
                    .hasMessageContaining("did not complete within 300 ms");
            assertThat(ms).isLessThan(5000);
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    public void memoryIsCapped(boolean compile) throws Exception {
        start(GUEST + "&args=hog&maxMemoryPages=8&compile=" + compile);

        Exchange out = pt().withBody("").request(Exchange.class);

        assertThat(out.getMessage().getBody(String.class)).isEqualTo("8");
    }

    @Test
    public void maxMemoryPagesBelowTheInitialSizeIsRejected() {
        assertThatThrownBy(() -> start(GUEST + "&maxMemoryPages=1"))
                .rootCause().hasMessageContaining("lower than the");
    }

    @Test
    public void functionModuleIsNotACommand() {
        assertThatThrownBy(() -> start("wasm:process?module=functions.wasm&mode=wasi"))
                .rootCause().hasMessageContaining("no _start function");
    }

    @Test
    public void wasiOptionsAreRejectedInFunctionMode() {
        assertThatThrownBy(() -> start("wasm:process?module=functions.wasm&args=x"))
                .rootCause().hasMessageContaining("only apply to mode=wasi");
    }

    @Test
    public void concurrentExchangesDoNotShareState() throws Exception {
        start(GUEST);
        ProducerTemplate template = context.createProducerTemplate();
        ExecutorService pool = Executors.newFixedThreadPool(8);
        try {
            List<Future<String>> results = new ArrayList<>();
            for (int t = 0; t < 8; t++) {
                for (int i = 0; i < 50; i++) {
                    String body = "msg-" + t + "-" + i + "-" + "x".repeat(i * 37);
                    results.add(pool.submit(() -> {
                        String out = template.requestBody("direct:in", body, String.class);
                        return out.equals(body.toUpperCase()) ? "ok" : "mismatch: " + out;
                    }));
                }
            }
            for (Future<String> f : results) {
                assertThat(f.get()).isEqualTo("ok");
            }
        } finally {
            pool.shutdownNow();
        }
    }
}
