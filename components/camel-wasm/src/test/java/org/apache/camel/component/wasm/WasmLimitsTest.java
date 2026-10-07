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

import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.ExchangeTimedOutException;
import org.apache.camel.FluentProducerTemplate;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.impl.DefaultCamelContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import run.endive.runtime.TrapException;
import run.endive.wasm.Parser;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * maxMemoryPages, timeout and compile in function mode, with limits_guest.wasm built from src/test/rust-wasi. Its
 * misbehave function echoes its input unless the input contains "spin" (loops for ever) or "trap" (grows memory by 8
 * pages, then traps; returns an "out of memory" error instead if memory cannot grow).
 */
public class WasmLimitsTest {

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

    private static int initialPages() throws Exception {
        try (InputStream is = WasmLimitsTest.class.getResourceAsStream("/limits_guest.wasm")) {
            return Parser.parse(is).memorySection().get().getMemory(0).limits().initialPages();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    public void timeoutInterruptsAndTheEndpointRecovers(boolean compile) throws Exception {
        start("wasm:misbehave?module=limits_guest.wasm&timeout=300&compile=" + compile);

        Exchange out = pt().withBody("hello").withHeader("what", "spin").request(Exchange.class);
        assertThat(out.getException()).isInstanceOf(ExchangeTimedOutException.class);

        // the interrupted instance was discarded, a fresh one serves the next exchange
        out = pt().withBody("hello").request(Exchange.class);
        assertThat(out.getException()).isNull();
        assertThat(out.getMessage().getBody(String.class)).isEqualTo("hello");
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    public void trapDiscardsTheInstance(boolean compile) throws Exception {
        // room for one trapping call (8 pages) but not two: if the instance that trapped were reused, the second call
        // could not grow its memory and would answer "out of memory" instead of trapping
        int cap = initialPages() + 12;
        start(
                "wasm:misbehave?module=limits_guest.wasm&maxMemoryPages="
              + cap + "&compile=" + compile);

        for (int i = 0; i < 3; i++) {
            Exchange out = pt().withBody("hello").withHeader("what", "trap").request(Exchange.class);
            assertThat(out.getException()).isInstanceOf(TrapException.class);
        }

        Exchange out = pt().withBody("hello").request(Exchange.class);
        assertThat(out.getMessage().getBody(String.class)).isEqualTo("hello");
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    public void memoryCapIsVisibleToTheGuest(boolean compile) throws Exception {
        // not even one growth of 8 pages fits: the guest reports it through the error flag
        int cap = initialPages() + 4;
        start(
                "wasm:misbehave?module=limits_guest.wasm&maxMemoryPages="
              + cap + "&compile=" + compile);

        Exchange out = pt().withBody("hello").withHeader("what", "trap").request(Exchange.class);

        assertThat(out.getException()).hasMessage("out of memory");
    }
}
