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
package org.apache.camel.processor;

import java.io.IOException;

import org.apache.camel.ContextTestSupport;
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * The failover load balancer fails over only on the exception that failed the attempt and its causes, not on its
 * suppressed exceptions (such as an exception thrown in a doFinally block), the same as onException and doCatch.
 */
class FailOverLoadBalanceSuppressedExceptionTest extends ContextTestSupport {

    @Override
    protected RouteBuilder createRouteBuilder() {
        return new RouteBuilder() {
            @Override
            public void configure() {
                failover("finally");
                from("direct:finally-attempt")
                        .doTry()
                            .throwException(new IOException("original"))
                        .doFinally()
                            .throwException(new IllegalArgumentException("cleanup"))
                        .end();

                failover("finallyRoute");
                from("direct:finallyRoute-attempt")
                        .doTry()
                            .throwException(new IOException("original"))
                        .doFinally()
                            .to("direct:cleanup")
                        .end();
                from("direct:cleanup")
                        .throwException(new IllegalArgumentException("cleanup"));

                failover("suppressed");
                from("direct:suppressed-attempt")
                        .process(e -> {
                            IOException original = new IOException("original");
                            original.addSuppressed(new IllegalArgumentException("cleanup"));
                            throw original;
                        });

                failover("cause");
                from("direct:cause-attempt")
                        .process(e -> {
                            throw new IOException("original", new IllegalArgumentException("cause"));
                        });

                failover("monitored");
                from("direct:monitored-attempt")
                        .throwException(new IllegalArgumentException("original"));

                failover("monitoredFinally");
                from("direct:monitoredFinally-attempt")
                        .doTry()
                            .throwException(new IllegalArgumentException("original"))
                        .doFinally()
                            .throwException(new IOException("cleanup"))
                        .end();

                failover("notMonitored");
                from("direct:notMonitored-attempt")
                        .throwException(new IOException("original"));

                from("direct:onException")
                        .onException(IllegalArgumentException.class).handled(true).to("mock:onException").end()
                        .doTry()
                            .throwException(new IOException("original"))
                        .doFinally()
                            .throwException(new IllegalArgumentException("cleanup"))
                        .end();
            }

            private void failover(String name) {
                from("direct:" + name)
                        .loadBalance().failover(IllegalArgumentException.class)
                            .to("direct:" + name + "-attempt")
                            .to("mock:second")
                        .end()
                        .to("mock:after");
            }
        };
    }

    @Test
    void doFinallyThrowsMonitoredException() throws Exception {
        assertNoFailover("direct:finally");
    }

    @Test
    void doFinallyCallsRouteThatThrowsMonitoredException() throws Exception {
        assertNoFailover("direct:finallyRoute");
    }

    @Test
    void processorAddsMonitoredExceptionAsSuppressed() throws Exception {
        assertNoFailover("direct:suppressed");
    }

    @Test
    void monitoredExceptionAsCause() throws Exception {
        assertFailover("direct:cause");
    }

    @Test
    void monitoredExceptionThrown() throws Exception {
        assertFailover("direct:monitored");
    }

    @Test
    void monitoredExceptionThrownWhenDoFinallyThrowsAnother() throws Exception {
        assertFailover("direct:monitoredFinally");
    }

    @Test
    void notMonitoredException() throws Exception {
        getMockEndpoint("mock:second").expectedMessageCount(0);
        getMockEndpoint("mock:after").expectedMessageCount(0);

        Exchange out = template.send("direct:notMonitored", e -> e.getMessage().setBody("Hello"));

        assertInstanceOf(IOException.class, out.getException());
        assertEquals("original", out.getException().getMessage());
        assertMockEndpointsSatisfied();
    }

    @Test
    void onExceptionDoesNotMatchSuppressedException() throws Exception {
        getMockEndpoint("mock:onException").expectedMessageCount(0);

        Exchange out = template.send("direct:onException", e -> e.getMessage().setBody("Hello"));

        assertInstanceOf(IOException.class, out.getException());
        assertEquals("original", out.getException().getMessage());
        assertMockEndpointsSatisfied();
    }

    private void assertNoFailover(String uri) throws Exception {
        getMockEndpoint("mock:second").expectedMessageCount(0);
        getMockEndpoint("mock:after").expectedMessageCount(0);

        Exchange out = template.send(uri, e -> e.getMessage().setBody("Hello"));

        // the exchange fails with the exception that failed the attempt
        assertInstanceOf(IOException.class, out.getException());
        assertEquals("original", out.getException().getMessage());
        assertEquals(1, out.getException().getSuppressed().length);
        assertInstanceOf(IllegalArgumentException.class, out.getException().getSuppressed()[0]);
        assertEquals("cleanup", out.getException().getSuppressed()[0].getMessage());
        assertMockEndpointsSatisfied();
    }

    private void assertFailover(String uri) throws Exception {
        getMockEndpoint("mock:second").expectedBodiesReceived("Hello");
        getMockEndpoint("mock:after").expectedBodiesReceived("Hello");

        Exchange out = template.send(uri, e -> e.getMessage().setBody("Hello"));

        assertNull(out.getException());
        assertMockEndpointsSatisfied();
    }
}
