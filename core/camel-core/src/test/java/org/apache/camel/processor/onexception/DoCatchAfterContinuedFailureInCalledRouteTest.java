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
package org.apache.camel.processor.onexception;

import java.time.Duration;

import org.apache.camel.ContextTestSupport;
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.model.CatchDefinition;
import org.apache.camel.support.DefaultExchange;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A route called from a doTry body continues a failure ({@code onException(...).continued(true)}). A later exception in
 * the same doTry body must still be caught by the doCatch.
 */
class DoCatchAfterContinuedFailureInCalledRouteTest extends ContextTestSupport {

    @Override
    public boolean isUseRouteBuilder() {
        return false;
    }

    private Exchange send(String call, boolean redelivery) throws Exception {
        context.addRoutes(new RouteBuilder() {
            @Override
            public void configure() {
                if (redelivery) {
                    onException(NumberFormatException.class).maximumRedeliveries(1).redeliveryDelay(0).continued(true);
                } else {
                    onException(NumberFormatException.class).continued(true);
                }

                var body = from("direct:start").doTry();
                switch (call) {
                    case "to" -> body.to("direct:callee");
                    case "routingSlip" -> body.routingSlip(constant("direct:callee"));
                    case "recipientList" -> body.recipientList(constant("direct:callee"));
                    case "calleeOk" -> body.to("direct:calleeOk");
                    default -> {
                        // nothing before the exception
                    }
                }
                body.to("mock:next")
                        .throwException(new IllegalStateException("after the call"))
                        .doCatch(IllegalArgumentException.class)
                        .to("mock:otherCatch")
                        .doCatch(IllegalStateException.class)
                        .to("mock:catch")
                        .doCatch(Exception.class)
                        .to("mock:lastCatch")
                        .end()
                        .to("mock:result");

                from("direct:callee").throwException(new NumberFormatException("in callee")).to("mock:calleeEnd");
                from("direct:calleeOk").to("mock:calleeEnd");
            }
        });
        context.start();

        return assertTimeoutPreemptively(Duration.ofSeconds(10),
                () -> template.send("direct:start", e -> e.getMessage().setBody("Hello")), "the send did not complete");
    }

    private void assertCaughtOnce(Exchange out) {
        assertNull(out.getException(), "the doCatch handles the exception");
        assertEquals(1, getMockEndpoint("mock:next").getReceivedCounter(), "mock:next");
        assertEquals(1, getMockEndpoint("mock:catch").getReceivedCounter(), "mock:catch");
        // only the first matching doCatch runs
        assertEquals(0, getMockEndpoint("mock:otherCatch").getReceivedCounter(), "mock:otherCatch");
        assertEquals(0, getMockEndpoint("mock:lastCatch").getReceivedCounter(), "mock:lastCatch");
        assertEquals(1, getMockEndpoint("mock:result").getReceivedCounter(), "mock:result");
        // the doCatch sees the exception it caught, not the continued one
        Exchange caught = getMockEndpoint("mock:catch").getReceivedExchanges().get(0);
        Exception e = caught.getProperty(Exchange.EXCEPTION_CAUGHT, Exception.class);
        assertInstanceOf(IllegalStateException.class, e);
        assertEquals("after the call", e.getMessage());
    }

    @Test
    void continuedInCalledRoute() throws Exception {
        Exchange out = send("to", false);
        // the called route continued after its failure
        assertEquals(1, getMockEndpoint("mock:calleeEnd").getReceivedCounter(), "mock:calleeEnd");
        assertCaughtOnce(out);
    }

    @Test
    void continuedInRoutingSlip() throws Exception {
        assertCaughtOnce(send("routingSlip", false));
    }

    @Test
    void continuedInRecipientList() throws Exception {
        assertCaughtOnce(send("recipientList", false));
    }

    @Test
    void continuedAfterRedeliveryInCalledRoute() throws Exception {
        assertCaughtOnce(send("to", true));
    }

    @Test
    void controlNothingBeforeTheException() throws Exception {
        assertCaughtOnce(send("none", false));
    }

    @Test
    void controlCalledRouteDoesNotFail() throws Exception {
        Exchange out = send("calleeOk", false);
        assertEquals(1, getMockEndpoint("mock:calleeEnd").getReceivedCounter(), "mock:calleeEnd");
        assertCaughtOnce(out);
    }

    @Test
    void debuggerAcceptsCatchAfterContinuedFailure() {
        CatchDefinition doCatch = new CatchDefinition();

        Exchange exchange = new DefaultExchange(context);
        exchange.setException(new IllegalStateException("after the call"));
        // the error handler stores the exception of a continued failure
        exchange.setProperty(Exchange.EXCEPTION_HANDLED, new NumberFormatException("in callee"));
        assertTrue(doCatch.acceptDebugger(exchange), "a continued failure is not a previous doCatch");

        // a previous doCatch of the same doTry handled the exception
        exchange.setProperty(Exchange.EXCEPTION_HANDLED, true);
        assertFalse(doCatch.acceptDebugger(exchange), "a previous doCatch handled the exception");
    }
}
