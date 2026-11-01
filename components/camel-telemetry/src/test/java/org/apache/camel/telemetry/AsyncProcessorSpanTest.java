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
package org.apache.camel.telemetry;

import java.util.List;
import java.util.Map;

import org.apache.camel.CamelContext;
import org.apache.camel.CamelContextAware;
import org.apache.camel.RoutesBuilder;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.model.RouteDefinition;
import org.apache.camel.telemetry.mock.MockSpanAdapter;
import org.apache.camel.telemetry.mock.MockTrace;
import org.apache.camel.telemetry.mock.MockTracer;
import org.apache.camel.test.junit6.ExchangeTestSupport;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The span of a step that completes on another thread (an asynchronous delay, the threads EIP) ends before the next
 * step of the route starts, so the next steps are children of the route span and not of the asynchronous step.
 */
class AsyncProcessorSpanTest extends ExchangeTestSupport {

    MockTracer mockTracer;

    @Override
    protected CamelContext createCamelContext() throws Exception {
        CamelContext context = super.createCamelContext();
        this.mockTracer = new MockTracer();
        CamelContextAware.trySetCamelContext(mockTracer, context);
        mockTracer.init(context);
        return context;
    }

    @Test
    void stepsAfterAsyncDelayAreChildrenOfTheRoute() {
        checkStepsAfter("direct:delay", "async-delay", "delay");
    }

    @Test
    void stepsAfterThreadsAreChildrenOfTheRoute() {
        checkStepsAfter("direct:threads", "threads-threads", "threads");
    }

    @Test
    void stepsAfterSyncDelayAreChildrenOfTheRoute() {
        // control: a delay that completes on the same thread
        checkStepsAfter("direct:syncdelay", "sync-delay", "sync");
    }

    @Test
    void stepsInsideChoiceAfterAsyncDelay() {
        template.sendBody("direct:choice", "my-body");
        Map<String, MockTrace> traces = mockTracer.traces();
        assertEquals(1, traces.size());
        List<Span> spans = traces.values().iterator().next().spans();
        MockSpanAdapter route = span(spans, "camel-direct", Op.EVENT_RECEIVED);
        MockSpanAdapter choice = span(spans, "choice-choice", Op.EVENT_PROCESS);
        MockSpanAdapter delay = span(spans, "innerDelay-delay", Op.EVENT_PROCESS);
        MockSpanAdapter inner = span(spans, "inner-setHeader", Op.EVENT_PROCESS);
        MockSpanAdapter after = span(spans, "after-setHeader", Op.EVENT_PROCESS);
        allDone(spans);
        assertEquals(route.getTag("spanid"), choice.getTag("parentSpan"));
        assertEquals(choice.getTag("spanid"), delay.getTag("parentSpan"));
        assertEquals(choice.getTag("spanid"), inner.getTag("parentSpan"));
        assertEquals(route.getTag("spanid"), after.getTag("parentSpan"));
        endsBeforeStart(delay, inner);
        endsBeforeStart(choice, after);
    }

    @Test
    void calledRouteWithAsyncDelay() {
        template.sendBody("direct:caller", "my-body");
        Map<String, MockTrace> traces = mockTracer.traces();
        assertEquals(1, traces.size());
        List<Span> spans = traces.values().iterator().next().spans();
        // the template's send, the caller route, the send to the called route, the called route
        assertEquals(Op.EVENT_SENT.toString(), ((MockSpanAdapter) spans.get(0)).getTag("op"));
        MockSpanAdapter caller = (MockSpanAdapter) spans.get(1);
        MockSpanAdapter send = (MockSpanAdapter) spans.get(2);
        MockSpanAdapter called = (MockSpanAdapter) spans.get(3);
        assertEquals(Op.EVENT_RECEIVED.toString(), caller.getTag("op"));
        assertEquals(Op.EVENT_SENT.toString(), send.getTag("op"));
        assertEquals(Op.EVENT_RECEIVED.toString(), called.getTag("op"));
        MockSpanAdapter calledDelay = span(spans, "calledDelay-delay", Op.EVENT_PROCESS);
        MockSpanAdapter calledStep = span(spans, "calledStep-setHeader", Op.EVENT_PROCESS);
        MockSpanAdapter callerStep = span(spans, "callerStep-setHeader", Op.EVENT_PROCESS);
        allDone(spans);
        assertEquals(caller.getTag("spanid"), send.getTag("parentSpan"));
        assertEquals(send.getTag("spanid"), called.getTag("parentSpan"));
        assertEquals(called.getTag("spanid"), calledDelay.getTag("parentSpan"));
        assertEquals(called.getTag("spanid"), calledStep.getTag("parentSpan"));
        assertEquals(caller.getTag("spanid"), callerStep.getTag("parentSpan"));
        // each span is ended by its own step: the called route and the send end after the steps they contain
        endsBeforeStart(calledDelay, calledStep);
        endsNotBefore(called, calledStep);
        endsNotBefore(send, called);
        endsBeforeStart(send, callerStep);
    }

    private void checkStepsAfter(String uri, String asyncStep, String prefix) {
        template.sendBody(uri, "my-body");
        Map<String, MockTrace> traces = mockTracer.traces();
        assertEquals(1, traces.size());
        List<Span> spans = traces.values().iterator().next().spans();
        MockSpanAdapter route = span(spans, "camel-direct", Op.EVENT_RECEIVED);
        MockSpanAdapter async = span(spans, asyncStep, Op.EVENT_PROCESS);
        MockSpanAdapter step2 = span(spans, prefix + "Step2-setHeader", Op.EVENT_PROCESS);
        MockSpanAdapter step3 = span(spans, prefix + "Step3-setHeader", Op.EVENT_PROCESS);
        allDone(spans);
        assertEquals(route.getTag("spanid"), async.getTag("parentSpan"));
        assertEquals(route.getTag("spanid"), step2.getTag("parentSpan"));
        assertEquals(route.getTag("spanid"), step3.getTag("parentSpan"));
        endsBeforeStart(async, step2);
    }

    private static MockSpanAdapter span(List<Span> spans, String component, Op op) {
        MockSpanAdapter answer = null;
        for (Span s : spans) {
            MockSpanAdapter msa = (MockSpanAdapter) s;
            if (component.equals(msa.getTag(TagConstants.COMPONENT)) && op.toString().equals(msa.getTag("op"))) {
                answer = msa;
            }
        }
        assertNotNull(answer, "No span " + component + " " + op + " in " + spans);
        return answer;
    }

    private static void allDone(List<Span> spans) {
        for (Span s : spans) {
            assertEquals("true", ((MockSpanAdapter) s).getTag("isDone"), "Span not ended: " + s);
        }
    }

    private static void endsBeforeStart(MockSpanAdapter first, MockSpanAdapter next) {
        long end = Long.parseLong(first.getTag("endTimestamp"));
        long start = Long.parseLong(next.getTag("initTimestamp"));
        assertTrue(end <= start, "The span " + first.tags() + " ends after the span " + next.tags() + " starts");
    }

    private static void endsNotBefore(MockSpanAdapter outer, MockSpanAdapter inner) {
        long outerEnd = Long.parseLong(outer.getTag("endTimestamp"));
        long innerEnd = Long.parseLong(inner.getTag("endTimestamp"));
        assertTrue(outerEnd >= innerEnd, "The span " + outer.tags() + " ends before the span " + inner.tags());
    }

    @Override
    protected RoutesBuilder createRouteBuilder() {
        return new RouteBuilder() {
            @Override
            public void configure() {
                // a delay is asynchronous by default
                from("direct:delay").routeId("delayRoute")
                        .delay(10).id("async")
                        .setHeader("step2", constant("2")).id("delayStep2")
                        .setHeader("step3", constant("3")).id("delayStep3");

                from("direct:threads").routeId("threadsRoute")
                        .threads(1).id("threads")
                        .setHeader("step2", constant("2")).id("threadsStep2")
                        .setHeader("step3", constant("3")).id("threadsStep3");

                from("direct:syncdelay").routeId("syncDelayRoute")
                        .delay(10).syncDelayed().id("sync")
                        .setHeader("step2", constant("2")).id("syncStep2")
                        .setHeader("step3", constant("3")).id("syncStep3");

                RouteDefinition choiceRoute = from("direct:choice").routeId("choiceRoute");
                choiceRoute.choice().id("choice")
                        .when(constant(true))
                        .delay(10).id("innerDelay")
                        .setHeader("inner", constant("x")).id("inner");
                choiceRoute.setHeader("after", constant("y")).id("after");

                from("direct:caller").routeId("callerRoute")
                        .to("direct:called")
                        .setHeader("caller", constant("x")).id("callerStep");

                from("direct:called").routeId("calledRoute")
                        .delay(10).id("calledDelay")
                        .setHeader("called", constant("y")).id("calledStep");
            }
        };
    }

}
