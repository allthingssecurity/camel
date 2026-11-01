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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.camel.ContextTestSupport;
import org.apache.camel.Exchange;
import org.apache.camel.builder.NotifyBuilder;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.support.SynchronizationAdapter;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The parts of a parallel split with shareUnitOfWork use the same unit of work at the same time, so a part that hands
 * over the on completions (such as sending to a seda endpoint) must not fail, or hand over an on completion twice, when
 * another part adds or hands over on completions at the same time.
 */
public class SplitParallelShareUnitOfWorkHandoverTest extends ContextTestSupport {

    private final CountingSynchronization consumer = new CountingSynchronization();
    private final CountingSynchronization part = new CountingSynchronization();
    // the number of handovers of the consumer on completion that wait until the other part has done its work
    private final AtomicInteger pausedHandovers = new AtomicInteger();
    private final CountDownLatch handoverStarted = new CountDownLatch(1);
    private final CountDownLatch partAdded = new CountDownLatch(1);
    private final CyclicBarrier bothInHandover = new CyclicBarrier(2);
    private volatile boolean barrier;

    @Test
    public void testHandoverWhileAnotherPartAddsOnCompletion() throws Exception {
        pausedHandovers.set(1);
        NotifyBuilder seda = new NotifyBuilder(context).from("seda:a").whenDone(1).create();

        Exchange out = template.send("direct:handoverAndAdd", e -> e.getIn().setBody("A,B"));

        assertNull(out.getException(), "the split must not fail");
        assertTrue(seda.matches(10, TimeUnit.SECONDS));
        assertEquals(1, consumer.completed.get(), "the consumer on completion runs once, with the seda copy");
        assertEquals(0, consumer.failed.get());
        assertEquals(1, part.completed.get(), "the on completion of the part runs once, with the split exchange");
        assertEquals(0, part.failed.get());
    }

    @Test
    public void testTwoPartsHandoverAtTheSameTime() throws Exception {
        barrier = true;
        pausedHandovers.set(2);
        NotifyBuilder seda = new NotifyBuilder(context).from("seda:a").whenDone(2).create();

        Exchange out = template.send("direct:twoHandovers", e -> e.getIn().setBody("A,B"));

        assertNull(out.getException(), "the split must not fail");
        assertTrue(seda.matches(10, TimeUnit.SECONDS));
        assertEquals(1, consumer.completed.get() + consumer.failed.get(),
                "the consumer on completion is handed over to one seda copy only, and runs once");
        assertEquals(0, consumer.failed.get());
    }

    @Test
    public void testSequentialSplit() throws Exception {
        // control: one part at a time
        NotifyBuilder seda = new NotifyBuilder(context).from("seda:a").whenDone(2).create();

        Exchange out = template.send("direct:sequential", e -> e.getIn().setBody("A,B"));

        assertNull(out.getException());
        assertTrue(seda.matches(10, TimeUnit.SECONDS));
        assertEquals(1, consumer.completed.get());
        assertEquals(0, consumer.failed.get());
    }

    @Override
    protected RouteBuilder createRouteBuilder() {
        return new RouteBuilder() {
            @Override
            public void configure() {
                // the consumer on completion is added before the split (such as by a file or JMS consumer)
                from("direct:handoverAndAdd")
                        .process(e -> e.getExchangeExtension().addOnCompletion(consumer))
                        .split(body().tokenize(",")).parallelProcessing().shareUnitOfWork()
                        .choice()
                        .when(body().isEqualTo("A")).to("seda:a")
                        .otherwise().process(e -> {
                            // another part adds an on completion while part A hands over
                            assertTrue(handoverStarted.await(10, TimeUnit.SECONDS));
                            e.getExchangeExtension().addOnCompletion(part);
                            partAdded.countDown();
                        })
                        .end()
                        .end()
                        .to("mock:result");

                from("direct:twoHandovers")
                        .process(e -> e.getExchangeExtension().addOnCompletion(consumer))
                        .split(body().tokenize(",")).parallelProcessing().shareUnitOfWork()
                            .to("seda:a")
                        .end()
                        .to("mock:result");

                from("direct:sequential")
                        .process(e -> e.getExchangeExtension().addOnCompletion(consumer))
                        .split(body().tokenize(",")).shareUnitOfWork()
                            .to("seda:a")
                        .end()
                        .to("mock:result");

                from("seda:a").to("mock:a");
            }
        };
    }

    private final class CountingSynchronization extends SynchronizationAdapter {
        private final AtomicInteger completed = new AtomicInteger();
        private final AtomicInteger failed = new AtomicInteger();

        @Override
        public void onComplete(Exchange exchange) {
            completed.incrementAndGet();
        }

        @Override
        public void onFailure(Exchange exchange) {
            failed.incrementAndGet();
        }

        @Override
        public boolean allowHandover() {
            // the handover asks the on completion whether it may be handed over, so it can hold the handover
            // at that point until the other part has done its work
            if (this == consumer && pausedHandovers.getAndDecrement() > 0) {
                try {
                    if (barrier) {
                        bothInHandover.await(10, TimeUnit.SECONDS);
                    } else {
                        handoverStarted.countDown();
                        assertTrue(partAdded.await(10, TimeUnit.SECONDS));
                    }
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }
            return true;
        }
    }
}
