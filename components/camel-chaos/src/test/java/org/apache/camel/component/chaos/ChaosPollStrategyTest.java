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
package org.apache.camel.component.chaos;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.camel.CamelContext;
import org.apache.camel.Consumer;
import org.apache.camel.Endpoint;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.spi.PollingConsumerPollStrategy;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The poll strategy of chaos on its own: a delay that is interrupted (the consumer is stopping) skips the poll and
 * keeps the interrupted status. The virtual clock is static, so this class runs alone.
 */
@Isolated
class ChaosPollStrategyTest {

    private CamelContext context;
    private FakeClock clock;

    @BeforeEach
    void start() {
        clock = FakeClock.install();
        context = new DefaultCamelContext();
        context.getPropertiesComponent().addInitialProperty("camel.chaos.enabled", "true");
        context.start();
    }

    @AfterEach
    void stop() {
        try {
            context.stop();
        } finally {
            clock.close();
        }
    }

    @Test
    void anInterruptedDelaySkipsThePollAndKeepsTheInterrupt() {
        ChaosManager chaos = ChaosManager.get(context);
        String id = chaos.addRule("endpoint=testpoll:p* delay duration=1s");
        Counting delegate = new Counting();
        ChaosPollStrategy strategy = new ChaosPollStrategy(chaos, delegate);
        Endpoint endpoint = context.getEndpoint("mock:x");

        // the endpoint does not match: the strategy of the consumer is asked
        assertThat(strategy.begin(null, endpoint)).isTrue();
        assertThat(delegate.begins.get()).isEqualTo(1);

        TestPollComponent component = new TestPollComponent();
        context.addComponent("testpoll", component);
        Endpoint polled = context.getEndpoint("testpoll:p");
        Thread.currentThread().interrupt();
        try {
            assertThat(strategy.begin(null, polled)).isFalse();
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            Thread.interrupted();
        }
        // the poll was skipped before the strategy of the consumer was asked
        assertThat(delegate.begins.get()).isEqualTo(1);
        assertThat(clock.sleeps()).isEmpty();

        // not interrupted: it waits, then asks the strategy of the consumer
        assertThat(strategy.begin(null, polled)).isTrue();
        assertThat(clock.sleeps()).containsExactly(1000L);
        assertThat(delegate.begins.get()).isEqualTo(2);
        assertThat(chaos.report().toJson().toJson()).contains("\"id\":\"" + id + "\"");
    }

    @Test
    void withoutAStrategyTheDefaultOneIsUsed() {
        ChaosPollStrategy strategy = new ChaosPollStrategy(ChaosManager.get(context), null);
        assertThat(strategy.getDelegate()).isNotNull();
        assertThat(strategy.begin(null, context.getEndpoint("mock:x"))).isTrue();
    }

    private static final class Counting implements PollingConsumerPollStrategy {
        final AtomicInteger begins = new AtomicInteger();

        @Override
        public boolean begin(Consumer consumer, Endpoint endpoint) {
            begins.incrementAndGet();
            return true;
        }

        @Override
        public void commit(Consumer consumer, Endpoint endpoint, int polledMessages) {
            // nothing
        }

        @Override
        public boolean rollback(Consumer consumer, Endpoint endpoint, int retryCounter, Exception cause) {
            return false;
        }
    }
}
