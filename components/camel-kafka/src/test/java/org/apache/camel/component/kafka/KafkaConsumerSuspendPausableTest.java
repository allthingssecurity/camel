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
package org.apache.camel.component.kafka;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

import org.apache.camel.BindToRegistry;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.kafka.consumer.errorhandler.KafkaConsumerListener;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.test.junit6.CamelTestSupport;
import org.apache.camel.test.junit6.TestExecutionConfiguration;
import org.apache.camel.throttling.ThrottlingExceptionRoutePolicy;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Suspending and resuming a route with a Kafka consumer and the pausable EIP ({@link KafkaConsumerListener}), with a
 * {@link MockConsumer} instead of a broker. The suspend wins over the check of the pausable EIP: a suspended route
 * consumes nothing, also when the check holds. A resume does not override the check: the consumer stays paused until
 * the check holds.
 */
class KafkaConsumerSuspendPausableTest extends CamelTestSupport {

    private static final String TOPIC = "suspend-pausable";
    private static final TopicPartition PARTITION = new TopicPartition(TOPIC, 0);
    // enough polls for a record to be returned and processed if the consumer were not paused
    private static final int POLLS = 5;

    private final CountingMockConsumer mockConsumer = new CountingMockConsumer();
    private final ThrottlingExceptionRoutePolicy keepOpenPolicy
            = new ThrottlingExceptionRoutePolicy(1, 60000, 100, null, true);
    // the check of the pausable EIP: whether the system the route depends on is available
    private volatile boolean available = true;
    private long offset;

    @BindToRegistry("mockFactory")
    private final KafkaClientFactory factory = new KafkaClientFactory() {
        @Override
        public Producer getProducer(Properties kafkaProps) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Consumer getConsumer(Properties kafkaProps) {
            return mockConsumer;
        }

        @Override
        public String getBrokers(KafkaConfiguration configuration) {
            return "localhost:9092";
        }
    };

    @Override
    public void configureTest(TestExecutionConfiguration testExecutionConfiguration) {
        testExecutionConfiguration.withUseRouteBuilder(false);
    }

    @Test
    void testSuspendedRouteConsumesNothingWhileTheCheckHolds() throws Exception {
        startRoute(false);
        MockEndpoint mock = getMockEndpoint("mock:result");
        mock.expectedBodiesReceived("a");
        addRecord("a");
        mock.assertIsSatisfied();
        mock.reset();

        KafkaConsumer consumer = kafkaConsumer();
        context.getRouteController().suspendRoute("kafka");
        await().atMost(20, TimeUnit.SECONDS).until(consumer::isKafkaPaused);
        awaitPolls();
        addRecord("b");
        awaitPolls();

        assertEquals(0, mock.getReceivedCounter(), "A suspended route must not receive records");
        assertTrue(consumer.isKafkaPaused());
        assertTrue(mockConsumer.paused().contains(PARTITION), "The Kafka consumer must stay paused");

        mock.expectedBodiesReceived("b");
        context.getRouteController().resumeRoute("kafka");
        mock.assertIsSatisfied();
    }

    @Test
    void testRouteStartedSuspendedConsumesNothingWhileTheCheckHolds() throws Exception {
        // keepOpen suspends the consumer when the route starts: the partition is paused when it is assigned
        long first = offset++;
        startRoute(true, () -> mockConsumer.addRecord(new ConsumerRecord<>(TOPIC, 0, first, null, "a")));
        KafkaConsumer consumer = kafkaConsumer();
        await().atMost(20, TimeUnit.SECONDS).until(consumer::isKafkaPaused);
        awaitPolls();

        MockEndpoint mock = getMockEndpoint("mock:result");
        assertEquals(0, mock.getReceivedCounter(), "A suspended route must not receive records");
        assertTrue(mockConsumer.paused().contains(PARTITION), "The Kafka consumer must stay paused");

        mock.expectedBodiesReceived("a");
        keepOpenPolicy.setKeepOpen(false);
        mock.assertIsSatisfied();
    }

    @Test
    void testCheckThatHoldsAgainDoesNotResumeASuspendedRoute() throws Exception {
        startRoute(false);
        MockEndpoint mock = getMockEndpoint("mock:result");
        mock.expectedBodiesReceived("a");
        addRecord("a");
        mock.assertIsSatisfied();
        mock.reset();

        // the check fails (the listener pauses the consumer and seeks to the beginning), then the route is suspended
        available = false;
        awaitPolls();
        context.getRouteController().suspendRoute("kafka");
        awaitPolls();
        // the check holds again while the route is suspended
        available = true;
        KafkaConsumer consumer = kafkaConsumer();
        await().atMost(20, TimeUnit.SECONDS).until(consumer::isKafkaPaused);
        addRecord("b");
        awaitPolls();

        assertEquals(0, mock.getReceivedCounter(), "A suspended route must not receive records");
        assertTrue(mockConsumer.paused().contains(PARTITION), "The Kafka consumer must stay paused");

        // MockConsumer returns an added record once, so the seek of the listener to the beginning (while its check
        // failed) does not return a again
        mock.expectedBodiesReceived("b");
        context.getRouteController().resumeRoute("kafka");
        mock.assertIsSatisfied();
    }

    @Test
    void testResumedRouteStaysPausedWhileTheCheckFails() throws Exception {
        available = false;
        startRoute(false);
        addRecord("a");
        awaitPolls();

        context.getRouteController().suspendRoute("kafka");
        awaitPolls();
        context.getRouteController().resumeRoute("kafka");
        awaitPolls();

        MockEndpoint mock = getMockEndpoint("mock:result");
        assertEquals(0, mock.getReceivedCounter(), "The pausable EIP keeps the consumer paused while its check fails");
        assertTrue(mockConsumer.paused().contains(PARTITION), "The Kafka consumer must stay paused");

        mock.expectedBodiesReceived("a");
        available = true;
        mock.assertIsSatisfied();
    }

    private void startRoute(boolean keepOpen) throws Exception {
        startRoute(keepOpen, () -> {
        });
    }

    private void startRoute(boolean keepOpen, Runnable afterAssignment) throws Exception {
        mockConsumer.updateBeginningOffsets(Map.of(PARTITION, 0L));
        mockConsumer.schedulePollTask(() -> {
            mockConsumer.rebalance(List.of(PARTITION));
            afterAssignment.run();
        });
        context.addRoutes(new RouteBuilder() {
            @Override
            public void configure() {
                from("kafka:" + TOPIC + "?brokers=localhost:9092&kafkaClientFactory=#mockFactory&groupId=test&pollTimeoutMs=10")
                        .routeId("kafka").routePolicy(keepOpenPolicy)
                        .pausable(new KafkaConsumerListener(), o -> available)
                        .to("mock:result");
            }
        });
        if (!keepOpen) {
            keepOpenPolicy.setKeepOpen(false);
        }
        context.start();
    }

    private KafkaConsumer kafkaConsumer() {
        return (KafkaConsumer) context.getRoute("kafka").getConsumer();
    }

    private void addRecord(String value) {
        await().atMost(20, TimeUnit.SECONDS).until(() -> mockConsumer.assignment().contains(PARTITION));
        mockConsumer.addRecord(new ConsumerRecord<>(TOPIC, 0, offset++, null, value));
    }

    /**
     * Waits until the fetcher thread started {@link #POLLS} more polls: the records returned by the polls before the
     * last one were processed (the fetcher processes the records of a poll before it polls again).
     */
    private void awaitPolls() {
        int target = mockConsumer.polls.get() + POLLS;
        await().atMost(20, TimeUnit.SECONDS).until(() -> mockConsumer.polls.get() >= target);
    }

    private static final class CountingMockConsumer extends MockConsumer<Object, Object> {
        final AtomicInteger polls = new AtomicInteger();

        CountingMockConsumer() {
            super("earliest");
        }

        @Override
        public ConsumerRecords<Object, Object> poll(Duration timeout) {
            polls.incrementAndGet();
            ConsumerRecords<Object, Object> records = super.poll(timeout);
            if (records.isEmpty()) {
                // a real consumer waits up to the poll timeout when there is nothing to fetch
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));
            }
            return records;
        }

        @Override
        public synchronized void close() {
            // keep the mock usable: a stopped route closes the consumer
        }
    }
}
