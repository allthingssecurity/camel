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
package org.apache.camel.component.google.pubsub;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.api.core.SettableApiFuture;
import com.google.api.gax.grpc.GrpcStatusCode;
import com.google.api.gax.rpc.PermissionDeniedException;
import com.google.api.gax.rpc.UnaryCallable;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.ReceivedMessage;
import io.grpc.Status;
import org.apache.camel.CamelContext;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.health.HealthCheck;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Logger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * A subscriber loop that ends with a failure (a non-retryable error, credentials that cannot be loaded for the
 * subscriber, an Error) must be logged and subscribe or pull again after the retry delay, and the health check of the
 * consumer must report DOWN while the loop does not pull: the route stays started meanwhile.
 */
class GooglePubsubConsumerLoopRestartTest {

    private static final Duration RETRY_DELAY = Duration.ofSeconds(1);

    private final CamelContext context = new DefaultCamelContext();
    private final SettableApiFuture<PullResponse> pending = SettableApiFuture.create();
    private final AtomicInteger pulls = new AtomicInteger();
    // log lines of the consumer, which logs to the logger of the endpoint option loggerId (the appender keeps the
    // formatted messages)
    private static final String LOGGER_ID = "GooglePubsubConsumerLoopRestartTest.consumer";
    private final OrphanChannelLogAppender consumerLog = new OrphanChannelLogAppender();
    private final Logger consumerLogger = (Logger) LogManager.getLogger(LOGGER_ID);

    @BeforeEach
    void setUp() {
        consumerLogger.addAppender(consumerLog);
    }

    @AfterEach
    void tearDown() {
        pending.cancel(true);
        context.stop();
        consumerLogger.removeAppender(consumerLog);
    }

    @Test
    void syncPullAfterNonRetryableErrorAndHealthDownMeanwhile() throws Exception {
        SubscriberStub stub = stub(() -> {
            if (pulls.incrementAndGet() == 1) {
                // such as a PERMISSION_DENIED while an IAM binding is changed
                return ApiFutures.immediateFailedFuture(new PermissionDeniedException(
                        "denied", null, GrpcStatusCode.of(Status.Code.PERMISSION_DENIED), false));
            }
            return pulls.get() == 2 ? ApiFutures.immediateFuture(message()) : pending;
        });
        GooglePubsubConsumer consumer = startRoute(true, new GooglePubsubComponent() {
            @Override
            public SubscriberStub getSubscriberStub(GooglePubsubEndpoint endpoint) {
                return stub;
            }
        });
        MockEndpoint result = context.getEndpoint("mock:result", MockEndpoint.class);
        result.expectedBodiesReceived("hello");

        HealthCheck.Result down = awaitHealth(consumer, HealthCheck.State.DOWN);
        assertInstanceOf(PermissionDeniedException.class, down.getError().orElseThrow().getCause());
        assertEquals(1, pulls.get());

        result.assertIsSatisfied();
        awaitHealth(consumer, HealthCheck.State.UP);
        assertLogged("Failure getting messages from PubSub. Subscribing again");
    }

    @Test
    void syncPullAfterErrorIsLogged() throws Exception {
        SubscriberStub stub = stub(() -> pulls.incrementAndGet() == 1 ? ApiFutures.immediateFuture(message()) : pending);
        AtomicInteger stubs = new AtomicInteger();
        GooglePubsubConsumer consumer = startRoute(true, new GooglePubsubComponent() {
            @Override
            public SubscriberStub getSubscriberStub(GooglePubsubEndpoint endpoint) {
                if (stubs.incrementAndGet() == 1) {
                    // such as a dependency mismatch of the client libraries
                    throw new NoClassDefFoundError("io/grpc/SomeClass");
                }
                return stub;
            }
        });
        MockEndpoint result = context.getEndpoint("mock:result", MockEndpoint.class);
        result.expectedBodiesReceived("hello");

        HealthCheck.Result down = awaitHealth(consumer, HealthCheck.State.DOWN);
        assertInstanceOf(NoClassDefFoundError.class, down.getError().orElseThrow());
        assertLogged("Failure getting messages from PubSub. Subscribing again");

        result.assertIsSatisfied();
        awaitHealth(consumer, HealthCheck.State.UP);
    }

    @Test
    void asyncSubscribesAgainAfterIOExceptionFromGetSubscriber() throws Exception {
        AtomicInteger subscribers = new AtomicInteger();
        CountDownLatch subscribed = new CountDownLatch(1);
        GooglePubsubConsumer consumer = startRoute(false, new GooglePubsubComponent() {
            @Override
            public Subscriber getSubscriber(String subscriptionName, MessageReceiver receiver, GooglePubsubEndpoint endpoint)
                    throws IOException {
                if (subscribers.incrementAndGet() == 1) {
                    // such as a metadata server that does not answer
                    throw new IOException("the credentials could not be loaded");
                }
                subscribed.countDown();
                return runningSubscriber(null);
            }
        });

        HealthCheck.Result down = awaitHealth(consumer, HealthCheck.State.DOWN);
        assertInstanceOf(IOException.class, down.getError().orElseThrow());

        await().atMost(Duration.ofSeconds(10)).until(() -> subscribed.getCount() == 0);
        awaitHealth(consumer, HealthCheck.State.UP);
        assertEquals(2, subscribers.get());
    }

    @Test
    void asyncSubscribesAgainAfterNonRetryableFailure() throws Exception {
        AtomicInteger subscribers = new AtomicInteger();
        GooglePubsubConsumer consumer = startRoute(false, new GooglePubsubComponent() {
            @Override
            public Subscriber getSubscriber(String subscriptionName, MessageReceiver receiver, GooglePubsubEndpoint endpoint) {
                if (subscribers.incrementAndGet() == 1) {
                    // the subscriber fails with a non-retryable error, such as NOT_FOUND while the subscription is
                    // created again
                    return runningSubscriber(new IllegalStateException(
                            new PermissionDeniedException(
                                    "denied", null, GrpcStatusCode.of(Status.Code.PERMISSION_DENIED), false)));
                }
                return runningSubscriber(null);
            }
        });

        awaitHealth(consumer, HealthCheck.State.DOWN);
        await().atMost(Duration.ofSeconds(10)).until(() -> subscribers.get() == 2);
        awaitHealth(consumer, HealthCheck.State.UP);
    }

    private HealthCheck.Result awaitHealth(GooglePubsubConsumer consumer, HealthCheck.State state) {
        HealthCheck check = consumer.getHealthCheck();
        assertNotNull(check, "the consumer has a health check");
        await().atMost(Duration.ofSeconds(10)).until(() -> check.call().getState() == state);
        return check.call();
    }

    private void assertLogged(String text) {
        assertTrue(consumerLog.getMessages().stream().anyMatch(m -> m.startsWith(text)),
                () -> "logged: " + consumerLog.getMessages());
    }

    private GooglePubsubConsumer startRoute(boolean synchronousPull, GooglePubsubComponent component) throws Exception {
        context.addComponent("google-pubsub", component);
        context.addRoutes(new RouteBuilder() {
            @Override
            public void configure() {
                from("google-pubsub:test-project:test-subscription?maxDeliveryAttempts=5&loggerId=" + LOGGER_ID
                     + "&synchronousPull=" + synchronousPull)
                        .routeId("pubsub").autoStartup(false)
                        .to("mock:result");
            }
        });
        context.start();
        GooglePubsubConsumer consumer = (GooglePubsubConsumer) context.getRoute("pubsub").getConsumer();
        consumer.setRetryDelay(RETRY_DELAY);
        context.getRouteController().startRoute("pubsub");
        return consumer;
    }

    /**
     * A subscriber that runs until it is stopped, or that fails at once with the given failure.
     */
    private static Subscriber runningSubscriber(RuntimeException failure) {
        Subscriber subscriber = mock(Subscriber.class);
        CountDownLatch stopped = new CountDownLatch(1);
        when(subscriber.startAsync()).thenReturn(subscriber);
        doAnswer(invocation -> {
            stopped.countDown();
            return subscriber;
        }).when(subscriber).stopAsync();
        doAnswer(invocation -> {
            if (failure != null) {
                throw failure;
            }
            stopped.await();
            return null;
        }).when(subscriber).awaitTerminated();
        return subscriber;
    }

    @SuppressWarnings("unchecked")
    private static SubscriberStub stub(PullAnswer pull) {
        UnaryCallable<PullRequest, PullResponse> pullCallable = mock(UnaryCallable.class);
        when(pullCallable.futureCall(any(PullRequest.class))).thenAnswer(invocation -> pull.answer());
        UnaryCallable<AcknowledgeRequest, Empty> acknowledgeCallable = mock(UnaryCallable.class);
        when(acknowledgeCallable.call(any(AcknowledgeRequest.class))).thenReturn(Empty.getDefaultInstance());

        SubscriberStub stub = mock(SubscriberStub.class);
        when(stub.pullCallable()).thenReturn(pullCallable);
        when(stub.acknowledgeCallable()).thenReturn(acknowledgeCallable);
        return stub;
    }

    private static PullResponse message() {
        return PullResponse.newBuilder()
                .addReceivedMessages(ReceivedMessage.newBuilder()
                        .setAckId("ack-1")
                        .setMessage(PubsubMessage.newBuilder()
                                .setMessageId("m1")
                                .setData(ByteString.copyFromUtf8("hello"))))
                .build();
    }

    @FunctionalInterface
    private interface PullAnswer {
        ApiFuture<PullResponse> answer();
    }
}
