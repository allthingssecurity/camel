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
package org.apache.camel.component.google.secret.manager.vault;

import java.lang.reflect.Field;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.api.core.ApiService;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.protobuf.Empty;
import com.google.protobuf.Timestamp;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.ModifyAckDeadlineRequest;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.ReceivedMessage;
import com.google.pubsub.v1.StreamingPullRequest;
import com.google.pubsub.v1.StreamingPullResponse;
import io.grpc.ManagedChannel;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.ServerServiceDefinition;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ServerCalls;
import io.grpc.stub.StreamObserver;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.spi.PeriodTaskScheduler;
import org.apache.camel.support.DefaultContextReloadStrategy;
import org.apache.camel.support.PluginHelper;
import org.apache.camel.support.TimerListenerManager;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * A Google Pub/Sub subscriber that fails (here on a PERMISSION_DENIED of its streaming pull) does not end the GCP
 * secret refresh: the next refresh starts a new subscriber, which receives the next secret event. The real Subscriber
 * runs against an in-process fake of the Pub/Sub Subscriber service.
 */
class PubsubReloadTriggerTaskSubscriberFailedTest {

    private static final String SERVICE = "google.pubsub.v1.Subscriber";
    private static final MethodDescriptor<StreamingPullRequest, StreamingPullResponse> STREAMING_PULL
            = MethodDescriptor.<StreamingPullRequest, StreamingPullResponse> newBuilder()
                    .setType(MethodDescriptor.MethodType.BIDI_STREAMING)
                    .setFullMethodName(MethodDescriptor.generateFullMethodName(SERVICE, "StreamingPull"))
                    .setRequestMarshaller(ProtoUtils.marshaller(StreamingPullRequest.getDefaultInstance()))
                    .setResponseMarshaller(ProtoUtils.marshaller(StreamingPullResponse.getDefaultInstance()))
                    .build();
    private static final MethodDescriptor<AcknowledgeRequest, Empty> ACKNOWLEDGE
            = MethodDescriptor.<AcknowledgeRequest, Empty> newBuilder()
                    .setType(MethodDescriptor.MethodType.UNARY)
                    .setFullMethodName(MethodDescriptor.generateFullMethodName(SERVICE, "Acknowledge"))
                    .setRequestMarshaller(ProtoUtils.marshaller(AcknowledgeRequest.getDefaultInstance()))
                    .setResponseMarshaller(ProtoUtils.marshaller(Empty.getDefaultInstance()))
                    .build();
    private static final MethodDescriptor<ModifyAckDeadlineRequest, Empty> MODIFY_ACK_DEADLINE
            = MethodDescriptor.<ModifyAckDeadlineRequest, Empty> newBuilder()
                    .setType(MethodDescriptor.MethodType.UNARY)
                    .setFullMethodName(MethodDescriptor.generateFullMethodName(SERVICE, "ModifyAckDeadline"))
                    .setRequestMarshaller(ProtoUtils.marshaller(ModifyAckDeadlineRequest.getDefaultInstance()))
                    .setResponseMarshaller(ProtoUtils.marshaller(Empty.getDefaultInstance()))
                    .build();

    @Test
    void rotationAfterTheSubscriberFailedTriggersTheReload() throws Exception {
        FakePubsub fake = new FakePubsub();
        String name = InProcessServerBuilder.generateName();
        Server server = InProcessServerBuilder.forName(name).directExecutor().addService(fake.service()).build().start();
        List<ManagedChannel> channels = new CopyOnWriteArrayList<>();
        List<Subscriber> subscribers = new CopyOnWriteArrayList<>();
        AtomicInteger reloads = new AtomicInteger();

        Logger logger = (Logger) LogManager.getLogger(PubsubReloadTriggerTask.class);
        LogEventCaptor captor = new LogEventCaptor();
        logger.addAppender(captor);
        DefaultCamelContext context = new DefaultCamelContext();
        try {
            context.addService(new DefaultContextReloadStrategy() {
                @Override
                public void onReload(Object source) {
                    reloads.incrementAndGet();
                }
            });
            PeriodTaskScheduler scheduler = PluginHelper.getPeriodTaskScheduler(context);
            ((TimerListenerManager) scheduler).setInterval(50);
            context.start();

            PubsubReloadTriggerTask task = new PubsubReloadTriggerTask() {
                @Override
                protected void doStart() throws Exception {
                    // what doStart sets up, without GCP credentials: the watched secrets, and a subscriber factory that
                    // builds subscribers with the receiver of the task, here against the fake Pub/Sub
                    setField(this, "secrets", "database-password");
                    subscriberFactory = () -> {
                        ManagedChannel channel = InProcessChannelBuilder.forName(name).directExecutor().build();
                        channels.add(channel);
                        Subscriber subscriber = Subscriber
                                .newBuilder(ProjectSubscriptionName.of("p", "secret-events"),
                                        new FilteringEventMessageReceiver())
                                .setChannelProvider(
                                        FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel)))
                                .setCredentialsProvider(NoCredentialsProvider.create())
                                .build();
                        subscribers.add(subscriber);
                        return subscriber;
                    };
                    setField(this, "subscriber", subscriberFactory.get());
                }
            };
            // as DefaultConfigurationConfigurer schedules it (gcp().getRefreshPeriod(), here 100 ms)
            scheduler.schedulePeriodTask(task, 100);

            // the first refresh starts the subscriber, which fails on the PERMISSION_DENIED of its streaming pull
            await().atMost(Duration.ofSeconds(10))
                    .untilAsserted(() -> assertThat(subscribers.get(0).state()).isEqualTo(ApiService.State.FAILED));

            // the permission is back, and the secret gets a new version
            fake.permitted = true;
            await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(reloads.get()).isEqualTo(1));

            assertThat(subscribers).hasSizeGreaterThan(1);
            assertThat(subscribers.get(subscribers.size() - 1).state()).isEqualTo(ApiService.State.RUNNING);
            assertThat(captor.events)
                    .anyMatch(event -> event.getLevel() == Level.WARN
                            && event.getMessage().getFormattedMessage().contains("PERMISSION_DENIED"));

            // stopping Camel stops the subscriber that replaced the failed one
            context.stop();
            assertThat(subscribers.get(subscribers.size() - 1).state()).isIn(ApiService.State.STOPPING,
                    ApiService.State.TERMINATED);
        } finally {
            logger.removeAppender(captor);
            context.stop();
            channels.forEach(ManagedChannel::shutdownNow);
            server.shutdownNow();
        }
    }

    private static void setField(Object target, String name, Object value) throws Exception {
        Field f = PubsubReloadTriggerTask.class.getDeclaredField(name);
        f.setAccessible(true);
        f.set(target, value);
    }

    /**
     * The fake Pub/Sub: answers a streaming pull with PERMISSION_DENIED (a status that the Subscriber does not retry:
     * it goes to FAILED) until permitted, then delivers one event of a new version of the watched secret, once.
     */
    private static final class FakePubsub {
        private volatile boolean permitted;
        private final AtomicBoolean delivered = new AtomicBoolean();

        ServerServiceDefinition service() {
            return ServerServiceDefinition.builder(SERVICE)
                    .addMethod(STREAMING_PULL, ServerCalls.asyncBidiStreamingCall(this::streamingPull))
                    .addMethod(ACKNOWLEDGE, ServerCalls.asyncUnaryCall((AcknowledgeRequest req, StreamObserver<Empty> o) -> {
                        o.onNext(Empty.getDefaultInstance());
                        o.onCompleted();
                    }))
                    .addMethod(MODIFY_ACK_DEADLINE,
                            ServerCalls.asyncUnaryCall((ModifyAckDeadlineRequest req, StreamObserver<Empty> o) -> {
                                o.onNext(Empty.getDefaultInstance());
                                o.onCompleted();
                            }))
                    .build();
        }

        private StreamObserver<StreamingPullRequest> streamingPull(StreamObserver<StreamingPullResponse> responses) {
            return new StreamObserver<>() {
                private boolean first = true;

                @Override
                public void onNext(StreamingPullRequest request) {
                    if (!first) {
                        // acks and ack deadline changes sent on the stream
                        return;
                    }
                    first = false;
                    if (!permitted) {
                        responses.onError(Status.PERMISSION_DENIED
                                .withDescription("User not authorized to perform this action.").asRuntimeException());
                        return;
                    }
                    if (!delivered.compareAndSet(false, true)) {
                        return;
                    }
                    PubsubMessage message = PubsubMessage.newBuilder()
                            .putAttributes("eventType", "SECRET_VERSION_ADD")
                            .putAttributes("secretId", "projects/p/secrets/database-password")
                            .setMessageId("1")
                            .setPublishTime(Timestamp.newBuilder().setSeconds(Instant.now().getEpochSecond()))
                            .build();
                    responses.onNext(StreamingPullResponse.newBuilder()
                            .addReceivedMessages(ReceivedMessage.newBuilder().setAckId("ack-1").setMessage(message))
                            .build());
                }

                @Override
                public void onError(Throwable t) {
                    // the subscriber closed its stream
                }

                @Override
                public void onCompleted() {
                    responses.onCompleted();
                }
            };
        }
    }

    private static final class LogEventCaptor extends AbstractAppender {
        private final List<LogEvent> events = new CopyOnWriteArrayList<>();

        LogEventCaptor() {
            super("PubsubReloadTriggerTaskCaptor", null, null, false, Property.EMPTY_ARRAY);
            start();
        }

        @Override
        public void append(LogEvent event) {
            events.add(event.toImmutable());
        }
    }
}
