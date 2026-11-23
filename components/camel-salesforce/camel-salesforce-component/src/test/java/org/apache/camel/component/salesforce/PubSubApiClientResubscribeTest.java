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
package org.apache.camel.component.salesforce;

import java.io.ByteArrayOutputStream;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import com.google.protobuf.ByteString;
import com.salesforce.eventbus.protobuf.ConsumerEvent;
import com.salesforce.eventbus.protobuf.FetchRequest;
import com.salesforce.eventbus.protobuf.FetchResponse;
import com.salesforce.eventbus.protobuf.ProducerEvent;
import com.salesforce.eventbus.protobuf.PubSubGrpc;
import com.salesforce.eventbus.protobuf.ReplayPreset;
import com.salesforce.eventbus.protobuf.SchemaInfo;
import com.salesforce.eventbus.protobuf.SchemaRequest;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.ForwardingClientCallListener;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.camel.component.salesforce.internal.SalesforceSession;
import org.apache.camel.component.salesforce.internal.client.PubSubApiClient;
import org.apache.camel.spi.ExceptionHandler;
import org.apache.camel.test.AvailablePortFinder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Resubscribe and stop of the Pub/Sub API client against an in-process fake Pub/Sub server. A client interceptor holds
 * the resubscribing thread inside {@code asyncStub.subscribe}, so each race is reached in the same order every run.
 */
class PubSubApiClientResubscribeTest {

    private static final String SCHEMA_JSON
            = "{\"type\":\"record\",\"name\":\"Ev\",\"namespace\":\"t\",\"fields\":[{\"name\":\"n\",\"type\":\"string\"}]}";

    private final AvailablePortFinder.Port port = AvailablePortFinder.find();
    private final List<PubSubApiClient> clients = new CopyOnWriteArrayList<>();
    private final CountDownLatch release = new CountDownLatch(1);
    private Server server;

    @AfterEach
    void tearDown() {
        release.countDown();
        clients.forEach(PubSubApiClient::stop);
        if (server != null) {
            server.shutdownNow();
        }
        port.release();
    }

    /**
     * A stream error makes the client resubscribe. The route is stopped while the resubscribe is opening the new call.
     * After stop returns, no subscription may be left open, and after a restart each event must reach the consumer
     * once.
     */
    @Test
    void stopDuringResubscribeClosesTheNewSubscription() throws Exception {
        FakeServer fake = new FakeServer();
        fake.onFirstFetch = (s, st) -> {
            if (st.index == 1) {
                s.sendEvent(st);
                st.fail(Status.UNAVAILABLE.withDescription("transient"));
            }
        };
        startServer(fake);

        List<String> received = new CopyOnWriteArrayList<>();
        PubSubApiConsumer consumer = consumer(mock(ExceptionHandler.class), received);
        HookedClient client = client();

        AtomicReference<Thread> resubscriber = new AtomicReference<>();
        CountDownLatch resubscribeCallStarted = new CountDownLatch(1);
        client.intercept(new PausingInterceptor(2, resubscriber, resubscribeCallStarted, null, release));

        client.subscribe(consumer, ReplayPreset.LATEST, null, false);
        assertTrue(resubscribeCallStarted.await(10, TimeUnit.SECONDS), "resubscribe started");
        assertEquals(List.of("e1"), received);

        // stop the route while the resubscribe is held inside asyncStub.subscribe()
        Thread stopper = new Thread(client::stop, "route-stop");
        stopper.start();
        // release the resubscribe once the stop has passed the point where it closes the open subscriptions
        // (awaitTermination), or once the stop waits for the resubscribe to finish
        await().atMost(15, TimeUnit.SECONDS)
                .until(() -> stopper.getState() == Thread.State.TIMED_WAITING || isBlockedOn(stopper, resubscriber.get()));
        release.countDown();

        stopper.join(30_000);
        assertFalse(stopper.isAlive());
        assertTrue(client.isStopped());

        await().atMost(5, TimeUnit.SECONDS)
                .untilAsserted(() -> assertEquals(List.of(), fake.openStreams(), "subscriptions open after stop"));
        int afterStop = received.size();
        fake.publishToOpenStreams();
        assertEquals(afterStop, received.size());

        // route start: a new client for the same consumer
        HookedClient client2 = client();
        client2.subscribe(consumer, ReplayPreset.LATEST, null, false);
        await().atMost(5, TimeUnit.SECONDS).until(() -> fake.streams.size() == 3 && fake.streams.get(2).fetchReceived());

        int beforePublish = received.size();
        fake.publishToOpenStreams();
        await().atMost(5, TimeUnit.SECONDS).until(() -> received.size() > beforePublish);
        assertEquals(1, fake.openStreams().size());
        assertEquals(beforePublish + 1, received.size());
    }

    /**
     * The server ends the subscription normally while the consumer is running. The client must subscribe again from the
     * last replay id, after the reconnect back-off.
     */
    @Test
    void serverCompletionResubscribesFromTheLastReplayId() throws Exception {
        FakeServer fake = new FakeServer();
        fake.onFirstFetch = (s, st) -> {
            if (st.index == 1) {
                s.sendEvent(st);
                st.complete();
            }
        };
        startServer(fake);

        List<String> received = new CopyOnWriteArrayList<>();
        ExceptionHandler handler = mock(ExceptionHandler.class);
        PubSubApiConsumer consumer = consumer(handler, received);
        HookedClient client = client();

        client.subscribe(consumer, ReplayPreset.LATEST, null, false);

        // back-off is 500 ms
        await().atMost(5, TimeUnit.SECONDS).until(() -> fake.streams.size() == 2 && fake.streams.get(1).fetchReceived());
        FetchRequest resubscribe = fake.streams.get(1).firstFetch;
        assertEquals(ReplayPreset.CUSTOM, resubscribe.getReplayPreset());
        assertEquals(ByteString.copyFromUtf8("1"), resubscribe.getReplayId());
        assertTrue(client.isStarted());

        fake.publishToOpenStreams();
        await().atMost(5, TimeUnit.SECONDS).until(() -> received.size() == 2);
        assertEquals(List.of("e1", "e2"), received);
        assertEquals(2, fake.streams.size());
        verify(handler, never()).handleException(anyString(), any(Throwable.class));
    }

    /**
     * The call opened by a resubscribe fails at once, and its error callback resubscribes again while the first thread
     * has not stored the failed call yet. The failed call must not replace the live subscription, so that a normal stop
     * closes the live one.
     */
    @Test
    void failedCallDoesNotReplaceTheLiveSubscription() throws Exception {
        FakeServer fake = new FakeServer();
        fake.onFirstFetch = (s, st) -> {
            if (st.index == 1) {
                s.sendEvent(st);
                st.fail(Status.UNAVAILABLE.withDescription("transient"));
            }
        };
        fake.onOpen = st -> {
            if (st.index == 2) {
                st.fail(Status.UNAVAILABLE.withDescription("rejected"));
            }
        };
        startServer(fake);

        List<String> received = new CopyOnWriteArrayList<>();
        PubSubApiConsumer consumer = consumer(mock(ExceptionHandler.class), received);
        HookedClient client = client();

        AtomicReference<Thread> firstResubscriber = new AtomicReference<>();
        AtomicReference<Thread> failedCallCallback = new AtomicReference<>();
        client.intercept(new PausingInterceptor(2, firstResubscriber, new CountDownLatch(1), failedCallCallback, release));

        client.subscribe(consumer, ReplayPreset.LATEST, null, false);
        // release the first resubscribe once the second one has opened its call, or once the second one waits for
        // the first one
        await().atMost(15, TimeUnit.SECONDS)
                .until(() -> fake.streams.size() == 3 && fake.streams.get(2).fetchReceived()
                        || isBlockedOn(failedCallCallback.get(), firstResubscriber.get()));
        release.countDown();
        await().atMost(5, TimeUnit.SECONDS).until(() -> !inClientCode(firstResubscriber.get()));
        await().atMost(5, TimeUnit.SECONDS).until(() -> fake.streams.size() == 3 && fake.streams.get(2).fetchReceived());

        // an ordinary route stop
        client.stop();
        assertTrue(client.isStopped());
        await().atMost(5, TimeUnit.SECONDS)
                .untilAsserted(() -> assertEquals(List.of(), fake.openStreams(), "subscriptions open after stop"));
        int afterStop = received.size();
        fake.publishToOpenStreams();
        assertEquals(afterStop, received.size());
    }

    private void startServer(FakeServer fake) throws Exception {
        server = ServerBuilder.forPort(port.getPort()).addService(fake).build().start();
    }

    private HookedClient client() {
        HookedClient client = new HookedClient(session(), port.getPort());
        clients.add(client);
        client.start();
        return client;
    }

    /** Whether {@code waiter} is blocked on a lock held by {@code owner}. */
    private static boolean isBlockedOn(Thread waiter, Thread owner) {
        if (waiter == null || owner == null) {
            return false;
        }
        ThreadInfo info = ManagementFactory.getThreadMXBean().getThreadInfo(waiter.threadId());
        return info != null && info.getLockOwnerId() == owner.threadId();
    }

    /** Whether {@code thread} still runs code of the Pub/Sub client. */
    private static boolean inClientCode(Thread thread) {
        return Arrays.stream(thread.getStackTrace())
                .anyMatch(f -> f.getClassName().startsWith(PubSubApiClient.class.getName()));
    }

    private static SalesforceSession session() {
        SalesforceSession session = mock(SalesforceSession.class);
        when(session.getAccessToken()).thenReturn("faketoken");
        when(session.getInstanceUrl()).thenReturn("https://myinstance");
        when(session.getOrgId()).thenReturn("00D123123123");
        return session;
    }

    private static PubSubApiConsumer consumer(ExceptionHandler handler, List<String> received) {
        PubSubApiConsumer consumer = mock(PubSubApiConsumer.class);
        when(consumer.getTopic()).thenReturn("/event/FakeTopic");
        when(consumer.getBatchSize()).thenReturn(100);
        when(consumer.getDeserializeType()).thenReturn(PubSubDeserializeType.AVRO);
        when(consumer.getEventClassMap()).thenReturn(Map.of());
        when(consumer.getExceptionHandler()).thenReturn(handler);
        doAnswer(inv -> {
            received.add(inv.getArgument(1));
            return null;
        }).when(consumer).processEvent(any(), anyString(), anyString(), anyString());
        return consumer;
    }

    /** Client whose async stub can be wrapped with an interceptor. */
    static class HookedClient extends PubSubApiClient {
        HookedClient(SalesforceSession session, int port) {
            super(session, new SalesforceLoginConfig(), "localhost", port, 500, 5000, true);
            setUsePlainTextConnection(true);
        }

        void intercept(ClientInterceptor interceptor) {
            asyncStub = asyncStub.withInterceptors(interceptor);
        }
    }

    /**
     * Holds the thread that starts call number {@code pauseCall} right after the call has started, that is inside
     * {@code asyncStub.subscribe}, until {@code release} is counted down. Optionally records the thread that runs the
     * close callback of that call.
     */
    static class PausingInterceptor implements ClientInterceptor {
        private final AtomicInteger calls = new AtomicInteger();
        private final int pauseCall;
        private final AtomicReference<Thread> pausedThread;
        private final CountDownLatch paused;
        private final AtomicReference<Thread> closeThread;
        private final CountDownLatch release;

        PausingInterceptor(int pauseCall, AtomicReference<Thread> pausedThread, CountDownLatch paused,
                           AtomicReference<Thread> closeThread, CountDownLatch release) {
            this.pauseCall = pauseCall;
            this.pausedThread = pausedThread;
            this.paused = paused;
            this.closeThread = closeThread;
            this.release = release;
        }

        @Override
        public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
                MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
            int n = calls.incrementAndGet();
            return new ForwardingClientCall.SimpleForwardingClientCall<>(next.newCall(method, callOptions)) {
                @Override
                public void start(Listener<RespT> listener, Metadata headers) {
                    Listener<RespT> l = listener;
                    if (n == pauseCall && closeThread != null) {
                        l = new ForwardingClientCallListener.SimpleForwardingClientCallListener<>(listener) {
                            @Override
                            public void onClose(Status status, Metadata trailers) {
                                closeThread.set(Thread.currentThread());
                                super.onClose(status, trailers);
                            }
                        };
                    }
                    super.start(l, headers);
                    if (n == pauseCall) {
                        pausedThread.set(Thread.currentThread());
                        paused.countDown();
                        try {
                            release.await(30, TimeUnit.SECONDS);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }
                }
            };
        }
    }

    /** One subscription stream as seen by the server. */
    static final class ServerStream {
        final int index;
        final ServerCallStreamObserver<FetchResponse> client;
        volatile FetchRequest firstFetch;
        volatile boolean halfClosedByClient;
        volatile boolean cancelled;
        volatile boolean ended;

        ServerStream(int index, ServerCallStreamObserver<FetchResponse> client) {
            this.index = index;
            this.client = client;
        }

        boolean fetchReceived() {
            return firstFetch != null;
        }

        boolean open() {
            return !halfClosedByClient && !cancelled && !ended;
        }

        synchronized void fail(Status status) {
            ended = true;
            client.onError(status.asRuntimeException(new Metadata()));
        }

        synchronized void complete() {
            ended = true;
            client.onCompleted();
        }
    }

    /** Fake Pub/Sub server; it ends a stream when the client half-closes it. */
    static class FakeServer extends PubSubGrpc.PubSubImplBase {
        final List<ServerStream> streams = new CopyOnWriteArrayList<>();
        final AtomicLong nextReplay = new AtomicLong(1);
        final Schema schema = new Schema.Parser().parse(SCHEMA_JSON);
        volatile BiConsumer<FakeServer, ServerStream> onFirstFetch = (s, st) -> {
        };
        volatile Consumer<ServerStream> onOpen = st -> {
        };

        @Override
        public void getSchema(SchemaRequest request, StreamObserver<SchemaInfo> responseObserver) {
            responseObserver.onNext(SchemaInfo.newBuilder().setSchemaId(request.getSchemaId()).setSchemaJson(SCHEMA_JSON)
                    .build());
            responseObserver.onCompleted();
        }

        @Override
        public StreamObserver<FetchRequest> subscribe(StreamObserver<FetchResponse> client) {
            ServerStream st = new ServerStream(streams.size() + 1, (ServerCallStreamObserver<FetchResponse>) client);
            st.client.setOnCancelHandler(() -> st.cancelled = true);
            streams.add(st);
            onOpen.accept(st);
            return new StreamObserver<>() {
                @Override
                public void onNext(FetchRequest request) {
                    if (st.firstFetch == null) {
                        st.firstFetch = request;
                        onFirstFetch.accept(FakeServer.this, st);
                    }
                }

                @Override
                public void onError(Throwable t) {
                    st.cancelled = true;
                }

                @Override
                public void onCompleted() {
                    st.halfClosedByClient = true;
                    synchronized (st) {
                        if (!st.ended) {
                            st.complete();
                        }
                    }
                }
            };
        }

        List<ServerStream> openStreams() {
            return streams.stream().filter(ServerStream::open).toList();
        }

        /** Sends one event to the given stream; the replay id of the n-th event is "n". */
        void sendEvent(ServerStream st) {
            long r = nextReplay.getAndIncrement();
            try {
                GenericRecord rec = new GenericData.Record(schema);
                rec.put("n", "event-" + r);
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                BinaryEncoder enc = EncoderFactory.get().directBinaryEncoder(out, null);
                new GenericDatumWriter<GenericRecord>(schema).write(rec, enc);
                ByteString replay = ByteString.copyFromUtf8(Long.toString(r));
                ConsumerEvent ce = ConsumerEvent.newBuilder()
                        .setEvent(ProducerEvent.newBuilder().setId("e" + r).setSchemaId("s1")
                                .setPayload(ByteString.copyFrom(out.toByteArray())))
                        .setReplayId(replay).build();
                synchronized (st) {
                    st.client.onNext(FetchResponse.newBuilder().addEvents(ce).setLatestReplayId(replay)
                            .setPendingNumRequested(99).setRpcId("rpc").build());
                }
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }

        /** Publishes one event: every open subscription that has asked for events receives it. */
        void publishToOpenStreams() {
            for (ServerStream st : streams) {
                if (st.open() && st.fetchReceived()) {
                    sendEvent(st);
                }
            }
        }
    }
}
