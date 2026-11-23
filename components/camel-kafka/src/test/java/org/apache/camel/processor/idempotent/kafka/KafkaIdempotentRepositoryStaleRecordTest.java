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
package org.apache.camel.processor.idempotent.kafka;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.camel.RuntimeCamelException;
import org.apache.camel.support.LRUCacheFactory;
import org.apache.camel.support.service.ServiceHelper;
import org.apache.camel.support.service.ServiceSupport;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link KafkaIdempotentRepository} with the background poller of its {@link KafkaChangelog} running on its own thread
 * over a {@link MockConsumer}. The test plays the broker: it decides when a sent record is written and when the poller
 * reads it. The topic has one partition, and the record sent n-th has offset n.
 */
class KafkaIdempotentRepositoryStaleRecordTest {

    private static final String TOPIC = "test-topic";
    private static final TopicPartition PARTITION = new TopicPartition(TOPIC, 0);

    private final ExecutorService exchanges = Executors.newCachedThreadPool();
    private final Semaphore sent = new Semaphore(0);
    private final Semaphore applied = new Semaphore(0);
    private MockProducer<String, String> producer;
    private MockConsumer<String, String> consumer;
    private KafkaIdempotentRepository repository;
    private ServiceSupport poller;
    private Thread pollerThread;

    @BeforeEach
    void setUp() throws Exception {
        producer = new MockProducer<>(false, null, new StringSerializer(), new StringSerializer()) {
            @Override
            public synchronized Future<RecordMetadata> send(ProducerRecord<String, String> record, Callback callback) {
                Future<RecordMetadata> answer = super.send(record, callback);
                sent.release();
                return answer;
            }
        };
        consumer = new MockConsumer<>("earliest");
        consumer.assign(List.of(PARTITION));
        consumer.updateBeginningOffsets(Map.of(PARTITION, 0L));

        repository = new KafkaIdempotentRepository();
        repository.setTopic(TOPIC);
        setField(repository, "cache", LRUCacheFactory.newLRUCache(1000));

        Method addToCache = KafkaIdempotentRepository.class.getDeclaredMethod("addToCache", ConsumerRecord.class);
        addToCache.setAccessible(true);
        KafkaChangelog<String> changelog = new KafkaChangelog<>(
                "KafkaIdempotentRepository", TOPIC, new Properties(), new Properties(), StringDeserializer.class,
                StringSerializer.class, 10, false, record -> {
                    try {
                        addToCache.invoke(repository, record);
                    } catch (ReflectiveOperationException e) {
                        throw new IllegalStateException(e);
                    }
                    applied.release();
                });
        changelog.setProducer(producer);
        setField(changelog, "consumer", consumer);
        setField(repository, "changelog", changelog);

        // the background poller of the changelog, as after the warm-up of the start
        Class<?> pollerClass = Class.forName(KafkaChangelog.class.getName() + "$TopicPoller");
        Constructor<?> constructor = pollerClass.getDeclaredConstructor(KafkaChangelog.class);
        constructor.setAccessible(true);
        poller = (ServiceSupport) constructor.newInstance(changelog);
        Field init = pollerClass.getDeclaredField("init");
        init.setAccessible(true);
        ((AtomicBoolean) init.get(poller)).set(true);
        ServiceHelper.startService(poller);
        pollerThread = new Thread((Runnable) poller, "TopicPoller");
        pollerThread.setDaemon(true);
        pollerThread.start();
    }

    @AfterEach
    void tearDown() throws Exception {
        ServiceHelper.stopService(poller);
        consumer.wakeup();
        pollerThread.join(5000);
        exchanges.shutdownNow();
    }

    @Test
    void ownAddReadBackAfterTheRemoveDoesNotRejectTheRedelivery() throws Exception {
        Future<Boolean> add = sendOnExchangeThread(() -> repository.add("K")); // offset 0
        producer.completeNext();
        assertTrue(add.get(5, TimeUnit.SECONDS));

        // the route fails, and the idempotent consumer removes the key: the remove record is in flight
        Future<Boolean> remove = sendOnExchangeThread(() -> repository.remove("K")); // offset 1
        // the poller reads the add of this instance back only now
        pollerReads(0);
        assertFalse(repository.contains("K"), "the older add must not undo the remove");

        producer.completeNext();
        assertTrue(remove.get(5, TimeUnit.SECONDS));

        // the redelivery of the failed message is accepted
        Future<Boolean> redelivery = sendOnExchangeThread(() -> repository.add("K")); // offset 2
        producer.completeNext();
        assertTrue(redelivery.get(5, TimeUnit.SECONDS));

        // once the poller has read every record of this instance back, it forgets their offsets
        pollerReads(1);
        assertTrue(repository.contains("K"));
        pollerReads(2);
        assertTrue(repository.contains("K"));
        assertEquals(0, localWrites().size());
    }

    @Test
    void ownRemoveReadBackAfterTheRedeliveryDoesNotAcceptADuplicate() throws Exception {
        Future<Boolean> add = sendOnExchangeThread(() -> repository.add("K")); // offset 0
        producer.completeNext();
        assertTrue(add.get(5, TimeUnit.SECONDS));
        pollerReads(0);

        // the route fails and removes the key
        Future<Boolean> remove = sendOnExchangeThread(() -> repository.remove("K")); // offset 1
        producer.completeNext();
        assertTrue(remove.get(5, TimeUnit.SECONDS));

        // the redelivery adds the key again, and its record is in flight
        Future<Boolean> redelivery = sendOnExchangeThread(() -> repository.add("K")); // offset 2
        // the poller reads the remove of this instance back only now
        pollerReads(1);
        assertTrue(repository.contains("K"), "the older remove must not undo the add of the redelivery");
        producer.completeNext();
        assertTrue(redelivery.get(5, TimeUnit.SECONDS));

        // a duplicate of the message taken by another thread of this instance, while the redelivery is processed
        Future<Boolean> duplicate = exchanges.submit(() -> repository.add("K"));
        assertFalse(duplicate.get(5, TimeUnit.SECONDS));
    }

    @Test
    void olderAddOfAPeerReadAfterTheRemoveDoesNotRejectTheRedelivery() throws Exception {
        Future<Boolean> add = sendOnExchangeThread(() -> repository.add("K")); // offset 0
        producer.completeNext();
        assertTrue(add.get(5, TimeUnit.SECONDS));
        pollerReads(0);

        // another instance writes an add of the key before the remove of this instance
        peerWrites("K", "add"); // offset 1

        Future<Boolean> remove = sendOnExchangeThread(() -> repository.remove("K")); // offset 2
        producer.completeNext();
        assertTrue(remove.get(5, TimeUnit.SECONDS));

        // the poller reads the add of the peer only now
        pollerReads(1);
        assertFalse(repository.contains("K"), "the older add of the peer must not undo the remove");

        Future<Boolean> redelivery = sendOnExchangeThread(() -> repository.add("K")); // offset 3
        producer.completeNext();
        assertTrue(redelivery.get(5, TimeUnit.SECONDS));
    }

    @Test
    void olderRemoveOfAPeerReadAfterTheRedeliveryDoesNotAcceptADuplicate() throws Exception {
        Future<Boolean> add = sendOnExchangeThread(() -> repository.add("K")); // offset 0
        producer.completeNext();
        assertTrue(add.get(5, TimeUnit.SECONDS));
        pollerReads(0);

        // another instance writes a remove of the key
        peerWrites("K", "remove"); // offset 1

        Future<Boolean> remove = sendOnExchangeThread(() -> repository.remove("K")); // offset 2
        producer.completeNext();
        assertTrue(remove.get(5, TimeUnit.SECONDS));
        Future<Boolean> redelivery = sendOnExchangeThread(() -> repository.add("K")); // offset 3
        producer.completeNext();
        assertTrue(redelivery.get(5, TimeUnit.SECONDS));

        // the poller reads the remove of the peer only now
        pollerReads(1);
        assertTrue(repository.contains("K"), "the older remove of the peer must not undo the add of the redelivery");
        Future<Boolean> duplicate = exchanges.submit(() -> repository.add("K"));
        assertFalse(duplicate.get(5, TimeUnit.SECONDS));
    }

    @Test
    void olderClearReadAfterAnAddKeepsTheKey() throws Exception {
        Future<?> clear = sendOnExchangeThread(() -> {
            repository.clear();
            return true;
        }); // offset 0
        producer.completeNext();
        clear.get(5, TimeUnit.SECONDS);
        Future<Boolean> add = sendOnExchangeThread(() -> repository.add("K")); // offset 1
        producer.completeNext();
        assertTrue(add.get(5, TimeUnit.SECONDS));

        pollerReads(0);
        assertTrue(repository.contains("K"), "the older clear must not undo the add");
        pollerReads(1);
        assertTrue(repository.contains("K"));
        assertEquals(0, localWrites().size());
    }

    @Test
    void addThatTimesOutButIsWrittenIsUndone() throws Exception {
        Future<Boolean> add = sendOnExchangeThread(() -> repository.add("K")); // offset 0
        producer.errorNext(
                new TimeoutException("Expiring 1 record(s) for test-topic-0: 120000 ms has passed since batch creation"));

        // the timeout does not tell whether the add was written: a remove of the key is written to undo it
        assertTrue(sent.tryAcquire(5, TimeUnit.SECONDS), "a remove of the key is sent after the add failed");
        assertEquals("remove", producer.history().get(1).value());
        assertFalse(repository.contains("K"));

        // the add was written after all, and the poller reads it while the remove is in flight
        pollerReads(0);
        assertFalse(repository.contains("K"), "the add that failed must not be applied");

        // add() fails once the remove is written
        producer.completeNext(); // offset 1
        ExecutionException e = assertThrows(ExecutionException.class, () -> add.get(5, TimeUnit.SECONDS));
        assertInstanceOf(RuntimeCamelException.class, e.getCause());
        pollerReads(1);
        assertFalse(repository.contains("K"));

        // the redelivery of the message is accepted
        Future<Boolean> redelivery = sendOnExchangeThread(() -> repository.add("K")); // offset 2
        producer.completeNext();
        assertTrue(redelivery.get(5, TimeUnit.SECONDS));
    }

    @Test
    void addInterruptedWhileWaitingIsUndone() throws Exception {
        AtomicReference<Exception> failure = new AtomicReference<>();
        Thread exchange = new Thread(() -> {
            try {
                repository.add("K");
            } catch (Exception e) {
                failure.set(e);
            }
        });
        exchange.start();
        assertTrue(sent.tryAcquire(5, TimeUnit.SECONDS)); // offset 0

        // the exchange thread is interrupted while it waits for the add
        exchange.interrupt();
        assertTrue(sent.tryAcquire(5, TimeUnit.SECONDS), "a remove of the key is sent after the add was interrupted");
        exchange.join(5000);
        assertFalse(exchange.isAlive());
        assertInstanceOf(RuntimeCamelException.class, failure.get());

        // the producer writes both records, the add first
        producer.completeNext(); // offset 0
        producer.completeNext(); // offset 1
        pollerReads(0);
        pollerReads(1);
        assertFalse(repository.contains("K"));

        Future<Boolean> redelivery = sendOnExchangeThread(() -> repository.add("K")); // offset 2
        producer.completeNext();
        assertTrue(redelivery.get(5, TimeUnit.SECONDS));
    }

    @Test
    void addThatFailsWithoutBeingWrittenIsNotUndone() throws Exception {
        Future<Boolean> add = sendOnExchangeThread(() -> repository.add("K"));
        producer.errorNext(new RecordTooLargeException("too large"));
        assertThrows(ExecutionException.class, () -> add.get(5, TimeUnit.SECONDS));

        assertEquals(1, producer.history().size());
        assertFalse(repository.contains("K"));
        assertEquals(0, localWrites().size());
    }

    @Test
    void addThatTimesOutKeepsTheAddOfAPeer() throws Exception {
        Future<Boolean> add = sendOnExchangeThread(() -> repository.add("K")); // offset 0
        // another instance adds the key, and the poller reads it while the add of this instance is in flight
        producer.send(new ProducerRecord<>(TOPIC, "K", "add")); // offset 1
        assertTrue(sent.tryAcquire(5, TimeUnit.SECONDS));
        pollerReads(1);

        producer.errorNext(new TimeoutException("Expiring 1 record(s)"));
        assertThrows(ExecutionException.class, () -> add.get(5, TimeUnit.SECONDS));

        // the add of the peer cannot be told from the add of this instance: no remove is written, and the key stays
        assertEquals(2, producer.history().size());
        assertTrue(repository.contains("K"));
    }

    // ------------------------------------------------------------------------------------------------------------------

    /** Calls add or remove on an exchange thread, and waits until it has sent its record. */
    private Future<Boolean> sendOnExchangeThread(Callable<Boolean> call) throws InterruptedException {
        Future<Boolean> answer = exchanges.submit(call);
        assertTrue(sent.tryAcquire(5, TimeUnit.SECONDS), "the record is sent");
        return answer;
    }

    /** The record sent n-th is in the topic at offset n, and the poller reads and applies it. */
    private void pollerReads(int offset) throws InterruptedException {
        ProducerRecord<String, String> record = producer.history().get(offset);
        consumer.addRecord(new ConsumerRecord<>(TOPIC, 0, offset, record.key(), record.value()));
        assertTrue(applied.tryAcquire(5, TimeUnit.SECONDS), "the poller applies the record");
    }

    /** Another instance writes a record to the topic: it takes the next offset. */
    private void peerWrites(String key, String action) throws InterruptedException {
        producer.send(new ProducerRecord<>(TOPIC, key, action));
        assertTrue(sent.tryAcquire(5, TimeUnit.SECONDS));
        producer.completeNext();
    }

    private Map<?, ?> localWrites() throws Exception {
        Field field = KafkaIdempotentRepository.class.getDeclaredField("localWrites");
        field.setAccessible(true);
        Map<?, ?> localWrites = (Map<?, ?>) field.get(repository);
        synchronized (localWrites) {
            return Map.copyOf(localWrites);
        }
    }

    private static void setField(Object target, String fieldName, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }
}
