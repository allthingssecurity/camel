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
package org.apache.camel.processor.keyvalue.kafka;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Properties;

import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.processor.idempotent.kafka.KafkaChangelog;
import org.apache.camel.support.DefaultExchange;
import org.apache.camel.support.KeyValueAggregationRepository;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The repository is a key-value store (documented as unlimited by default), so a key must not disappear because other
 * keys were written after it.
 * <p>
 * No broker is needed: {@code start()} creates the local replica and then fails to reach the (absent) broker within the
 * short API timeout set below. The test then replaces the changelog with one whose producer is a {@link MockProducer},
 * so that the repository writes succeed.
 */
public class KafkaKeyValueRepositoryKeepsAllKeysTest {

    private DefaultCamelContext context;
    private KafkaKeyValueRepository repository;
    private MockProducer<String, byte[]> producer;

    @BeforeEach
    void setUp() throws Exception {
        context = new DefaultCamelContext();
        context.start();
        repository = startedRepository(0);
    }

    private KafkaKeyValueRepository startedRepository(int maxCacheSize) throws Exception {
        Properties consumerConfig = new Properties();
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:1");
        consumerConfig.put(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, "200");
        consumerConfig.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, "200");
        Properties producerConfig = new Properties();
        producerConfig.put("bootstrap.servers", "localhost:1");

        KafkaKeyValueRepository answer = new KafkaKeyValueRepository("kv-topic", consumerConfig, producerConfig);
        answer.setCamelContext(context);
        answer.setStartupOnly(true);
        if (maxCacheSize > 0) {
            answer.setMaxCacheSize(maxCacheSize);
        }
        // creates the local replica, then fails to sync from the topic as there is no broker
        assertThrows(Exception.class, answer::start);

        producer = new MockProducer<>(true, null, new StringSerializer(), new ByteArraySerializer());
        KafkaChangelog<byte[]> changelog = new KafkaChangelog<>(
                "KafkaKeyValueRepository", "kv-topic", new Properties(), new Properties(), ByteArrayDeserializer.class,
                ByteArraySerializer.class, 100, true, r -> {
                });
        Method setProducer = KafkaChangelog.class.getDeclaredMethod("setProducer", Producer.class);
        setProducer.setAccessible(true);
        setProducer.invoke(changelog, producer);
        Field field = KafkaKeyValueRepository.class.getDeclaredField("changelog");
        field.setAccessible(true);
        field.set(answer, changelog);
        return answer;
    }

    @AfterEach
    void tearDown() {
        context.stop();
    }

    @Test
    void keyIsKeptAfterManyOtherKeys() {
        repository.put("first", "value-of-first");
        for (int i = 0; i < 1500; i++) {
            repository.put("key-" + i, "value-" + i);
        }

        assertEquals(1501, producer.history().size());
        assertEquals("value-of-first", repository.get("first"), "the first key is still in the store");
        assertTrue(repository.contains("first"));
        assertEquals(1501, repository.size());
    }

    @Test
    void aggregationGroupIsKeptWhileOtherGroupsAreAggregated() {
        KeyValueAggregationRepository aggregation = new KeyValueAggregationRepository(repository);
        aggregation.setCamelContext(context);
        aggregation.setUseRecovery(false);

        Exchange open = new DefaultExchange(context);
        open.getMessage().setBody("A1+A2");
        aggregation.add(context, "group-a", open);
        for (int i = 0; i < 1500; i++) {
            Exchange other = new DefaultExchange(context);
            other.getMessage().setBody("B" + i);
            aggregation.add(context, "group-" + i, other);
        }

        Exchange found = aggregation.get(context, "group-a");
        assertNotNull(found, "the open group-a must still be in the aggregation repository");
        assertEquals("A1+A2", found.getMessage().getBody(String.class));
    }

    @Test
    void maxCacheSizeStillBoundsTheLocalReplica() throws Exception {
        KafkaKeyValueRepository bounded = startedRepository(10);
        for (int i = 0; i < 20; i++) {
            bounded.put("key-" + i, "value-" + i);
        }

        assertEquals(10, bounded.size());
        assertEquals("value-19", bounded.get("key-19"));
    }

    @Test
    void maxCacheSizeDefaultsToUnlimited() {
        assertEquals(0, new KafkaKeyValueRepository().getMaxCacheSize());
    }
}
