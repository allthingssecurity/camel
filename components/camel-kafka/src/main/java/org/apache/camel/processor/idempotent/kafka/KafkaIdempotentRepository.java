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

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.camel.CamelContext;
import org.apache.camel.CamelContextAware;
import org.apache.camel.api.management.ManagedOperation;
import org.apache.camel.api.management.ManagedResource;
import org.apache.camel.spi.Configurer;
import org.apache.camel.spi.IdempotentRepository;
import org.apache.camel.spi.Metadata;
import org.apache.camel.support.LRUCacheFactory;
import org.apache.camel.support.service.ServiceSupport;
import org.apache.camel.util.ObjectHelper;
import org.apache.camel.util.StringHelper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Kafka topic-based implementation of {@link org.apache.camel.spi.IdempotentRepository}. Uses a local cache of
 * previously seen Message IDs. Mutations that come in via the ({@link #add(String)}), or {@link #remove(String)} method
 * will update the local cache and broadcast the change in state on a Kafka topic to other instances. The cache is
 * back-filled from the topic by a Kafka consumer. The topic used must be unique per logical repository (i.e. two routes
 * de-duplicate using different repositories, and different topics). This class makes no assumptions about the number of
 * partitions (it is designed to consume from all at the same time), or replication factor of the topic. Each repository
 * instance that uses the topic (e.g. typically on different machines running in parallel) controls its own consumer
 * group, so in a cluster of 10 Camel processes using the same topic each will control its own offset. On startup, the
 * instance consumes the full content of the topic, rebuilding the cache to the latest state. To use, this repository
 * must be placed in the Camel registry.
 */
@Metadata(label = "bean",
          description = "Idempotent repository that uses Kafka to store message ids. Uses a local cache of previously seen Message IDs."
                        + " The topic used must be unique per logical repository (i.e. two routes de-duplicate using different repositories, and different topics)"
                        + " On startup, the instance consumes the full content of the topic, rebuilding the cache to the latest state.",
          annotations = { "interfaceName=org.apache.camel.spi.IdempotentRepository" })
@Configurer(metadataOnly = true)
@ManagedResource(description = "Kafka IdempotentRepository")
public class KafkaIdempotentRepository extends ServiceSupport implements IdempotentRepository, CamelContextAware {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaIdempotentRepository.class);

    private static final int DEFAULT_MAXIMUM_CACHE_SIZE = 1000;
    private static final int DEFAULT_POLL_DURATION_MS = 100;

    private CamelContext camelContext;
    private final AtomicLong cacheCounter = new AtomicLong();
    // internal properties
    private Map<String, Object> cache;
    private KafkaChangelog<String> changelog;
    // the keys changed by this instance whose records the background poller has not read back yet, at most
    // maxCacheSize without a send in flight (the least recently changed is forgotten first); also the lock that orders
    // the changes of the cache by this instance and by the poller
    private final Map<String, LocalWrite> localWrites = new LinkedHashMap<>(16, 0.75f, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, LocalWrite> eldest) {
            // a key whose record the poller does not read back (a partition added to the topic after the start), or
            // a poller that lags far behind: forget it, and apply its older records as before
            return size() > maxCacheSize && eldest.getValue().pending == 0;
        }
    };

    private Properties producerConfig;
    private Properties consumerConfig;

    // configurable
    @Metadata(description = "Sets the name of the Kafka topic used by this idempotent repository."
                            + " Each functionally-separate repository should use a different topic.",
              required = true)
    private String topic;
    @Metadata(description = "The URL for the kafka brokers to use", required = true)
    private String bootstrapServers;
    @Metadata(description = "A string that uniquely identifies the group of consumer processes to which this consumer belongs. By setting the"
                            + " same group id, multiple processes can indicate that they are all part of the same consumer group.")
    private String groupId;
    @Metadata(description = "Sets the maximum size of the local key cache. The value must be greater than 0.",
              defaultValue = "" + DEFAULT_MAXIMUM_CACHE_SIZE)
    private int maxCacheSize = DEFAULT_MAXIMUM_CACHE_SIZE;
    @Metadata(description = "Sets the poll duration of the Kafka consumer. The local caches are updated immediately; this value will affect"
                            + " how far behind other peers in the cluster are, which are updating their caches from the topic, relative to the"
                            + " idempotent consumer instance issued the cache action message. The default value of this is 100"
                            + " If setting this value explicitly, be aware that there is a tradeoff between"
                            + " the remote cache liveness and the volume of network traffic between this repository's consumer and the Kafka"
                            + " brokers. The cache warmup process also depends on there being one poll that fetches nothing - this indicates that"
                            + " the stream has been consumed up to the current point. If the poll duration is excessively long for the rate at"
                            + " which messages are sent on the topic, there exists a possibility that the cache cannot be warmed up and will"
                            + " operate in an inconsistent state relative to its peers until it catches up.",
              defaultValue = "" + DEFAULT_POLL_DURATION_MS)
    private int pollDurationMs = DEFAULT_POLL_DURATION_MS;
    @Metadata(description = "Whether to sync on startup only, or to continue syncing while Camel is running.")
    private boolean startupOnly;

    enum CacheAction {
        add,
        remove,
        clear
    }

    public KafkaIdempotentRepository() {
    }

    public KafkaIdempotentRepository(String topic, String bootstrapServers) {
        this(topic, bootstrapServers, DEFAULT_MAXIMUM_CACHE_SIZE, DEFAULT_POLL_DURATION_MS);
    }

    public KafkaIdempotentRepository(String topic, String bootstrapServers, String groupId) {
        this(topic, bootstrapServers, DEFAULT_MAXIMUM_CACHE_SIZE, DEFAULT_POLL_DURATION_MS, groupId);
    }

    public KafkaIdempotentRepository(String topic, String bootstrapServers, int maxCacheSize, int pollDurationMs) {
        this.topic = topic;
        this.bootstrapServers = bootstrapServers;
        this.maxCacheSize = maxCacheSize;
        this.pollDurationMs = pollDurationMs;
    }

    public KafkaIdempotentRepository(String topic, Properties consumerConfig, Properties producerConfig) {
        this(topic, consumerConfig, producerConfig, DEFAULT_MAXIMUM_CACHE_SIZE, DEFAULT_POLL_DURATION_MS);
    }

    @Deprecated
    public KafkaIdempotentRepository(String topic, Properties consumerConfig, Properties producerConfig, String groupId) {
        this(topic, consumerConfig, producerConfig, DEFAULT_MAXIMUM_CACHE_SIZE, DEFAULT_POLL_DURATION_MS, groupId);
    }

    public KafkaIdempotentRepository(String topic, Properties consumerConfig, Properties producerConfig, int maxCacheSize,
                                     int pollDurationMs) {
        this.topic = topic;
        this.consumerConfig = consumerConfig;
        this.producerConfig = producerConfig;
        this.maxCacheSize = maxCacheSize;
        this.pollDurationMs = pollDurationMs;
    }

    public KafkaIdempotentRepository(String topic, String bootstrapServers, int maxCacheSize, int pollDurationMs,
                                     String groupId) {
        this.topic = topic;
        this.bootstrapServers = bootstrapServers;
        this.maxCacheSize = maxCacheSize;
        this.pollDurationMs = pollDurationMs;
        this.groupId = groupId;
    }

    public KafkaIdempotentRepository(String topic, Properties consumerConfig, Properties producerConfig, int maxCacheSize,
                                     int pollDurationMs, String groupId) {
        this.topic = topic;
        this.consumerConfig = consumerConfig;
        this.producerConfig = producerConfig;
        this.maxCacheSize = maxCacheSize;
        this.pollDurationMs = pollDurationMs;
        this.groupId = groupId;
    }

    public String getTopic() {
        return topic;
    }

    /**
     * Sets the name of the Kafka topic used by this idempotent repository. Each functionally-separate repository should
     * use a different topic.
     *
     * @param topic The topic name.
     */
    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    /**
     * Sets the bootstrap.servers property on the internal Kafka producer and consumer. Use this as shorthand if not
     * setting {@link #consumerConfig} and {@link #producerConfig}. If used, this component will apply sensible default
     * configurations for the producer and consumer.
     */
    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public boolean isStartupOnly() {
        return startupOnly;
    }

    /**
     * Whether to sync on startup only, or to continue syncing while Camel is running.
     */
    public void setStartupOnly(boolean startupOnly) {
        this.startupOnly = startupOnly;
    }

    public Properties getProducerConfig() {
        return producerConfig;
    }

    /**
     * Sets the properties that will be used by the Kafka producer. Overrides {@link #bootstrapServers}, so must define
     * the
     *
     * <pre>
     * bootstrap.servers
     * </pre>
     * <p>
     * property itself. Prefer using {@link #bootstrapServers} for default configuration unless you specifically need
     * non-standard configuration options such as SSL/SASL.
     *
     * @param producerConfig The producer configuration properties.
     */
    public void setProducerConfig(Properties producerConfig) {
        this.producerConfig = producerConfig;
    }

    public Properties getConsumerConfig() {
        return consumerConfig;
    }

    /**
     * Sets the properties that will be used by the Kafka consumer. Overrides {@link #bootstrapServers}, so must define
     * the
     *
     * <pre>
     * bootstrap.servers
     * </pre>
     * <p>
     * property itself. Prefer using {@link #bootstrapServers} for default configuration unless you specifically need
     * non-standard configuration options such as SSL/SASL.
     *
     * @param consumerConfig The consumer configuration properties.
     */
    public void setConsumerConfig(Properties consumerConfig) {
        this.consumerConfig = consumerConfig;
    }

    public int getMaxCacheSize() {
        return maxCacheSize;
    }

    /**
     * Sets the maximum size of the local key cache.
     *
     * @param maxCacheSize The maximum key cache size.
     */
    public void setMaxCacheSize(int maxCacheSize) {
        if (maxCacheSize <= 0) {
            throw new IllegalArgumentException("maxCacheSize must be greater than 0, was: " + maxCacheSize);
        }
        this.maxCacheSize = maxCacheSize;
    }

    public int getPollDurationMs() {
        return pollDurationMs;
    }

    /**
     * Sets the poll duration of the Kafka consumer. The local caches are updated immediately; this value will affect
     * how far behind other peers in the cluster are, which are updating their caches from the topic, relative to the
     * idempotent consumer instance issued the cache action message. The default value of this is
     * {@link #DEFAULT_POLL_DURATION_MS}. If setting this value explicitly, be aware that there is a tradeoff between
     * the remote cache liveness and the volume of network traffic between this repository's consumer and the Kafka
     * brokers. The cache warmup process also depends on there being one poll that fetches nothing - this indicates that
     * the stream has been consumed up to the current point. If the poll duration is excessively long for the rate at
     * which messages are sent on the topic, there exists a possibility that the cache cannot be warmed up and will
     * operate in an inconsistent state relative to its peers until it catches up.
     *
     * @param pollDurationMs The poll duration in milliseconds.
     */
    public void setPollDurationMs(int pollDurationMs) {
        this.pollDurationMs = pollDurationMs;
    }

    public String getGroupId() {
        return groupId;
    }

    /**
     * A string that uniquely identifies the group of consumer processes to which this consumer belongs. By setting the
     * same group id, multiple processes can indicate that they are all part of the same consumer group.
     */
    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    @Override
    public void setCamelContext(CamelContext camelContext) {
        this.camelContext = camelContext;
    }

    @Override
    public CamelContext getCamelContext() {
        return this.camelContext;
    }

    @Override
    protected void doStart() throws Exception {
        ObjectHelper.notNull(camelContext, "camelContext");
        StringHelper.notEmpty(topic, "topic");

        this.cache = LRUCacheFactory.newLRUCache(maxCacheSize);
        synchronized (localWrites) {
            localWrites.clear();
        }

        consumerConfig = KafkaChangelog.consumerConfig(consumerConfig, bootstrapServers, groupId);
        producerConfig = KafkaChangelog.producerConfig(producerConfig, bootstrapServers);
        changelog = new KafkaChangelog<>(
                "KafkaIdempotentRepository", topic, consumerConfig, producerConfig, StringDeserializer.class,
                StringSerializer.class, pollDurationMs,
                startupOnly, this::addToCache);
        changelog.start(camelContext, this);
    }

    @Override
    protected void doStop() throws Exception {
        if (changelog != null) {
            changelog.stop();
        }
        LOG.debug("Stopped KafkaIdempotentRepository. Cache counter: {}", cacheCounter.get());
    }

    private void addToCache(ConsumerRecord<String, String> consumerRecord) {
        cacheCounter.incrementAndGet();
        String value = consumerRecord.value();
        String messageId = consumerRecord.key();
        // a record without a value (a tombstone) or an add or remove without a key is not written by this repository:
        // skip it, so the records after it in the same poll are still applied, and the start does not fail
        if (value == null || (messageId == null && !CacheAction.clear.name().equals(value))) {
            LOG.warn(
                    "Unexpected record with key:\"{}\" and value:\"{}\" received on [topic:{}, partition:{}, offset:{}]. Ignoring.",
                    messageId, value, consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset());
            return;
        }
        CacheAction action;
        try {
            action = CacheAction.valueOf(value);
        } catch (IllegalArgumentException iax) {
            LOG.warn(
                    "Unexpected action value:\"{}\" received on [topic:{}, partition:{}, offset:{}]. Ignoring.",
                    value, consumerRecord.topic(),
                    consumerRecord.partition(), consumerRecord.offset());
            return;
        }
        synchronized (localWrites) {
            if (action == CacheAction.clear) {
                clearFromTopic(consumerRecord.partition(), consumerRecord.offset());
            } else {
                applyFromTopic(messageId, action, consumerRecord.partition(), consumerRecord.offset());
            }
        }
    }

    /**
     * Applies an add or remove record of the topic, unless it is older than a change of the same key by this instance.
     * {@link #add(String)} and {@link #remove(String)} change the cache at once, and the poller reads the older records
     * of the key (the earlier records of this instance, or of a peer) later: such a record would undo the newer change.
     * The records of a key are ordered by their offset, as the producer writes all of them to one partition. Called
     * with the lock held.
     */
    private void applyFromTopic(String key, CacheAction action, int partition, long offset) {
        LocalWrite write = localWrites.get(key);
        if (write == null) {
            applyToCache(key, action);
        } else if (write.isNewerThan(partition, offset)) {
            LOG.debug("Ignoring {} of messageId:{} at [partition:{}, offset:{}], older than a change by this instance",
                    action, key, partition, offset);
            if (write.pending == 0 && write.isReadBackBy(partition, offset)) {
                localWrites.remove(key);
            }
        } else if (write.pending > 0) {
            // the offset of the change in flight is not known yet: decide when its send completes
            write.defer(action, partition, offset);
        } else {
            applyToCache(key, action);
            if (write.isReadBackBy(partition, offset)) {
                localWrites.remove(key);
            }
        }
    }

    /**
     * Applies a clear record of the topic, but keeps the keys changed by this instance after it, or with a change in
     * flight. A clear has no key, so on a topic with more than one partition it is not ordered against the records of
     * the keys. Called with the lock held.
     */
    private void clearFromTopic(int partition, long offset) {
        Map<String, Object> keep = new HashMap<>();
        for (Iterator<Map.Entry<String, LocalWrite>> it = localWrites.entrySet().iterator(); it.hasNext();) {
            Map.Entry<String, LocalWrite> entry = it.next();
            LocalWrite write = entry.getValue();
            boolean newer = write.isNewerThan(partition, offset);
            if (!newer && write.pending > 0) {
                write.defer(CacheAction.remove, partition, offset);
            } else if (!newer && write.isReadBackBy(partition, offset)) {
                it.remove();
            }
            Object value = cache.get(entry.getKey());
            if (value != null && (newer || write.pending > 0)) {
                keep.put(entry.getKey(), value);
            }
        }
        cache.clear();
        cache.putAll(keep);
    }

    private void applyToCache(String key, CacheAction action) {
        if (action == CacheAction.add) {
            LOG.debug("Adding to cache messageId:{}", key);
            cache.put(key, key);
        } else {
            LOG.debug("Removing from cache messageId:{}", key);
            cache.remove(key);
        }
    }

    /**
     * Records that a change of the key by this instance is in flight. Called with the lock held, together with the
     * change of the cache. Without the background poller (startupOnly) no record is applied after the start, so there
     * is nothing to order.
     */
    private void beginLocalWrite(String key) {
        if (!startupOnly) {
            localWrites.computeIfAbsent(key, k -> new LocalWrite()).pending++;
        }
    }

    /**
     * Records the offset of a change of the key by this instance once its send completed (null if it failed), and
     * applies the newest record of the key that the poller read meanwhile, unless it is older than the change.
     */
    private void endLocalWrite(String key, RecordMetadata metadata) {
        synchronized (localWrites) {
            LocalWrite write = localWrites.get(key);
            if (write == null) {
                return;
            }
            write.pending--;
            write.written(metadata);
            if (write.pending > 0) {
                return;
            }
            if (write.deferredAction != null) {
                CacheAction action = write.deferredAction;
                write.deferredAction = null;
                applyFromTopic(key, action, write.deferredPartition, write.deferredOffset);
            }
            if (write.partition < 0) {
                // nothing of this instance in the topic to order the records against
                localWrites.remove(key, write);
            }
        }
    }

    @Override
    public boolean add(String key) {
        // the poller stores key -> key for an add of a peer, so roll back only the value put by this call
        Object token = new Object();
        synchronized (localWrites) {
            if (cache.putIfAbsent(key, token) != null) {
                return false;
            }
            beginLocalWrite(key);
        }
        RecordMetadata metadata;
        try {
            metadata = broadcastAction(key, CacheAction.add);
        } catch (Exception e) {
            boolean undo = false;
            synchronized (localWrites) {
                boolean rolledBack = cache.remove(key, token);
                // applies the records of the key read meanwhile: an add read from the topic puts the key back
                endLocalWrite(key, null);
                if (rolledBack && !cache.containsKey(key) && isUnknownOutcome(e)) {
                    // this call holds the key until its remove is written, as while its add was in flight, so a later
                    // add of the key by this instance is written after the remove
                    cache.put(key, token);
                    beginLocalWrite(key);
                    undo = true;
                }
            }
            if (undo) {
                undo(key, token);
            }
            throw e;
        }
        endLocalWrite(key, metadata);
        return true;
    }

    /**
     * Whether the failed send may still have written the record: the delivery timed out, or the thread was interrupted
     * while waiting for the result.
     */
    private static boolean isUnknownOutcome(Exception e) {
        Throwable cause = e.getCause();
        if (cause instanceof InterruptedException) {
            return true;
        }
        if (cause instanceof ExecutionException) {
            cause = cause.getCause();
        }
        return cause instanceof RetriableException;
    }

    /**
     * The send of an add failed, but the record may have been written. Then every instance, and every later start,
     * would take the redelivery of the message as a duplicate. Writes a remove of the key, so the add is undone if it
     * was written: the producer writes it after the add. This is best effort: the remove can fail too, and if an add of
     * the key was read from the topic while waiting for the add (this instance's own add cannot be told from one of a
     * peer), the key is kept. Waits until the remove is written, before the add fails, so that the redelivery of the
     * message, on any instance, comes after it. Called without the lock, as the producer can block (up to max.block.ms
     * when it has no metadata of the topic), with the key held by the token of the add.
     */
    private void undo(String key, Object token) {
        LOG.warn("The add of messageId:{} failed but may have been written to topic:{}. Writing a remove of the key.", key,
                topic);
        RecordMetadata metadata = null;
        try {
            metadata = changelog.sendAsync(key, CacheAction.remove.name()).get();
        } catch (InterruptedException e) {
            // the remove is with the producer: it is written after the add, but the redelivery may not wait for it
            Thread.currentThread().interrupt();
        } catch (ExecutionException e) {
            LOG.warn("Cannot write the remove of messageId:{} to topic:{} due to: {}", key, topic,
                    e.getCause().getMessage(), e.getCause());
        } catch (Exception e) {
            LOG.warn("Cannot write the remove of messageId:{} to topic:{} due to: {}", key, topic, e.getMessage(), e);
        } finally {
            synchronized (localWrites) {
                cache.remove(key, token);
                endLocalWrite(key, metadata);
            }
        }
    }

    private RecordMetadata broadcastAction(String key, CacheAction action) {
        LOG.debug("Broadcasting action:{} for key:{}", action, key);
        ObjectHelper.notNull(changelog, "changelog");
        return changelog.send(key, action.toString());
    }

    @Override
    @ManagedOperation(description = "Does the store contain the given key")
    public boolean contains(String key) {
        return cache.containsKey(key);
    }

    @Override
    @ManagedOperation(description = "Remove the key from the store")
    public boolean remove(String key) {
        // update the local cache and broadcast the removal on the topic, which will be reflected at a later point in
        // any peers
        synchronized (localWrites) {
            cache.remove(key);
            beginLocalWrite(key);
        }
        RecordMetadata metadata = null;
        try {
            metadata = broadcastAction(key, CacheAction.remove);
        } finally {
            endLocalWrite(key, metadata);
        }
        return true;
    }

    @Override
    public boolean confirm(String key) {
        return true; // no-op
    }

    @Override
    public void clear() {
        synchronized (localWrites) {
            cache.clear();
        }
        broadcastAction(null, CacheAction.clear);
    }

    @ManagedOperation(description = "Number of sync events received from the kafka topic")
    public long getCacheCounter() {
        return cacheCounter.get();
    }

    @ManagedOperation(description = "Number of elements currently in the cache")
    public long getCacheSize() {
        return cache != null ? cache.size() : 0;
    }

    /**
     * The changes of one key by this instance that the poller has not read back yet. Guarded by the lock.
     */
    private static final class LocalWrite {
        // the sends of the key in flight
        int pending;
        // the newest record of the key written by this instance, partition -1 if none
        int partition = -1;
        long offset = -1;
        // the newest record of the key read from the topic while a send was in flight
        CacheAction deferredAction;
        int deferredPartition;
        long deferredOffset;

        boolean isNewerThan(int partition, long offset) {
            return this.partition == partition && offset <= this.offset;
        }

        boolean isReadBackBy(int partition, long offset) {
            return this.partition == partition && offset >= this.offset;
        }

        void written(RecordMetadata metadata) {
            // no offset with acks=0
            if (metadata != null && metadata.hasOffset()
                    && (metadata.partition() != partition || metadata.offset() > offset)) {
                partition = metadata.partition();
                offset = metadata.offset();
            }
        }

        void defer(CacheAction action, int partition, long offset) {
            if (deferredAction == null || deferredPartition != partition || offset > deferredOffset) {
                deferredAction = action;
                deferredPartition = partition;
                deferredOffset = offset;
            }
        }
    }
}
