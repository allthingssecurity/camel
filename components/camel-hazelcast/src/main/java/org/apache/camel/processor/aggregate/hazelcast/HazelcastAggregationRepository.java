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
package org.apache.camel.processor.aggregate.hazelcast;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.TimeUnit;

import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.EntryView;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionOptions;
import com.hazelcast.transaction.TransactionalMap;
import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.RuntimeCamelException;
import org.apache.camel.component.hazelcast.HazelcastSerializationFilterHelper;
import org.apache.camel.component.hazelcast.HazelcastUtil;
import org.apache.camel.spi.Configurer;
import org.apache.camel.spi.InMemoryStrategy;
import org.apache.camel.spi.Metadata;
import org.apache.camel.spi.OptimisticLockingAggregationRepository;
import org.apache.camel.spi.RecoverableAggregationRepository;
import org.apache.camel.support.DefaultExchange;
import org.apache.camel.support.DefaultExchangeHolder;
import org.apache.camel.support.service.ServiceSupport;
import org.apache.camel.util.ObjectHelper;
import org.apache.camel.util.StringHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Hazelcast-based AggregationRepository implementing {@link RecoverableAggregationRepository} and
 * {@link OptimisticLockingAggregationRepository}. Defaults to thread-safe (non-optimistic) locking and recoverable
 * strategy. Hazelcast settings are given to an end-user and can be controlled with repositoryName and
 * persistentRespositoryName, both are {@link com.hazelcast.map.IMap} &lt;String, Exchange&gt;. However
 * HazelcastAggregationRepository can run it's own Hazelcast instance, but obviously no benefits of Hazelcast clustering
 * are gained this way. If the {@link HazelcastAggregationRepository} uses it's own local {@link HazelcastInstance} it
 * will DESTROY this instance on {@link #doStop()}. You should control {@link HazelcastInstance} lifecycle yourself
 * whenever you instantiate {@link HazelcastAggregationRepository} passing a reference to the instance.
 */
@Metadata(label = "bean",
          description = "Aggregation repository that uses Hazelcast Cache to store exchanges.",
          annotations = { "interfaceName=org.apache.camel.spi.AggregationRepository" })
@Configurer(metadataOnly = true)
public class HazelcastAggregationRepository extends ServiceSupport
        implements RecoverableAggregationRepository,
        OptimisticLockingAggregationRepository, InMemoryStrategy {

    protected static final String COMPLETED_SUFFIX = "-completed";

    private static final Logger LOG = LoggerFactory.getLogger(HazelcastAggregationRepository.class.getName());

    protected boolean useLocalHzInstance;
    protected IMap<String, DefaultExchangeHolder> cache;
    protected IMap<String, DefaultExchangeHolder> persistedCache;
    @Metadata(description = "Name of cache to use", required = true)
    protected String mapName;
    @Metadata(description = "To use an existing Hazelcast instance instead of local")
    protected HazelcastInstance hazelcastInstance;
    @Metadata(label = "advanced", description = "Name of cache to use for completed exchanges")
    protected String persistenceMapName;
    @Metadata(description = "Whether to use optimistic locking")
    protected boolean optimistic;
    @Metadata(description = "Whether or not recovery is enabled", defaultValue = "true")
    protected boolean useRecovery = true;
    @Metadata(description = "Sets the interval between recovery scans", defaultValue = "5000")
    protected long recoveryInterval = 5000;
    @Metadata(description = "Sets an optional dead letter channel which exhausted recovered Exchange should be send to.")
    protected String deadLetterUri;
    @Metadata(description = "Sets an optional limit of the number of redelivery attempt of recovered Exchange should be attempted, before its exhausted."
                            + " When this limit is hit, then the Exchange is moved to the dead letter channel.",
              defaultValue = "3")
    protected int maximumRedeliveries = 3;
    @Metadata(label = "advanced", security = "insecure:serialization",
              description = "Whether headers on the Exchange that are Java objects and Serializable should be included and saved to the repository")
    protected boolean allowSerializedHeaders;
    @Metadata(label = "advanced", defaultValue = "10000",
              description = "With optimistic locking, how long (in millis) adding or removing a group waits for the lock of"
                            + " the correlation key, which another aggregator holds while it checks and writes the group."
                            + " When the lock is not acquired in time, the update fails with an OptimisticLockingException"
                            + " and the Aggregate EIP tries again as its optimistic lock retry policy says.")
    protected long optimisticLockTimeout = 10000;

    // optimistic locking: the groups that get() returned, each with the key, and the version, exchange id and aggregated
    // size of the stored entry as it was read
    private final Map<Exchange, ReadGroup> readGroups = Collections.synchronizedMap(new WeakHashMap<>());

    private record ReadGroup(String key, long version, String exchangeId, Integer size) {
    }

    public HazelcastAggregationRepository() {
    }

    /**
     * Creates new {@link HazelcastAggregationRepository} that defaults to non-optimistic locking with recoverable
     * behavior and a local Hazelcast instance. Recoverable repository name defaults to {@code repositoryName} +
     * "-compeleted".
     *
     * @param repositoryName {@link IMap} repository name;
     */
    public HazelcastAggregationRepository(final String repositoryName) {
        mapName = repositoryName;
        persistenceMapName = String.format("%s%s", mapName, COMPLETED_SUFFIX);
    }

    /**
     * Creates new {@link HazelcastAggregationRepository} that defaults to non-optimistic locking with recoverable
     * behavior and a local Hazelcast instance.
     *
     * @param repositoryName           {@link IMap} repository name;
     * @param persistentRepositoryName {@link IMap} recoverable repository name;
     */
    public HazelcastAggregationRepository(final String repositoryName, final String persistentRepositoryName) {
        mapName = repositoryName;
        persistenceMapName = persistentRepositoryName;
    }

    /**
     * Creates new {@link HazelcastAggregationRepository} with recoverable behavior and a local Hazelcast instance.
     * Recoverable repository name defaults to {@code repositoryName} + "-compeleted".
     *
     * @param repositoryName {@link IMap} repository name;
     * @param optimistic     whether to use optimistic locking manner.
     */
    public HazelcastAggregationRepository(final String repositoryName, boolean optimistic) {
        this(repositoryName);
        this.optimistic = optimistic;
    }

    /**
     * Creates new {@link HazelcastAggregationRepository} with recoverable behavior and a local Hazelcast instance.
     *
     * @param repositoryName           {@link IMap} repository name;
     * @param persistentRepositoryName {@link IMap} recoverable repository name;
     * @param optimistic               whether to use optimistic locking manner.
     */
    public HazelcastAggregationRepository(final String repositoryName, final String persistentRepositoryName,
                                          boolean optimistic) {
        this(repositoryName, persistentRepositoryName);
        this.optimistic = optimistic;
    }

    /**
     * Creates new {@link HazelcastAggregationRepository} that defaults to non-optimistic locking with recoverable
     * behavior. Recoverable repository name defaults to {@code repositoryName} + "-compeleted".
     *
     * @param repositoryName {@link IMap} repository name;
     * @param hzInstanse     externally configured {@link HazelcastInstance}.
     */
    public HazelcastAggregationRepository(final String repositoryName, HazelcastInstance hzInstanse) {
        this(repositoryName, false);
        this.hazelcastInstance = hzInstanse;
    }

    /**
     * Creates new {@link HazelcastAggregationRepository} that defaults to non-optimistic locking with recoverable
     * behavior.
     *
     * @param repositoryName           {@link IMap} repository name;
     * @param persistentRepositoryName {@link IMap} recoverable repository name;
     * @param hzInstanse               externally configured {@link HazelcastInstance}.
     */
    public HazelcastAggregationRepository(final String repositoryName, final String persistentRepositoryName,
                                          HazelcastInstance hzInstanse) {
        this(repositoryName, persistentRepositoryName, false);
        this.hazelcastInstance = hzInstanse;
    }

    /**
     * Creates new {@link HazelcastAggregationRepository} with recoverable behavior. Recoverable repository name
     * defaults to {@code repositoryName} + "-compeleted".
     *
     * @param repositoryName {@link IMap} repository name;
     * @param optimistic     whether to use optimistic locking manner;
     * @param hzInstance     externally configured {@link HazelcastInstance}.
     */
    public HazelcastAggregationRepository(final String repositoryName, boolean optimistic, HazelcastInstance hzInstance) {
        this(repositoryName, optimistic);
        this.hazelcastInstance = hzInstance;
    }

    /**
     * Creates new {@link HazelcastAggregationRepository} with recoverable behavior.
     *
     * @param repositoryName           {@link IMap} repository name;
     * @param optimistic               whether to use optimistic locking manner;
     * @param persistentRepositoryName {@link IMap} recoverable repository name;
     * @param hzInstance               externally configured {@link HazelcastInstance}.
     */
    public HazelcastAggregationRepository(final String repositoryName, final String persistentRepositoryName,
                                          boolean optimistic, HazelcastInstance hzInstance) {
        this(repositoryName, persistentRepositoryName, optimistic);
        this.hazelcastInstance = hzInstance;
    }

    @Override
    public Exchange add(CamelContext camelContext, String key, Exchange oldExchange, Exchange newExchange)
            throws OptimisticLockingException {
        if (!optimistic) {
            throw new UnsupportedOperationException();
        }
        LOG.trace("Adding an Exchange with ID {} for key {} in an optimistic manner.", newExchange.getExchangeId(), key);
        if (oldExchange == null) {
            DefaultExchangeHolder holder = DefaultExchangeHolder.marshal(newExchange, true, allowSerializedHeaders);
            final DefaultExchangeHolder misbehaviorHolder = cache.putIfAbsent(key, holder);
            if (misbehaviorHolder != null) {
                Exchange misbehaviorEx = unmarshallExchange(camelContext, misbehaviorHolder);
                LOG.error(
                        "Optimistic locking failed for exchange with key {}: IMap#putIfAbsend returned Exchange with ID {}, while it's expected no exchanges to be returned",
                        key, misbehaviorEx != null ? misbehaviorEx.getExchangeId() : "<null>");
                throw new OptimisticLockingException();
            }
        } else {
            DefaultExchangeHolder newHolder = DefaultExchangeHolder.marshal(newExchange, true, allowSerializedHeaders);
            ReadGroup read = readGroup(key, oldExchange);
            if (read != null) {
                lock(key);
                try {
                    checkUnchanged(camelContext, key, read);
                    cache.set(key, newHolder);
                } finally {
                    cache.unlock(key);
                }
            } else {
                DefaultExchangeHolder oldHolder = DefaultExchangeHolder.marshal(oldExchange, true, allowSerializedHeaders);
                if (!cache.replace(key, oldHolder, newHolder)) {
                    LOG.error(
                            "Optimistic locking failed for exchange with key {}: IMap#replace returned no Exchanges, while it's expected to replace one",
                            key);
                    throw new OptimisticLockingException();
                }
            }
        }
        LOG.trace("Added an Exchange with ID {} for key {} in optimistic manner.", newExchange.getExchangeId(), key);
        return oldExchange;
    }

    @Override
    public Exchange add(CamelContext camelContext, String key, Exchange exchange) {
        if (optimistic) {
            throw new UnsupportedOperationException();
        }
        LOG.trace("Adding an Exchange with ID {} for key {} in a thread-safe manner.", exchange.getExchangeId(), key);
        // Use IMap-based locking (community edition compatible)
        cache.lock(key);
        try {
            DefaultExchangeHolder newHolder = DefaultExchangeHolder.marshal(exchange, true, allowSerializedHeaders);
            DefaultExchangeHolder oldHolder = cache.put(key, newHolder);
            return unmarshallExchange(camelContext, oldHolder);
        } finally {
            LOG.trace("Added an Exchange with ID {} for key {} in a thread-safe manner.", exchange.getExchangeId(), key);
            cache.unlock(key);
        }
    }

    @Override
    public Set<String> scan(CamelContext camelContext) {
        if (useRecovery) {
            LOG.trace("Scanning for exchanges to recover in {} context", camelContext.getName());
            Set<String> scanned = Collections.unmodifiableSet(persistedCache.keySet());
            LOG.trace("Found {} keys for exchanges to recover in {} context", scanned.size(), camelContext.getName());
            return scanned;
        } else {
            LOG.warn(
                    "What for to run recovery scans in {} context while repository {} is running in non-recoverable aggregation repository mode?!",
                    camelContext.getName(), mapName);
            return Collections.emptySet();
        }
    }

    @Override
    public Exchange recover(CamelContext camelContext, String exchangeId) {
        LOG.trace("Recovering an Exchange with ID {}.", exchangeId);
        return useRecovery ? unmarshallExchange(camelContext, persistedCache.get(exchangeId)) : null;
    }

    @Override
    public void setRecoveryInterval(long interval, TimeUnit timeUnit) {
        this.recoveryInterval = timeUnit.toMillis(interval);
    }

    @Override
    public void setRecoveryInterval(long interval) {
        this.recoveryInterval = interval;
    }

    @Override
    public long getRecoveryInterval() {
        return recoveryInterval;
    }

    @Override
    public void setUseRecovery(boolean useRecovery) {
        this.useRecovery = useRecovery;
    }

    @Override
    public boolean isUseRecovery() {
        return useRecovery;
    }

    @Override
    public void setDeadLetterUri(String deadLetterUri) {
        this.deadLetterUri = deadLetterUri;
    }

    @Override
    public String getDeadLetterUri() {
        return deadLetterUri;
    }

    @Override
    public void setMaximumRedeliveries(int maximumRedeliveries) {
        this.maximumRedeliveries = maximumRedeliveries;
    }

    @Override
    public int getMaximumRedeliveries() {
        return maximumRedeliveries;
    }

    /**
     * Gets the group of the correlation key.
     * <p>
     * With optimistic locking, this method keeps the version of the stored entry for the exchange it returns, with the
     * exchange id and the aggregated size ({@link Exchange#AGGREGATED_SIZE}) of the group.
     * {@link #add(CamelContext, String, Exchange, Exchange)} and {@link #remove(CamelContext, String, Exchange)}, when
     * given that exchange for the same key, lock the key ({@link IMap#tryLock(Object, long, TimeUnit)}), check that the
     * stored entry has the same version, exchange id and aggregated size as when it was read, and write while they hold
     * the lock; else they fail with an {@link OptimisticLockingException}. Hazelcast starts the version of an entry
     * again at 0 when the entry is created after a remove, and also when it is loaded from a map store (after an
     * eviction, or a reload of the map): a new group of the key has another exchange id, and the Aggregate EIP stores a
     * larger aggregated size with every message it adds, so a group that was updated and then loaded again does not
     * look like the group that was read. (A caller that stores groups without an aggregated size, outside the Aggregate
     * EIP, is checked by version and exchange id only; and an exchange id generator whose ids repeat, such as
     * {@code SimpleUuidGenerator} on two members, can give a new group of the key the exchange id of a removed one.)
     * They do not compare the group with the exchange they are given, as the Aggregate EIP sets properties on the group
     * before it removes it, and an aggregation strategy may change the group (or an object that it holds, such as a
     * list) before it is stored again. The serialized form of the group is not compared either, as it can change when a
     * value is serialized again (such as a {@code HashSet}) or serialized by another member. The lock is held only for
     * that check and the write, not while the group is aggregated, and the members of the cluster need no Camel classes
     * for it. An update waits for the lock at most {@link #setOptimisticLockTimeout(long) optimisticLockTimeout} (10
     * seconds by default), then fails with an {@link OptimisticLockingException}, so the Aggregate EIP tries again.
     * Such a wait is only long when the aggregator that holds the lock hangs while it checks and writes (a frozen JVM,
     * a store that does not answer): Hazelcast releases its lock when it removes the member or client from the cluster,
     * which takes about a minute for a member and longer for a client with the default settings. For an exchange that
     * this method did not return for the key, the stored group is compared with the exchange as it is now, in
     * serialized form.
     * <p>
     * In any mode, a group is stored in serialized form: the list of {@code GroupedBodyAggregationStrategy} is an
     * exchange property, stored only with {@code allowSerializedHeaders=true}, and the lists of
     * {@code GroupedExchangeAggregationStrategy} and {@code GroupedMessageAggregationStrategy} hold exchanges and
     * messages, which cannot be serialized, so these two strategies cannot be used with this repository.
     */
    @Override
    public Exchange get(CamelContext camelContext, String key) {
        if (!optimistic) {
            return unmarshallExchange(camelContext, cache.get(key));
        }
        EntryView<String, DefaultExchangeHolder> entry = entryView(key);
        if (entry == null) {
            return null;
        }
        Exchange exchange = unmarshallExchange(camelContext, entry.getValue());
        // the exchange id and size are kept as read, before the caller can change the exchange
        readGroups.put(exchange, new ReadGroup(key, entry.getVersion(), exchange.getExchangeId(), aggregatedSize(exchange)));
        return exchange;
    }

    /**
     * The entry of the key, with its value and version read together. An entry that is only in a map store is loaded
     * first, as {@link IMap#getEntryView(Object)} does not load it.
     */
    private EntryView<String, DefaultExchangeHolder> entryView(String key) {
        EntryView<String, DefaultExchangeHolder> entry = cache.getEntryView(key);
        if (entry == null && cache.containsKey(key)) {
            entry = cache.getEntryView(key);
        }
        return entry;
    }

    private static Integer aggregatedSize(Exchange exchange) {
        return exchange.getProperty(Exchange.AGGREGATED_SIZE, Integer.class);
    }

    /**
     * Locks the key for the check and the write of an update, waiting at most {@link #optimisticLockTimeout}. No lease
     * is given: a lock that ran out during a slow write would let another update pass the check.
     */
    private void lock(String key) {
        boolean locked;
        try {
            locked = cache.tryLock(key, optimisticLockTimeout, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeCamelException("Interrupted while waiting for the lock of the group with key " + key, e);
        }
        if (!locked) {
            LOG.warn("Optimistic locking failed for exchange with key {}: the lock of the key was not acquired in {} ms",
                    key, optimisticLockTimeout);
            throw new OptimisticLockingException();
        }
    }

    /**
     * The group as {@link #get} read it, when it returned the exchange for this key, else {@code null}.
     */
    private ReadGroup readGroup(String key, Exchange exchange) {
        ReadGroup read = readGroups.remove(exchange);
        return read != null && read.key().equals(key) ? read : null;
    }

    /**
     * Checks, while the key is locked, that the stored group is still the one that was read: Hazelcast increments the
     * version of an entry on every write, and starts it again at 0 for an entry that is created after a remove or
     * loaded from a map store, so a new group of the key is told apart by its exchange id, and a group that was updated
     * and loaded again by its aggregated size.
     */
    private void checkUnchanged(CamelContext camelContext, String key, ReadGroup read) {
        EntryView<String, DefaultExchangeHolder> entry = entryView(key);
        Exchange stored = entry != null && entry.getVersion() == read.version()
                ? unmarshallExchange(camelContext, entry.getValue()) : null;
        if (stored == null || !read.exchangeId().equals(stored.getExchangeId())
                || !Objects.equals(read.size(), aggregatedSize(stored))) {
            LOG.error("Optimistic locking failed for exchange with key {}: the group was changed or removed since it was read",
                    key);
            throw new OptimisticLockingException();
        }
    }

    /**
     * Checks if the key in question is in the repository.
     *
     * @param key Object - key in question
     */
    public boolean containsKey(Object key) {
        if (cache != null) {
            return cache.containsKey(key);
        } else {
            return false;
        }
    }

    public boolean isAllowSerializedHeaders() {
        return allowSerializedHeaders;
    }

    public void setAllowSerializedHeaders(boolean allowSerializedHeaders) {
        this.allowSerializedHeaders = allowSerializedHeaders;
    }

    public long getOptimisticLockTimeout() {
        return optimisticLockTimeout;
    }

    /**
     * With optimistic locking, how long (in millis) adding or removing a group waits for the lock of the correlation
     * key, which another aggregator holds while it checks and writes the group (10 seconds by default). When the lock
     * is not acquired in time, the update fails with an {@link OptimisticLockingException}, and the Aggregate EIP tries
     * again as its optimistic lock retry policy says.
     */
    public void setOptimisticLockTimeout(long optimisticLockTimeout) {
        this.optimisticLockTimeout = optimisticLockTimeout;
    }

    public HazelcastInstance getHazelcastInstance() {
        return hazelcastInstance;
    }

    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        this.hazelcastInstance = hazelcastInstance;
    }

    /**
     * Removes the {@code exchange} from the operational storage and moves it into the persistent one if the
     * {@link HazelcastAggregationRepository} runs in recoverable mode. Both operations are applied atomically inside a
     * Hazelcast transaction (ONE_PHASE). When {@code optimistic} is {@code true}, the group is removed only if it is
     * still the group that was read: for an exchange that {@link #get} returned for the key, the key is locked and the
     * version of the entry is checked (see {@link #get}); for any other exchange,
     * {@link com.hazelcast.transaction.TransactionalMap#remove(Object, Object)} is used as the conditional
     * compare-and-delete. Either way the losing thread fails before it touches the persistent map. When
     * {@code optimistic} is {@code false} and {@code useRecovery} is {@code true}, the same transactional pattern is
     * used without the conditional check.
     *
     * @param camelContext the current CamelContext
     * @param key          the correlation key
     * @param exchange     the exchange to remove
     */
    @Override
    public void remove(CamelContext camelContext, String key, Exchange exchange) {
        DefaultExchangeHolder holder = DefaultExchangeHolder.marshal(exchange, true, allowSerializedHeaders);
        if (optimistic) {
            ReadGroup read = readGroup(key, exchange);
            if (read != null) {
                lock(key);
                try {
                    checkUnchanged(camelContext, key, read);
                    removeOptimistic(key, exchange, holder, null);
                } finally {
                    cache.unlock(key);
                }
            } else {
                removeOptimistic(key, exchange, holder, holder);
            }
        } else {
            if (useRecovery) {
                LOG.trace("Removing an exchange with ID {} for key {} in a thread-safe manner.", exchange.getExchangeId(), key);
                // The only considerable case for transaction usage is fault tolerance:
                // the transaction will be rolled back automatically (default timeout is 2 minutes)
                // if no commit occurs during the timeout. So we are still consistent whether local node crashes.
                TransactionOptions tOpts = new TransactionOptions();

                tOpts.setTransactionType(TransactionOptions.TransactionType.ONE_PHASE);
                TransactionContext tCtx = hazelcastInstance.newTransactionContext(tOpts);

                try {
                    tCtx.beginTransaction();

                    TransactionalMap<String, DefaultExchangeHolder> tCache = tCtx.getMap(cache.getName());
                    TransactionalMap<String, DefaultExchangeHolder> tPersistentCache = tCtx.getMap(persistedCache.getName());

                    tCache.remove(key);
                    LOG.trace("Putting an exchange with ID {} for key {} into a recoverable storage in a thread-safe manner.",
                            exchange.getExchangeId(), key);
                    // store the given exchange and not the removed entry: when a group is completed by an incoming
                    // exchange, the last aggregated exchange is not added to the repository before it is removed
                    tPersistentCache.put(exchange.getExchangeId(), holder);

                    tCtx.commitTransaction();
                    LOG.trace("Removed an exchange with ID {} for key {} in a thread-safe manner.", exchange.getExchangeId(),
                            key);
                    LOG.trace("Put an exchange with ID {} for key {} into a recoverable storage in a thread-safe manner.",
                            exchange.getExchangeId(), key);
                } catch (Exception exception) {
                    tCtx.rollbackTransaction();

                    final String msg = String.format(
                            "Transaction with ID %s was rolled back for remove operation with a key %s and an Exchange ID %s.",
                            tCtx.getTxnId(), key, exchange.getExchangeId());
                    LOG.warn(msg, exception);
                    throw new RuntimeCamelException(msg, exception);
                }
            } else {
                cache.remove(key);
            }
        }
    }

    /**
     * Removes the group in optimistic mode: with {@code expected == null}, the caller holds the lock of the key and
     * checked the group, so the entry is removed as it is; else only if it is equal to {@code expected} in serialized
     * form.
     */
    private void removeOptimistic(
            String key, Exchange exchange, DefaultExchangeHolder holder, DefaultExchangeHolder expected) {
        LOG.trace("Removing an exchange with ID {} for key {} in an optimistic manner.", exchange.getExchangeId(), key);
        if (useRecovery) {
            // Use a Hazelcast transaction to atomically apply the conditional remove and the recovery write.
            // A plain two-step approach (in either order) has two failure modes:
            //   1. put-then-remove (the pre-fix order): the two steps cannot be fenced: a concurrent thread
            //      holds the same exchangeId (it read the same stored entry), so it would delete the winning
            //      thread's recovery entry during its own cleanup on the compare-and-remove failure (CAMEL-26796).
            //   2. remove-then-put: if the JVM stops or persistedCache.put() throws after a successful
            //      cache.remove(), the group is in neither map (CAMEL-26663).
            // TransactionalMap.remove(key, value) is a compare-and-delete that enrolls the entry in the
            // transaction: the entry is gone or holds a different value from the loser's perspective, so
            // its remove returns false, and it never reaches the recovery write. With the key locked and the
            // group checked, the loser fails the check before it starts the transaction.
            TransactionOptions tOpts = new TransactionOptions();
            tOpts.setTransactionType(TransactionOptions.TransactionType.ONE_PHASE);
            TransactionContext tCtx = hazelcastInstance.newTransactionContext(tOpts);
            try {
                tCtx.beginTransaction();

                TransactionalMap<String, DefaultExchangeHolder> tCache = tCtx.getMap(cache.getName());
                TransactionalMap<String, DefaultExchangeHolder> tPersistentCache = tCtx.getMap(persistedCache.getName());

                if (expected == null) {
                    tCache.delete(key);
                } else if (!tCache.remove(key, expected)) {
                    tCtx.rollbackTransaction();
                    LOG.error(
                            "Optimistic locking failed for exchange with key {}: TransactionalMap#remove returned false (compare-and-delete found no matching entry).",
                            key);
                    throw new OptimisticLockingException();
                }
                LOG.trace("Putting an exchange with ID {} for key {} into a recoverable storage in an optimistic manner.",
                        exchange.getExchangeId(), key);
                // store the given exchange and not the removed entry: when a group is completed by an incoming
                // exchange, the last aggregated exchange is not added to the repository before it is removed
                tPersistentCache.put(exchange.getExchangeId(), holder);

                tCtx.commitTransaction();
                LOG.trace("Removed an exchange with ID {} for key {} in an optimistic manner.", exchange.getExchangeId(),
                        key);
                LOG.trace("Put an exchange with ID {} for key {} into a recoverable storage in an optimistic manner.",
                        exchange.getExchangeId(), key);
            } catch (OptimisticLockingException e) {
                throw e;
            } catch (Exception exception) {
                tCtx.rollbackTransaction();

                final String msg = String.format(
                        "Transaction with ID %s was rolled back for optimistic remove operation with a key %s and an Exchange ID %s.",
                        tCtx.getTxnId(), key, exchange.getExchangeId());
                LOG.warn(msg, exception);
                throw new RuntimeCamelException(msg, exception);
            }
        } else if (expected == null) {
            cache.delete(key);
            LOG.trace("Removed an exchange with ID {} for key {} in an optimistic manner.", exchange.getExchangeId(), key);
        } else {
            if (!cache.remove(key, expected)) {
                LOG.error(
                        "Optimistic locking failed for exchange with key {}: IMap#remove returned false (compare-and-delete found no matching entry).",
                        key);
                throw new OptimisticLockingException();
            }
            LOG.trace("Removed an exchange with ID {} for key {} in an optimistic manner.", exchange.getExchangeId(), key);
        }
    }

    @Override
    public void confirm(CamelContext camelContext, String exchangeId) {
        LOG.trace("Confirming an exchange with ID {}.", exchangeId);
        if (useRecovery) {
            persistedCache.remove(exchangeId);
        }
    }

    @Override
    public Set<String> getKeys() {
        return Collections.unmodifiableSet(cache.keySet());
    }

    /**
     * Whether the aggregates, and the completed exchanges kept for recovery, are kept only in memory, decided when
     * asked as the cluster changes; see {@link HazelcastUtil#isInMemory(HazelcastInstance, String)}.
     */
    @Override
    public boolean isInMemory() {
        return cache != null && HazelcastUtil.isInMemory(hazelcastInstance, mapName)
                && (!useRecovery || HazelcastUtil.isInMemory(hazelcastInstance, persistenceMapName));
    }

    /**
     * @return Persistent repository {@link IMap} name;
     */
    public String getPersistentRepositoryName() {
        return persistenceMapName;
    }

    @Override
    protected void doStart() throws Exception {
        StringHelper.notEmpty(mapName, "repositoryName");
        if (maximumRedeliveries < 0) {
            throw new IllegalArgumentException("Maximum redelivery retries must be zero or a positive integer.");
        }
        if (recoveryInterval < 0) {
            throw new IllegalArgumentException("Recovery interval must be zero or a positive integer.");
        }
        if (optimisticLockTimeout < 0) {
            throw new IllegalArgumentException("Optimistic lock timeout must be zero or a positive integer.");
        }
        if (persistenceMapName == null) {
            persistenceMapName = String.format("%s%s", mapName, COMPLETED_SUFFIX);
        }
        if (hazelcastInstance == null) {
            useLocalHzInstance = true;
            Config cfg = new XmlConfigBuilder().build();
            cfg.setProperty("hazelcast.version.check.enabled", "false");
            HazelcastSerializationFilterHelper.applyDefault(cfg);
            hazelcastInstance = Hazelcast.newHazelcastInstance(cfg);
        } else {
            ObjectHelper.notNull(hazelcastInstance, "hazelcastInstance");
        }
        cache = hazelcastInstance.getMap(mapName);
        if (useRecovery) {
            persistedCache = hazelcastInstance.getMap(persistenceMapName);
        }
    }

    @Override
    protected void doStop() throws Exception {
        readGroups.clear();
        if (useLocalHzInstance) {
            hazelcastInstance.getLifecycleService().shutdown();
            hazelcastInstance = null;
        }
    }

    protected Exchange unmarshallExchange(CamelContext camelContext, DefaultExchangeHolder holder) {
        Exchange exchange = null;
        if (holder != null) {
            exchange = new DefaultExchange(camelContext);
            DefaultExchangeHolder.unmarshal(exchange, holder);
        }
        return exchange;
    }
}
