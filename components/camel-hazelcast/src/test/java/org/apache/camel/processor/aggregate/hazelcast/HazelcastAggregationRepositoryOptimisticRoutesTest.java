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

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serial;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.IntBinaryOperator;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.cluster.Address;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.map.MapStore;
import org.apache.camel.AggregationStrategy;
import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.RuntimeCamelException;
import org.apache.camel.builder.AggregationStrategies;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.model.AggregateDefinition;
import org.apache.camel.processor.aggregate.GroupedBodyAggregationStrategy;
import org.apache.camel.processor.aggregate.OptimisticLockRetryPolicy;
import org.apache.camel.spi.OptimisticLockingAggregationRepository.OptimisticLockingException;
import org.apache.camel.support.DefaultExchange;
import org.apache.camel.support.DefaultExchangeHolder;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Runs {@link HazelcastAggregationRepository} with {@code optimistic=true} through the Aggregate EIP. The Aggregate EIP
 * changes the group it read from the repository before it removes it (it sets the correlation key and, on a timeout,
 * how the group was completed), and an aggregation strategy may change the group in place before it is stored again,
 * also an object that the group holds, such as a list, so the repository must check that the stored group is still the
 * one it read, not compare it with the exchange it is given. It must not depend on the serialized form of the group
 * either: a {@code HashSet} is written differently after it was read, and a member in another JVM may write a value
 * differently. The maps whose name starts with {@link #STORED_MAPS} have a map store, from which Hazelcast loads an
 * evicted entry with its version at 0 again. The last test is a control that passes without the change too.
 */
class HazelcastAggregationRepositoryOptimisticRoutesTest extends HazelcastAggregationRepositoryCamelTestSupport {

    private static final int GROUPS = 5;
    private static final int PER_GROUP = 4;
    private static final String STORED_MAPS = "optimisticStored";

    enum Strategy {
        // changes the old exchange and returns it
        IN_PLACE,
        // returns the new exchange with the sum
        NEW_EXCHANGE,
        // returns a copy of the old exchange with the sum
        COPY;

        AggregationStrategy create() {
            return switch (this) {
                case IN_PLACE -> new SumOfIntsAggregationStrategy();
                case NEW_EXCHANGE -> (oldExchange, newExchange) -> {
                    if (oldExchange != null) {
                        newExchange.getIn().setBody(sum(oldExchange, newExchange));
                    }
                    return newExchange;
                };
                case COPY -> (oldExchange, newExchange) -> {
                    if (oldExchange == null) {
                        return newExchange;
                    }
                    Exchange copy = oldExchange.copy();
                    copy.getIn().setBody(sum(oldExchange, newExchange));
                    return copy;
                };
            };
        }

        private static int sum(Exchange oldExchange, Exchange newExchange) {
            return oldExchange.getIn().getBody(Integer.class) + newExchange.getIn().getBody(Integer.class);
        }
    }

    enum ListStrategy {
        // Camel's own: adds each body to a list in an exchange property of the old exchange
        GROUPED_BODY,
        // adds each body to the list that is the body of the old exchange
        LIST_BODY;

        AggregationStrategy create() {
            return switch (this) {
                case GROUPED_BODY -> new GroupedBodyAggregationStrategy();
                case LIST_BODY -> (oldExchange, newExchange) -> {
                    if (oldExchange == null) {
                        List<Object> list = new ArrayList<>();
                        list.add(newExchange.getIn().getBody());
                        newExchange.getIn().setBody(list);
                        return newExchange;
                    }
                    oldExchange.getIn().getBody(List.class).add(newExchange.getIn().getBody());
                    return oldExchange;
                };
            };
        }
    }

    @ParameterizedTest
    @CsvSource({ "IN_PLACE,false", "IN_PLACE,true", "NEW_EXCHANGE,false", "NEW_EXCHANGE,true", "COPY,false", "COPY,true" })
    void completionSizeCompletesEveryGroup(Strategy strategy, boolean useRecovery) throws Exception {
        String name = "optimisticSize" + strategy + useRecovery;
        HazelcastAggregationRepository repository = repository(name, getFirstInstance(), useRecovery);
        CamelContext camel = startRoute(repository, strategy, aggregate -> aggregate.completionSize(PER_GROUP));
        try {
            MockEndpoint mock = expectEveryGroupOnce(camel);
            ProducerTemplate template = camel.createProducerTemplate();
            // the groups are interleaved, so each group is stored and updated before it completes
            for (int i = 0; i < PER_GROUP; i++) {
                for (int g = 0; g < GROUPS; g++) {
                    send(template, g + 1, "g" + g);
                }
            }
            mock.assertIsSatisfied();
            assertTrue(repository.getKeys().isEmpty(), "groups left in the repository: " + repository.getKeys());
        } finally {
            camel.stop();
        }
    }

    @ParameterizedTest
    @CsvSource({ "IN_PLACE,1", "IN_PLACE,3", "NEW_EXCHANGE,1", "NEW_EXCHANGE,3", "COPY,1", "COPY,3" })
    void completionTimeoutCompletesEveryGroup(Strategy strategy, int perGroup) throws Exception {
        String name = "optimisticTimeout" + strategy + perGroup;
        HazelcastAggregationRepository repository = repository(name, getFirstInstance(), false);
        CamelContext camel = startRoute(repository, strategy,
                aggregate -> aggregate.completionTimeout(200).completionTimeoutCheckerInterval(50));
        try {
            MockEndpoint mock = camel.getEndpoint("mock:result", MockEndpoint.class);
            List<Integer> expected = new ArrayList<>();
            for (int g = 0; g < GROUPS; g++) {
                expected.add((g + 1) * perGroup);
            }
            mock.expectedBodiesReceivedInAnyOrder(expected);
            ProducerTemplate template = camel.createProducerTemplate();
            for (int i = 0; i < perGroup; i++) {
                for (int g = 0; g < GROUPS; g++) {
                    send(template, g + 1, "g" + g);
                }
            }
            mock.assertIsSatisfied();
            assertTrue(repository.getKeys().isEmpty(), "groups left in the repository: " + repository.getKeys());
        } finally {
            camel.stop();
        }
    }

    @ParameterizedTest
    @CsvSource({ "IN_PLACE", "NEW_EXCHANGE", "COPY" })
    void twoMembersAggregatingTheSameKeysCompleteEveryGroup(Strategy strategy) throws Exception {
        String name = "optimisticTwoMembers" + strategy;
        // both members aggregate every group: half of the messages of each group go through each member
        CamelContext one = startRoute(repository(name, getFirstInstance(), true), strategy,
                aggregate -> aggregate.completionSize(PER_GROUP * 2));
        CamelContext two = startRoute(repository(name, getSecondInstance(), true), strategy,
                aggregate -> aggregate.completionSize(PER_GROUP * 2));
        try {
            MockEndpoint mockOne = one.getEndpoint("mock:result", MockEndpoint.class);
            MockEndpoint mockTwo = two.getEndpoint("mock:result", MockEndpoint.class);

            sendConcurrently(one, two);

            await().atMost(20, TimeUnit.SECONDS)
                    .until(() -> mockOne.getReceivedCounter() + mockTwo.getReceivedCounter() >= GROUPS);
            List<Integer> bodies = new ArrayList<>();
            mockOne.getReceivedExchanges().forEach(e -> bodies.add(e.getIn().getBody(Integer.class)));
            mockTwo.getReceivedExchanges().forEach(e -> bodies.add(e.getIn().getBody(Integer.class)));
            bodies.sort(null);
            assertEquals(expectedSums(PER_GROUP * 2), bodies);
            assertTrue(getFirstInstance().getMap(name).isEmpty(), "groups left in the repository");
        } finally {
            one.stop();
            two.stop();
        }
    }

    @Test
    void twoMembersCompletingByTimeoutDeliverEveryMessageOnce() throws Exception {
        String name = "optimisticTwoMembersTimeout";
        CamelContext one = startRoute(repository(name, getFirstInstance(), true), Strategy.IN_PLACE,
                aggregate -> aggregate.completionTimeout(300).completionTimeoutCheckerInterval(50));
        CamelContext two = startRoute(repository(name, getSecondInstance(), true), Strategy.IN_PLACE,
                aggregate -> aggregate.completionTimeout(300).completionTimeoutCheckerInterval(50));
        try {
            MockEndpoint mockOne = one.getEndpoint("mock:result", MockEndpoint.class);
            MockEndpoint mockTwo = two.getEndpoint("mock:result", MockEndpoint.class);

            sendConcurrently(one, two);

            // a group may be completed in more than one part (a timeout between two messages), but every message
            // is in exactly one aggregate, and no group is left behind
            int total = expectedSums(PER_GROUP * 2).stream().mapToInt(Integer::intValue).sum();
            await().atMost(20, TimeUnit.SECONDS)
                    .until(() -> sum(mockOne) + sum(mockTwo) >= total && getFirstInstance().getMap(name).isEmpty());
            // and no message is delivered twice, also not by a late completion
            await().during(500, TimeUnit.MILLISECONDS).atMost(2, TimeUnit.SECONDS)
                    .until(() -> sum(mockOne) + sum(mockTwo) == total);
            assertTrue(getFirstInstance().getMap(name).isEmpty(), "groups left in the repository");
        } finally {
            one.stop();
            two.stop();
        }
    }

    @ParameterizedTest
    @CsvSource({ "GROUPED_BODY,false", "GROUPED_BODY,true", "LIST_BODY,false", "LIST_BODY,true" })
    void listStrategiesCompleteEveryGroup(ListStrategy strategy, boolean useRecovery) throws Exception {
        String name = "optimisticList" + strategy + useRecovery;
        HazelcastAggregationRepository repository = repository(name, getFirstInstance(), useRecovery);
        // the list of GroupedBodyAggregationStrategy is an exchange property, stored only with serialized values
        repository.setAllowSerializedHeaders(true);
        CamelContext camel = startRoute(repository, strategy.create(), aggregate -> aggregate.completionSize(PER_GROUP));
        try {
            MockEndpoint mock = camel.getEndpoint("mock:result", MockEndpoint.class);
            mock.expectedMessageCount(GROUPS);
            mock.setAssertPeriod(200);
            ProducerTemplate template = camel.createProducerTemplate();
            for (int i = 0; i < PER_GROUP; i++) {
                for (int g = 0; g < GROUPS; g++) {
                    send(template, listBody(i, g), "g" + g);
                }
            }
            mock.assertIsSatisfied();
            assertEquals(expectedLists(1), receivedLists(mock));
            assertTrue(repository.getKeys().isEmpty(), "groups left in the repository: " + repository.getKeys());
        } finally {
            camel.stop();
        }
    }

    @ParameterizedTest
    @CsvSource({ "GROUPED_BODY", "LIST_BODY" })
    void twoMembersWithAListStrategyLoseNoMessage(ListStrategy strategy) throws Exception {
        String name = "optimisticTwoMembersList" + strategy;
        HazelcastAggregationRepository first = repository(name, getFirstInstance(), true);
        HazelcastAggregationRepository second = repository(name, getSecondInstance(), true);
        first.setAllowSerializedHeaders(true);
        second.setAllowSerializedHeaders(true);
        CamelContext one = startRoute(first, strategy.create(), aggregate -> aggregate.completionSize(PER_GROUP * 2));
        CamelContext two = startRoute(second, strategy.create(), aggregate -> aggregate.completionSize(PER_GROUP * 2));
        try {
            MockEndpoint mockOne = one.getEndpoint("mock:result", MockEndpoint.class);
            MockEndpoint mockTwo = two.getEndpoint("mock:result", MockEndpoint.class);

            sendConcurrently(one, two, HazelcastAggregationRepositoryOptimisticRoutesTest::listBody);

            await().atMost(20, TimeUnit.SECONDS)
                    .until(() -> mockOne.getReceivedCounter() + mockTwo.getReceivedCounter() >= GROUPS);
            // every group has the messages of both members: a list changed in place loses no update of the other member
            List<List<Integer>> lists = receivedLists(mockOne);
            lists.addAll(receivedLists(mockTwo));
            lists.sort(Comparator.comparing(list -> list.get(0)));
            assertEquals(expectedLists(2), lists);
            assertTrue(getFirstInstance().getMap(name).isEmpty(), "groups left in the repository");
        } finally {
            one.stop();
            two.stop();
        }
    }

    @Test
    void groupChangedByAnotherAggregatorIsNotOverwritten() {
        HazelcastAggregationRepository repository = repository("optimisticStale", getFirstInstance(), false);
        repository.start();
        try {
            CamelContext camel = context();
            repository.add(camel, "k", null, exchange(camel, 1));

            Exchange stale = repository.get(camel, "k");
            Exchange current = repository.get(camel, "k");
            repository.add(camel, "k", current, exchange(camel, 2));

            // the group read first was changed meanwhile: neither replacing nor removing it may succeed
            stale.getIn().setBody(3);
            assertThrows(OptimisticLockingException.class, () -> repository.add(camel, "k", stale, stale));
            Exchange staleAgain = repository.get(camel, "k");
            repository.add(camel, "k", repository.get(camel, "k"), exchange(camel, 4));
            assertThrows(OptimisticLockingException.class, () -> repository.remove(camel, "k", staleAgain));

            assertEquals(4, repository.get(camel, "k").getIn().getBody(Integer.class));
        } finally {
            repository.stop();
        }
    }

    @Test
    void staleReadIsNotTakenForALaterReadOfTheSameKey() {
        HazelcastAggregationRepository repository = repository("optimisticStaleLater", getFirstInstance(), false);
        repository.start();
        try {
            CamelContext camel = context();
            repository.add(camel, "k", null, exchange(camel, 1));

            Exchange staleForAdd = repository.get(camel, "k");
            Exchange staleForRemove = repository.get(camel, "k");
            repository.add(camel, "k", repository.get(camel, "k"), exchange(camel, 2));

            // another aggregator reads the newer group after it was written: the stale reads must not use that read
            Exchange later = repository.get(camel, "k");
            assertThrows(OptimisticLockingException.class,
                    () -> repository.add(camel, "k", staleForAdd, exchange(camel, 3)));
            Exchange laterAgain = repository.get(camel, "k");
            assertThrows(OptimisticLockingException.class, () -> repository.remove(camel, "k", staleForRemove));
            assertEquals(2, repository.get(camel, "k").getIn().getBody(Integer.class));

            // the later reads still update and remove the group
            repository.add(camel, "k", later, exchange(camel, 4));
            assertThrows(OptimisticLockingException.class, () -> repository.remove(camel, "k", laterAgain));
            repository.remove(camel, "k", repository.get(camel, "k"));
            assertNull(repository.get(camel, "k"));
        } finally {
            repository.stop();
        }
    }

    @Test
    void staleReadChangedInPlaceDoesNotOverwriteANewerGroup() {
        HazelcastAggregationRepository repository = repository("optimisticStaleInPlace", getFirstInstance(), false);
        repository.start();
        try {
            CamelContext camel = context();
            repository.add(camel, "k", null, listExchange(camel, 1));

            Exchange staleForAdd = repository.get(camel, "k");
            Exchange staleForRemove = repository.get(camel, "k");
            Exchange current = repository.get(camel, "k");
            current.getIn().getBody(List.class).add(2);
            repository.add(camel, "k", current, current);

            // the stale reads add to their list in place: the stored group is not the group as they read it, so
            // neither replacing nor removing it may succeed
            staleForAdd.getIn().getBody(List.class).add(3);
            assertThrows(OptimisticLockingException.class, () -> repository.add(camel, "k", staleForAdd, staleForAdd));
            staleForRemove.getIn().getBody(List.class).add(4);
            assertThrows(OptimisticLockingException.class, () -> repository.remove(camel, "k", staleForRemove));

            assertEquals(List.of(1, 2), repository.get(camel, "k").getIn().getBody(List.class));
        } finally {
            repository.stop();
        }
    }

    @Test
    void readGroupIsComparedOnlyForItsOwnKey() {
        HazelcastAggregationRepository repository = repository("optimisticOtherKey", getFirstInstance(), false);
        repository.start();
        try {
            CamelContext camel = context();
            Exchange first = exchange(camel, 1);
            repository.add(camel, "a", null, first);
            repository.add(camel, "b", null, first);

            // the same group is stored under both keys, but what was read for "a" does not stand for "b": the
            // exchange as it is now (changed) is compared for "b"
            Exchange readForA = repository.get(camel, "a");
            readForA.getIn().setBody(5);
            assertThrows(OptimisticLockingException.class, () -> repository.remove(camel, "b", readForA));
            assertEquals(1, repository.get(camel, "b").getIn().getBody(Integer.class));
        } finally {
            repository.stop();
        }
    }

    @Test
    void staleReadOfACompletedGroupDoesNotOverwriteANewGroupOfTheKey() {
        HazelcastAggregationRepository repository = repository("optimisticStaleNewGroup", getFirstInstance(), false);
        repository.start();
        try {
            CamelContext camel = context();
            repository.add(camel, "k", null, exchange(camel, 1));
            Exchange staleForAdd = repository.get(camel, "k");
            Exchange staleForRemove = repository.get(camel, "k");

            // the group is completed, and a new group of the key is stored: Hazelcast starts its version again
            repository.remove(camel, "k", repository.get(camel, "k"));
            repository.add(camel, "k", null, exchange(camel, 2));

            assertThrows(OptimisticLockingException.class,
                    () -> repository.add(camel, "k", staleForAdd, exchange(camel, 3)));
            assertThrows(OptimisticLockingException.class, () -> repository.remove(camel, "k", staleForRemove));
            assertEquals(2, repository.get(camel, "k").getIn().getBody(Integer.class));
        } finally {
            repository.stop();
        }
    }

    @ParameterizedTest
    @CsvSource({ "add,false", "remove,false", "remove,true" })
    void anotherMemberCannotWriteBetweenTheCheckAndTheWrite(String operation, boolean useRecovery) throws Exception {
        String name = "optimisticWindow" + operation + useRecovery;
        HazelcastAggregationRepository first = repository(name, getFirstInstance(), useRecovery);
        HazelcastAggregationRepository second = repository(name, getSecondInstance(), useRecovery);
        first.start();
        second.start();
        IMap<String, DefaultExchangeHolder> map = first.cache;
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            CamelContext camel = context();
            first.add(camel, "k", null, exchange(camel, 1));
            Exchange read = first.get(camel, "k");
            Exchange readBySecond = second.get(camel, "k");

            // right after the first member checked the stored group, the second member updates the group it read:
            // it waits for the first member to write
            AtomicReference<Future<Exchange>> secondWrite = new AtomicReference<>();
            first.cache = afterNextGetEntryView(map, () -> {
                Future<Exchange> write = executor.submit(() -> second.add(camel, "k", readBySecond, exchange(camel, 99)));
                secondWrite.set(write);
                assertThrows(TimeoutException.class, () -> write.get(500, TimeUnit.MILLISECONDS));
            });
            if ("add".equals(operation)) {
                first.add(camel, "k", read, exchange(camel, 2));
            } else {
                first.remove(camel, "k", read);
            }
            first.cache = map;

            // then it finds the group changed or removed, so its update fails and the first write stays
            ExecutionException failure = assertThrows(ExecutionException.class,
                    () -> secondWrite.get().get(10, TimeUnit.SECONDS));
            assertInstanceOf(OptimisticLockingException.class, failure.getCause());
            Exchange stored = first.get(camel, "k");
            if ("add".equals(operation)) {
                assertEquals(2, stored.getIn().getBody(Integer.class));
            } else {
                assertNull(stored);
            }
        } finally {
            first.cache = map;
            executor.shutdownNow();
            first.stop();
            second.stop();
        }
    }

    @ParameterizedTest
    @ValueSource(classes = { HashSet.class, LinkedHashSet.class })
    @SuppressWarnings("rawtypes")
    void setAccumulatedInCollectionCompletes(Class<? extends Collection> type) throws Exception {
        // a HashSet of fewer than 7 elements is written with another capacity after it was read, so it serializes
        // differently from the stored one
        HazelcastAggregationRepository repository = repository("optimisticSet" + type.getSimpleName(), getFirstInstance(),
                false);
        CamelContext camel = startRoute(repository, AggregationStrategies.flexible().accumulateInCollection(type),
                aggregate -> aggregate.completionSize(3));
        try {
            MockEndpoint mock = camel.getEndpoint("mock:result", MockEndpoint.class);
            mock.expectedMessageCount(1);
            mock.setAssertPeriod(200);
            ProducerTemplate template = camel.createProducerTemplate();
            for (String body : List.of("a", "b", "c")) {
                template.sendBodyAndHeader("direct:in", body, "id", "g");
            }
            mock.assertIsSatisfied();
            assertEquals(Set.of("a", "b", "c"),
                    Set.copyOf(mock.getReceivedExchanges().get(0).getIn().getBody(Collection.class)));
            assertTrue(repository.getKeys().isEmpty(), "groups left in the repository: " + repository.getKeys());
        } finally {
            camel.stop();
        }
    }

    @Test
    void setBodyCompletedByTimeoutIsSent() throws Exception {
        HazelcastAggregationRepository repository = repository("optimisticSetTimeout", getFirstInstance(), false);
        CamelContext camel
                = startRoute(repository, (oldExchange, newExchange) -> oldExchange != null ? oldExchange : newExchange,
                        aggregate -> aggregate.completionTimeout(200).completionTimeoutCheckerInterval(50));
        try {
            MockEndpoint mock = camel.getEndpoint("mock:result", MockEndpoint.class);
            mock.expectedBodiesReceived(new HashSet<>(List.of("x", "y")));
            camel.createProducerTemplate().sendBodyAndHeader("direct:in", new HashSet<>(List.of("x", "y")), "id", "t");
            mock.assertIsSatisfied();
            assertTrue(repository.getKeys().isEmpty(), "groups left in the repository: " + repository.getKeys());
        } finally {
            camel.stop();
        }
    }

    @ParameterizedTest
    @CsvSource({ "size,false", "size,true", "timeout,false" })
    void valueThatSerializesDifferentlyEachTimeCompletes(String completion, boolean useRecovery) throws Exception {
        HazelcastAggregationRepository repository = repository("optimisticChangingForm" + completion + useRecovery,
                getFirstInstance(), useRecovery);
        CamelContext camel = startRoute(repository, new ChangingFormSumStrategy(),
                "size".equals(completion)
                        ? aggregate -> aggregate.completionSize(PER_GROUP)
                        : aggregate -> aggregate.completionTimeout(300).completionTimeoutCheckerInterval(50));
        try {
            MockEndpoint mock = camel.getEndpoint("mock:result", MockEndpoint.class);
            mock.expectedMessageCount(GROUPS);
            mock.setAssertPeriod(200);
            ProducerTemplate template = camel.createProducerTemplate();
            for (int i = 0; i < PER_GROUP; i++) {
                for (int g = 0; g < GROUPS; g++) {
                    send(template, g + 1, "g" + g);
                }
            }
            mock.assertIsSatisfied();
            List<Integer> sums = new ArrayList<>();
            mock.getReceivedExchanges().forEach(e -> sums.add(e.getIn().getBody(ChangingForm.class).sum));
            sums.sort(null);
            assertEquals(expectedSums(PER_GROUP), sums);
            assertTrue(repository.getKeys().isEmpty(), "groups left in the repository: " + repository.getKeys());
        } finally {
            camel.stop();
        }
    }

    @Test
    void twoMembersWithAValueThatSerializesDifferentlyEachTimeLoseNoMessage() throws Exception {
        String name = "optimisticTwoMembersChangingForm";
        CamelContext one = startRoute(repository(name, getFirstInstance(), true), new ChangingFormSumStrategy(),
                aggregate -> aggregate.completionSize(PER_GROUP * 2));
        CamelContext two = startRoute(repository(name, getSecondInstance(), true), new ChangingFormSumStrategy(),
                aggregate -> aggregate.completionSize(PER_GROUP * 2));
        try {
            MockEndpoint mockOne = one.getEndpoint("mock:result", MockEndpoint.class);
            MockEndpoint mockTwo = two.getEndpoint("mock:result", MockEndpoint.class);

            sendConcurrently(one, two);

            await().atMost(20, TimeUnit.SECONDS)
                    .until(() -> mockOne.getReceivedCounter() + mockTwo.getReceivedCounter() >= GROUPS);
            List<Integer> sums = new ArrayList<>();
            mockOne.getReceivedExchanges().forEach(e -> sums.add(e.getIn().getBody(ChangingForm.class).sum));
            mockTwo.getReceivedExchanges().forEach(e -> sums.add(e.getIn().getBody(ChangingForm.class).sum));
            sums.sort(null);
            assertEquals(expectedSums(PER_GROUP * 2), sums);
            assertTrue(getFirstInstance().getMap(name).isEmpty(), "groups left in the repository");
        } finally {
            one.stop();
            two.stop();
        }
    }

    @Test
    void groupStoredBeforeTheUpgradeIsCompleted() throws Exception {
        String name = "optimisticStoredBefore";
        // a group stored and updated by an earlier version, which wrote the holder of the group to the map
        CamelContext camel = context();
        Exchange stored = exchange(camel, 1);
        stored.getIn().setHeader("id", "g");
        stored.setProperty(Exchange.AGGREGATED_SIZE, 1);
        IMap<String, DefaultExchangeHolder> map = getFirstInstance().getMap(name);
        map.put("g", DefaultExchangeHolder.marshal(stored, true, false));
        map.put("g", DefaultExchangeHolder.marshal(stored, true, false));

        HazelcastAggregationRepository repository = repository(name, getFirstInstance(), false);
        CamelContext route = startRoute(repository, Strategy.IN_PLACE, aggregate -> aggregate.completionSize(3));
        try {
            MockEndpoint mock = route.getEndpoint("mock:result", MockEndpoint.class);
            mock.expectedBodiesReceived(111);
            ProducerTemplate template = route.createProducerTemplate();
            send(template, 10, "g");
            send(template, 100, "g");
            mock.assertIsSatisfied();
            assertTrue(map.isEmpty(), "groups left in the repository");
        } finally {
            route.stop();
        }
    }

    @BeforeAll
    void addMapStoreToStoredMaps() {
        // a dynamic configuration, for the maps of both members
        getFirstInstance().getConfig().addMapConfig(new MapConfig(STORED_MAPS + "*")
                .setMapStoreConfig(new MapStoreConfig().setEnabled(true).setClassName(GroupStore.class.getName())));
    }

    @ParameterizedTest
    @ValueSource(strings = { "add", "remove" })
    void staleReadDoesNotOverwriteAGroupLoadedAgainFromAMapStore(String operation) {
        String name = STORED_MAPS + "Stale" + operation;
        String key = name + "-k";
        GroupStore.removeKeys(name);
        HazelcastAggregationRepository first = repository(name, getFirstInstance(), false);
        HazelcastAggregationRepository second = repository(name, getSecondInstance(), false);
        first.start();
        second.start();
        try {
            CamelContext camel = context();
            IMap<String, DefaultExchangeHolder> map = first.cache;
            first.add(camel, key, null, sizedExchange(camel, 1, 1));
            // the group is only in the map store, as after an eviction: get loads it, with the version at 0
            map.evict(key);
            Exchange stale = first.get(camel, key);
            assertEquals(0, map.getEntryView(key).getVersion());

            // the other member adds a message to the group in place, as the Aggregate EIP does, then the group is
            // evicted again
            Exchange current = second.get(camel, key);
            current.getIn().setBody(3);
            current.setProperty(Exchange.AGGREGATED_SIZE, 2);
            second.add(camel, key, current, current);
            map.evict(key);

            // the check loads the group again, with the version at 0 as when the stale read was made, and the same
            // exchange id, but the group has a larger aggregated size: neither replacing nor removing it may succeed
            stale.getIn().setBody(11);
            stale.setProperty(Exchange.AGGREGATED_SIZE, 2);
            if ("add".equals(operation)) {
                assertThrows(OptimisticLockingException.class, () -> first.add(camel, key, stale, stale));
            } else {
                assertThrows(OptimisticLockingException.class, () -> first.remove(camel, key, stale));
            }
            assertEquals(0, map.getEntryView(key).getVersion());
            assertEquals(3, first.get(camel, key).getIn().getBody(Integer.class));

            // a new read removes the group, also from the map store
            first.remove(camel, key, first.get(camel, key));
            assertNull(first.get(camel, key));
            assertFalse(GroupStore.DATA.containsKey(key));
        } finally {
            first.stop();
            second.stop();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = { false, true })
    void groupsEvictedToAMapStoreBetweenMessagesComplete(boolean useRecovery) throws Exception {
        String name = STORED_MAPS + "Route" + useRecovery;
        GroupStore.removeKeys(name);
        HazelcastAggregationRepository repository = repository(name, getFirstInstance(), useRecovery);
        CamelContext camel = startRoute(repository, Strategy.IN_PLACE, aggregate -> aggregate.completionSize(PER_GROUP));
        try {
            MockEndpoint mock = expectEveryGroupOnce(camel);
            ProducerTemplate template = camel.createProducerTemplate();
            IMap<String, DefaultExchangeHolder> map = getFirstInstance().getMap(name);
            // every group is only in the map store when its next message comes
            for (int i = 0; i < PER_GROUP; i++) {
                for (int g = 0; g < GROUPS; g++) {
                    send(template, g + 1, name + "-g" + g);
                }
                map.evictAll();
            }
            mock.assertIsSatisfied();
            for (int g = 0; g < GROUPS; g++) {
                assertFalse(GroupStore.DATA.containsKey(name + "-g" + g), "group left in the map store");
            }
            assertTrue(repository.getKeys().isEmpty(), "groups left in the repository: " + repository.getKeys());
        } finally {
            camel.stop();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "add", "remove" })
    void updateFailsWhenTheLockOfTheKeyIsNotAcquiredInTime(String operation) throws Exception {
        String name = "optimisticLockTimeout" + operation;
        HazelcastAggregationRepository repository = repository(name, getFirstInstance(), false);
        repository.setOptimisticLockTimeout(200);
        repository.start();
        IMap<String, DefaultExchangeHolder> otherMember = getSecondInstance().getMap(name);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            CamelContext camel = context();
            repository.add(camel, "k", null, exchange(camel, 1));
            Exchange read = repository.get(camel, "k");

            // another member holds the lock of the key, as an aggregator that hangs while it writes the group
            otherMember.lock("k");
            try {
                Future<Object> update = executor.submit(() -> {
                    if ("add".equals(operation)) {
                        repository.add(camel, "k", read, exchange(camel, 2));
                    } else {
                        repository.remove(camel, "k", read);
                    }
                    return null;
                });
                // the update fails when the lock timeout is over, so that the Aggregate EIP tries again
                ExecutionException failure
                        = assertThrows(ExecutionException.class, () -> update.get(10, TimeUnit.SECONDS));
                assertInstanceOf(OptimisticLockingException.class, failure.getCause());
            } finally {
                otherMember.unlock("k");
            }

            // the group is not changed, and a new read updates it
            assertEquals(1, repository.get(camel, "k").getIn().getBody(Integer.class));
            repository.add(camel, "k", repository.get(camel, "k"), exchange(camel, 2));
            assertEquals(2, repository.get(camel, "k").getIn().getBody(Integer.class));
        } finally {
            executor.shutdownNow();
            repository.stop();
        }
    }

    @Test
    void aggregateTriesAgainWhenTheLockOfTheKeyIsNotAcquiredInTime() throws Exception {
        String name = "optimisticLockTimeoutRoute";
        HazelcastAggregationRepository repository = repository(name, getFirstInstance(), false);
        repository.setOptimisticLockTimeout(100);
        AtomicInteger failures = new AtomicInteger();
        AggregationStrategy strategy = new SumOfIntsAggregationStrategy() {
            @Override
            public void onOptimisticLockFailure(Exchange oldExchange, Exchange newExchange) {
                failures.incrementAndGet();
            }
        };
        CamelContext camel = startRoute(repository, strategy, aggregate -> aggregate.completionSize(3));
        IMap<String, DefaultExchangeHolder> otherMember = getSecondInstance().getMap(name);
        try {
            MockEndpoint mock = camel.getEndpoint("mock:result", MockEndpoint.class);
            mock.expectedBodiesReceived(6);
            ProducerTemplate template = camel.createProducerTemplate();
            send(template, 1, "g");

            // another member holds the lock of the key: the second message fails to update the group and is tried
            // again until the lock is released
            CompletableFuture<Object> second;
            otherMember.lock("g");
            try {
                second = template.asyncRequestBodyAndHeader("direct:in", 2, "id", "g");
                await().atMost(10, TimeUnit.SECONDS).until(() -> failures.get() >= 1);
            } finally {
                otherMember.unlock("g");
            }
            second.get(10, TimeUnit.SECONDS);
            send(template, 3, "g");
            mock.assertIsSatisfied();
            assertTrue(repository.getKeys().isEmpty(), "groups left in the repository: " + repository.getKeys());
        } finally {
            camel.stop();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "client", "member" })
    void interruptedWhileWaitingForTheLockLeavesTheKeyUnlocked(String mode) throws Exception {
        String name = "optimisticInterruptWait" + mode;
        HazelcastInstance client = "client".equals(mode) ? newClient() : null;
        HazelcastAggregationRepository repository
                = repository(name, client != null ? client : getFirstInstance(), false);
        repository.start();
        IMap<String, DefaultExchangeHolder> map = repository.cache;
        IMap<String, DefaultExchangeHolder> otherMember = getSecondInstance().getMap(name);
        Thread updater = null;
        try {
            CamelContext camel = context();
            repository.add(camel, "k", null, exchange(camel, 1));
            Exchange read = repository.get(camel, "k");

            // another member holds the lock of the key, and the update is interrupted while it waits for the lock
            // (the lock timeout is the default of 10 seconds)
            otherMember.lock("k");
            CountDownLatch tried = new CountDownLatch(1);
            repository.cache = before("tryLock", map, tried::countDown);
            AtomicReference<Throwable> failure = new AtomicReference<>();
            AtomicBoolean interruptKept = new AtomicBoolean();
            updater = new Thread(() -> {
                try {
                    repository.add(camel, "k", read, exchange(camel, 2));
                } catch (Throwable e) {
                    failure.set(e);
                }
                interruptKept.set(Thread.currentThread().isInterrupted());
            });
            updater.start();
            assertTrue(tried.await(10, TimeUnit.SECONDS));
            updater.interrupt();
            updater.join(5000);
            assertFalse(updater.isAlive(), "the update still waits for the lock after it was interrupted");
            assertInstanceOf(RuntimeCamelException.class, failure.get());
            assertTrue(interruptKept.get(), "the interrupt flag is not kept");
            repository.cache = map;

            // when the other member unlocks the key, nobody holds its lock: the interrupted update left no request
            // for it, which a client would otherwise get granted after it stopped waiting
            otherMember.unlock("k");
            assertKeyCanBeUpdated(name, repository, camel);
        } finally {
            repository.cache = map;
            if (updater != null) {
                updater.join(15000);
            }
            otherMember.forceUnlock("k");
            repository.stop();
            if (client != null) {
                client.shutdown();
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "client", "member" })
    void interruptedWhileTryingTheLockLeavesTheKeyUnlocked(String mode) throws Exception {
        String name = "optimisticInterruptTry" + mode;
        HazelcastInstance client = "client".equals(mode) ? newClient() : null;
        HazelcastAggregationRepository repository
                = repository(name, client != null ? client : getFirstInstance(), false);
        repository.start();
        IMap<String, DefaultExchangeHolder> map = repository.cache;
        try {
            CamelContext camel = context();
            repository.add(camel, "k", null, exchange(camel, 1));
            Exchange read = repository.get(camel, "k");

            // the thread is interrupted during a try to lock the free key: a client has sent the try when it stops
            // waiting for the answer, and the member grants it; a member grants it to the interrupted thread
            AtomicBoolean interrupt = new AtomicBoolean(true);
            repository.cache = before("tryLock", map, () -> {
                if (interrupt.getAndSet(false)) {
                    Thread.currentThread().interrupt();
                }
            });
            assertThrows(RuntimeCamelException.class, () -> repository.add(camel, "k", read, exchange(camel, 2)));
            assertTrue(Thread.interrupted(), "the interrupt flag is not kept");
            repository.cache = map;

            // the lock was released, and the group is not changed
            assertKeyCanBeUpdated(name, repository, camel);
        } finally {
            Thread.interrupted();
            repository.cache = map;
            getSecondInstance().getMap(name).forceUnlock("k");
            repository.stop();
            if (client != null) {
                client.shutdown();
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "client", "member" })
    void interruptedWhileUnlockingKeepsTheUpdate(String mode) throws Exception {
        String name = "optimisticInterruptUnlock" + mode;
        HazelcastInstance client = "client".equals(mode) ? newClient() : null;
        HazelcastAggregationRepository repository
                = repository(name, client != null ? client : getFirstInstance(), false);
        repository.start();
        IMap<String, DefaultExchangeHolder> map = repository.cache;
        try {
            CamelContext camel = context();
            repository.add(camel, "k", null, exchange(camel, 1));
            Exchange read = repository.get(camel, "k");

            // the thread is interrupted during the unlock after the write: a client has sent the unlock when it stops
            // waiting for the answer, and the member does it
            AtomicBoolean interrupt = new AtomicBoolean(true);
            repository.cache = before("unlock", map, () -> {
                if (interrupt.getAndSet(false)) {
                    Thread.currentThread().interrupt();
                }
            });
            repository.add(camel, "k", read, exchange(camel, 2));
            assertTrue(Thread.interrupted(), "the interrupt flag is not kept");
            repository.cache = map;

            // the update is written, and the key is unlocked
            IMap<String, DefaultExchangeHolder> otherMember = getSecondInstance().getMap(name);
            assertTrue(otherMember.tryLock("k", 5, TimeUnit.SECONDS), "the lock of the key is held");
            otherMember.unlock("k");
            assertEquals(2, repository.get(camel, "k").getIn().getBody(Integer.class));
        } finally {
            Thread.interrupted();
            repository.cache = map;
            getSecondInstance().getMap(name).forceUnlock("k");
            repository.stop();
            if (client != null) {
                client.shutdown();
            }
        }
    }

    // the group of key k has the body 1, its lock is free, and other members and the repository can update it
    private static void assertKeyCanBeUpdated(String name, HazelcastAggregationRepository repository, CamelContext camel)
            throws Exception {
        IMap<String, DefaultExchangeHolder> otherMember = getSecondInstance().getMap(name);
        assertTrue(otherMember.tryLock("k", 5, TimeUnit.SECONDS), "the lock of the key is held");
        otherMember.unlock("k");
        HazelcastAggregationRepository second = repository(name, getSecondInstance(), false);
        second.setOptimisticLockTimeout(1000);
        second.start();
        try {
            assertEquals(1, second.get(camel, "k").getIn().getBody(Integer.class));
            second.add(camel, "k", second.get(camel, "k"), exchange(camel, 3));
        } finally {
            second.stop();
        }
        repository.add(camel, "k", repository.get(camel, "k"), exchange(camel, 4));
        assertEquals(4, repository.get(camel, "k").getIn().getBody(Integer.class));
    }

    // a Hazelcast client of the module's cluster
    private static HazelcastInstance newClient() {
        Address address = getFirstInstance().getCluster().getLocalMember().getAddress();
        ClientConfig config = new ClientConfig();
        config.setClusterName(getFirstInstance().getConfig().getClusterName());
        config.getNetworkConfig().addAddress(address.getHost() + ":" + address.getPort());
        config.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(30000);
        return HazelcastClient.newHazelcastClient(config);
    }

    @Test
    void threadSafeRepositoryCompletesEveryGroup() throws Exception {
        HazelcastAggregationRepository repository = new HazelcastAggregationRepository(
                "threadSafeControl", false, getFirstInstance());
        CamelContext camel = new DefaultCamelContext();
        camel.addRoutes(new RouteBuilder() {
            @Override
            public void configure() {
                from("direct:in")
                        .aggregate(header("id"), new SumOfIntsAggregationStrategy())
                        .aggregationRepository(repository)
                        .completionSize(PER_GROUP)
                        .to("mock:result");
            }
        });
        camel.start();
        try {
            MockEndpoint mock = expectEveryGroupOnce(camel);
            ProducerTemplate template = camel.createProducerTemplate();
            for (int i = 0; i < PER_GROUP; i++) {
                for (int g = 0; g < GROUPS; g++) {
                    send(template, g + 1, "g" + g);
                }
            }
            mock.assertIsSatisfied();
            assertTrue(repository.getKeys().isEmpty());
        } finally {
            camel.stop();
        }
    }

    private static HazelcastAggregationRepository repository(
            String name, HazelcastInstance instance, boolean useRecovery) {
        HazelcastAggregationRepository repository = new HazelcastAggregationRepository(name, true, instance);
        repository.setUseRecovery(useRecovery);
        return repository;
    }

    private static CamelContext startRoute(
            HazelcastAggregationRepository repository, Strategy strategy,
            Consumer<AggregateDefinition> completion)
            throws Exception {
        return startRoute(repository, strategy.create(), completion);
    }

    private static CamelContext startRoute(
            HazelcastAggregationRepository repository, AggregationStrategy strategy,
            Consumer<AggregateDefinition> completion)
            throws Exception {
        CamelContext camel = new DefaultCamelContext();
        camel.addRoutes(new RouteBuilder() {
            @Override
            public void configure() {
                AggregateDefinition aggregate = from("direct:in")
                        .aggregate(header("id"), strategy)
                        .aggregationRepository(repository)
                        .optimisticLocking()
                        // bounded, so that a group that can never be updated fails the message instead of retrying
                        .optimisticLockRetryPolicy(new OptimisticLockRetryPolicy()
                                .maximumRetries(50).retryDelay(1).maximumRetryDelay(20).exponentialBackOff()
                                .randomBackOff());
                completion.accept(aggregate);
                aggregate.to("mock:result");
            }
        });
        camel.start();
        return camel;
    }

    private static MockEndpoint expectEveryGroupOnce(CamelContext camel) {
        MockEndpoint mock = camel.getEndpoint("mock:result", MockEndpoint.class);
        mock.expectedBodiesReceivedInAnyOrder(expectedSums(PER_GROUP));
        mock.setAssertPeriod(200);
        return mock;
    }

    private static List<Integer> expectedSums(int perGroup) {
        List<Integer> sums = new ArrayList<>();
        for (int g = 0; g < GROUPS; g++) {
            sums.add((g + 1) * perGroup);
        }
        return sums;
    }

    private static void sendConcurrently(CamelContext one, CamelContext two) throws Exception {
        sendConcurrently(one, two, (i, g) -> g + 1);
    }

    // body(i, g) is the body of the i-th message of group g
    private static void sendConcurrently(CamelContext one, CamelContext two, IntBinaryOperator body) throws Exception {
        CyclicBarrier barrier = new CyclicBarrier(2);
        AtomicReference<Exception> failure = new AtomicReference<>();
        List<Thread> senders = new ArrayList<>();
        for (CamelContext camel : List.of(one, two)) {
            Thread sender = new Thread(() -> {
                try (ProducerTemplate template = camel.createProducerTemplate()) {
                    barrier.await(10, TimeUnit.SECONDS);
                    for (int i = 0; i < PER_GROUP; i++) {
                        for (int g = 0; g < GROUPS; g++) {
                            send(template, body.applyAsInt(i, g), "g" + g);
                        }
                    }
                } catch (Exception e) {
                    failure.compareAndSet(null, e);
                }
            });
            senders.add(sender);
            sender.start();
        }
        for (Thread sender : senders) {
            sender.join(TimeUnit.SECONDS.toMillis(30));
            assertFalse(sender.isAlive());
        }
        assertNull(failure.get());
    }

    private static void send(ProducerTemplate template, int body, String group) {
        // several headers, not in the order of their names, and a header whose value is not a String
        Map<String, Object> headers = new LinkedHashMap<>();
        headers.put("zeta", "last");
        headers.put("id", group);
        headers.put("Alpha", body);
        headers.put("beta", "middle");
        template.sendBodyAndHeaders("direct:in", body, headers);
    }

    private static int listBody(int i, int g) {
        return g * 100 + i;
    }

    // the list of each group, sorted by group, when each member sent every message of every group
    private static List<List<Integer>> expectedLists(int members) {
        List<List<Integer>> lists = new ArrayList<>();
        for (int g = 0; g < GROUPS; g++) {
            List<Integer> list = new ArrayList<>();
            for (int i = 0; i < PER_GROUP; i++) {
                for (int m = 0; m < members; m++) {
                    list.add(listBody(i, g));
                }
            }
            lists.add(list);
        }
        return lists;
    }

    // the list body of each exchange received, sorted, by group
    private static List<List<Integer>> receivedLists(MockEndpoint mock) {
        List<List<Integer>> lists = new ArrayList<>();
        for (Exchange exchange : mock.getReceivedExchanges()) {
            List<Integer> list = new ArrayList<>();
            for (Object body : exchange.getIn().getBody(List.class)) {
                list.add((Integer) body);
            }
            list.sort(null);
            lists.add(list);
        }
        lists.sort(Comparator.comparing(list -> list.get(0)));
        return lists;
    }

    private static int sum(MockEndpoint mock) {
        return mock.getReceivedExchanges().stream().mapToInt(e -> e.getIn().getBody(Integer.class)).sum();
    }

    // the map, which runs the action before each call of the method
    @SuppressWarnings("unchecked")
    private static IMap<String, DefaultExchangeHolder> before(
            String methodName, IMap<String, DefaultExchangeHolder> map, Runnable action) {
        return (IMap<String, DefaultExchangeHolder>) Proxy.newProxyInstance(IMap.class.getClassLoader(),
                new Class<?>[] { IMap.class }, (proxy, method, args) -> {
                    if (methodName.equals(method.getName())) {
                        action.run();
                    }
                    try {
                        return method.invoke(map, args);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
    }

    // the map, which runs the action right after the next getEntryView
    @SuppressWarnings("unchecked")
    private static IMap<String, DefaultExchangeHolder> afterNextGetEntryView(
            IMap<String, DefaultExchangeHolder> map, Runnable action) {
        AtomicReference<Runnable> next = new AtomicReference<>(action);
        return (IMap<String, DefaultExchangeHolder>) Proxy.newProxyInstance(IMap.class.getClassLoader(),
                new Class<?>[] { IMap.class }, (proxy, method, args) -> {
                    Object result;
                    try {
                        result = method.invoke(map, args);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                    if ("getEntryView".equals(method.getName())) {
                        Runnable run = next.getAndSet(null);
                        if (run != null) {
                            run.run();
                        }
                    }
                    return result;
                });
    }

    /**
     * A value whose serialized form is different each time it is written, as a {@code HashSet} of enums is when it is
     * written by another JVM (the order of its elements follows their identity hash codes there).
     */
    static final class ChangingForm implements Serializable {
        @Serial
        private static final long serialVersionUID = 1L;
        private static final AtomicInteger WRITES = new AtomicInteger();

        int sum;

        ChangingForm(int sum) {
            this.sum = sum;
        }

        @Serial
        private void writeObject(ObjectOutputStream out) throws IOException {
            out.defaultWriteObject();
            out.writeInt(WRITES.incrementAndGet());
        }

        @Serial
        private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
            in.defaultReadObject();
            in.readInt();
        }
    }

    // adds the body of each message to the sum of the ChangingForm body of the group, in place
    private static final class ChangingFormSumStrategy implements AggregationStrategy {
        @Override
        public Exchange aggregate(Exchange oldExchange, Exchange newExchange) {
            int body = newExchange.getIn().getBody(Integer.class);
            if (oldExchange == null) {
                newExchange.getIn().setBody(new ChangingForm(body));
                return newExchange;
            }
            oldExchange.getIn().getBody(ChangingForm.class).sum += body;
            return oldExchange;
        }
    }

    private static Exchange listExchange(CamelContext camel, int element) {
        List<Integer> list = new ArrayList<>();
        list.add(element);
        Exchange exchange = new DefaultExchange(camel);
        exchange.getIn().setBody(list);
        return exchange;
    }

    private static Exchange exchange(CamelContext camel, int body) {
        Exchange exchange = new DefaultExchange(camel);
        exchange.getIn().setBody(body);
        return exchange;
    }

    // a group as the Aggregate EIP stores it, with its aggregated size
    private static Exchange sizedExchange(CamelContext camel, int body, int size) {
        Exchange exchange = exchange(camel, body);
        exchange.setProperty(Exchange.AGGREGATED_SIZE, size);
        return exchange;
    }

    /**
     * The map store of the maps whose name starts with {@link #STORED_MAPS}, in memory and shared by both members (the
     * keys of the tests differ).
     */
    public static final class GroupStore implements MapStore<String, DefaultExchangeHolder> {
        static final Map<String, DefaultExchangeHolder> DATA = new ConcurrentHashMap<>();

        public GroupStore() {
        }

        // the groups that an earlier run of the class left in the store
        static void removeKeys(String prefix) {
            DATA.keySet().removeIf(key -> key.startsWith(prefix));
        }

        @Override
        public void store(String key, DefaultExchangeHolder value) {
            DATA.put(key, value);
        }

        @Override
        public void storeAll(Map<String, DefaultExchangeHolder> map) {
            DATA.putAll(map);
        }

        @Override
        public void delete(String key) {
            DATA.remove(key);
        }

        @Override
        public void deleteAll(Collection<String> keys) {
            keys.forEach(DATA::remove);
        }

        @Override
        public DefaultExchangeHolder load(String key) {
            return DATA.get(key);
        }

        @Override
        public Map<String, DefaultExchangeHolder> loadAll(Collection<String> keys) {
            Map<String, DefaultExchangeHolder> loaded = new HashMap<>();
            for (String key : keys) {
                DefaultExchangeHolder value = DATA.get(key);
                if (value != null) {
                    loaded.put(key, value);
                }
            }
            return loaded;
        }

        @Override
        public Iterable<String> loadAllKeys() {
            // nothing is loaded when a map is created
            return null;
        }
    }
}
