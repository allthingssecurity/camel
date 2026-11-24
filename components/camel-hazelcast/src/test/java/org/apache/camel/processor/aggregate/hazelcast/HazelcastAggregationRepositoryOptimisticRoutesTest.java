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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.IntBinaryOperator;

import com.hazelcast.core.HazelcastInstance;
import org.apache.camel.AggregationStrategy;
import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.model.AggregateDefinition;
import org.apache.camel.processor.aggregate.GroupedBodyAggregationStrategy;
import org.apache.camel.processor.aggregate.OptimisticLockRetryPolicy;
import org.apache.camel.spi.OptimisticLockingAggregationRepository.OptimisticLockingException;
import org.apache.camel.support.DefaultExchange;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Runs {@link HazelcastAggregationRepository} with {@code optimistic=true} through the Aggregate EIP. The Aggregate EIP
 * changes the group it read from the repository before it removes it (it sets the correlation key and, on a timeout,
 * how the group was completed), and an aggregation strategy may change the group in place before it is stored again, so
 * the repository must compare the stored group with the group as it was read, not with the exchange it is given. A
 * strategy may also change an object that the group holds, such as a list, so the group as it was read must not change
 * with the exchange. The last test is a control that passes without the change too.
 */
class HazelcastAggregationRepositoryOptimisticRoutesTest extends HazelcastAggregationRepositoryCamelTestSupport {

    private static final int GROUPS = 5;
    private static final int PER_GROUP = 4;

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

            // the stale reads add to their list in place, so the stored group is read again: it is not the group as
            // they read it, so neither replacing nor removing it may succeed
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
}
