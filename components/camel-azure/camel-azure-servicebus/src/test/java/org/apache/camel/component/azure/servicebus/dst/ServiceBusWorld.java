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
package org.apache.camel.component.azure.servicebus.dst;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import com.azure.messaging.servicebus.ServiceBusErrorContext;
import com.azure.messaging.servicebus.ServiceBusReceivedMessageContext;
import org.apache.camel.AsyncCallback;
import org.apache.camel.Exchange;
import org.apache.camel.ServiceStatus;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.azure.servicebus.ServiceBusComponent;
import org.apache.camel.component.azure.servicebus.ServiceBusConfiguration;
import org.apache.camel.component.azure.servicebus.ServiceBusConstants;
import org.apache.camel.component.azure.servicebus.ServiceBusEndpoint;
import org.apache.camel.component.azure.servicebus.client.ServiceBusClientFactory;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.AsyncProcessorSupport;
import org.apache.camel.test.dst.Dst;
import org.apache.camel.test.dst.DstCamel;
import org.apache.camel.test.dst.DstExecutor;
import org.apache.camel.test.dst.DstLedger;
import org.apache.camel.test.dst.DstWorld;
import org.apache.camel.test.dst.Invariants;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * The world around a camel-azure-servicebus consumer in peek-lock mode: a fake queue ({@link FakeServiceBus}), a
 * sender, a route with an asynchronous step (a downstream call) that is sometimes longer than the lock duration and
 * sometimes fails, and an operator that stops and starts the route while messages are processed. Faults: lock renewals
 * fail or take long, settlements fail.
 * <p>
 * The route is asynchronous, so processMessage returns at once and the lock is kept only by the lock renewer of the
 * consumer (maxAutoLockRenewDuration), which renews every message whose lock expires before the run after the next one
 * of its period task (every 10 seconds, or a second later). The seed picks the lock duration of the queue, the max
 * delivery count, maxConcurrentCalls (with the asynchronous route it bounds only the concurrent processMessage calls),
 * maxAutoLockRenewDuration (60 or 300 seconds), enableDeadLettering, the latency of the renewals and the fault rates.
 * <p>
 * Properties: no lock is lost, and no message is delivered again, while the consumer holds the message, unless an
 * injected renewal of its lock failed (the consumer stops renewing a message after a renewal error;
 * {@code -Ddst.servicebus.strictRenew=true} does not excuse it), the route was stopped while it was processed (once a
 * stop timed out the consumer stops renewing, and the exchange goes on), or maxAutoLockRenewDuration passed; the
 * message of a completed exchange is completed, unless its settlement failed, found its lock lost under one of those
 * excuses, or came after a stop closed the processor client while the exchange was in flight (the V2 processor of the
 * SDK closes its receiver on stop, see FakeServiceBus; the message is then delivered again; {@code
 * -Ddst.servicebus.strictClose=true} does not excuse it); a message is completed only after its exchange completed, and
 * abandoned or dead-lettered only after it failed; once the faults cleared every message is completed or dead-lettered;
 * the consumer settles each delivery at most once and renews only what it holds, through an open client.
 * <p>
 * Not explored: sessions, receiveAndDelete mode, a processorClient given by the user (the consumer does not renew its
 * locks), a synchronous route (the SDK renews the lock while processMessage runs, which is not modelled), topics.
 */
@DstWorld(component = "azure-servicebus",
          fake = "FakeServiceBus: a queue in peek-lock mode (lock duration, delivery count, dead-letter queue) behind"
                 + " the processor and receiver clients of ServiceBusClientFactory",
          covers = {
                  "the lock renewer of the consumer (PEEK_LOCK, maxAutoLockRenewDuration) with an asynchronous route",
                  "processing that takes longer than the lock duration",
                  "complete, abandon and dead-letter, also after the lock was lost",
                  "maxConcurrentCalls of one to three (with the asynchronous route it bounds the processMessage calls)",
                  "route stop and start while messages are processed" },
          faults = {
                  "a lock renewal fails", "a lock renewal takes long", "a settlement fails", "processing fails",
                  "processing is slower than the lock duration" },
          invariants = {
                  "no lock is lost and no message is delivered again while the consumer holds it, unless a renewal"
                         + " failed, the route was stopped or maxAutoLockRenewDuration passed",
                  "the message of a completed exchange is completed, unless its settlement failed or was excused",
                  "a message is completed only after its exchange completed",
                  "a message is abandoned or dead-lettered only after its exchange failed",
                  "every message is completed or dead-lettered once the faults cleared",
                  "the consumer settles a delivery at most once and renews only what it holds, through an open client" },
          issues = { "CAMEL-25701" })
final class ServiceBusWorld {

    static final String ROUTE = "servicebus";
    /** Excuses a lost lock only when the renewal found it lost (not after an injected transient renewal failure). */
    static final boolean STRICT_RENEW = Boolean.getBoolean("dst.servicebus.strictRenew");
    /** Does not excuse the settlement of an exchange that completed after a stop closed the processor client. */
    static final boolean STRICT_CLOSE = Boolean.getBoolean("dst.servicebus.strictClose");

    private final Dst dst;
    private final DstLedger ledger;
    private final FakeServiceBus bus;
    private final int maxConcurrentCalls;
    private final boolean deadLettering;
    private final double processFailRate;
    private final long maxAutoLockRenewDuration;
    private final List<String> violations = new ArrayList<>();
    private final List<String> lostWhileHeld = new ArrayList<>();
    /** The time each delivery was made. */
    private final Map<String, Long> deliveredAt = new HashMap<>();
    /**
     * The outcome of the settlement of each delivery (COMPLETED, ABANDONED, DEAD_LETTERED, fault, lockLost, closed).
     */
    private final Map<String, String> outcomes = new HashMap<>();
    private final List<String> notCompleted = new ArrayList<>();
    private final List<String> redeliveredWhileHeld = new ArrayList<>();
    private final List<String> wrongSettlements = new ArrayList<>();
    /** The deliveries ("id#n") whose exchange is in progress, by message id. */
    private final Map<String, String> held = new HashMap<>();
    private final Set<String> renewFaulted = new HashSet<>();
    /** The deliveries in progress when the route was stopped: the consumer stops renewing their locks. */
    private final Set<String> heldAtStop = new HashSet<>();
    private final Set<String> completed = new HashSet<>();
    private final Set<String> failed = new HashSet<>();
    private boolean faults = true;
    /** Exchanges whose asynchronous step has not answered yet. */
    private int inFlight;
    private int sendsLeft;
    private int opsLeft;
    private boolean finished;
    private String finalStatus;
    private boolean finalSettled;

    ServiceBusWorld(Dst dst) {
        this.dst = dst;
        this.ledger = new DstLedger(dst);
        this.bus = new FakeServiceBus(dst, ledger);
        bus.lockDurationMillis = dst.pick(List.of(30_000L, 60_000L));
        bus.maxDeliveryCount = dst.pick(List.of(3, 10));
        bus.renewFailRate = dst.pick(List.of(0.0, 0.05, 0.2));
        bus.settleFailRate = dst.pick(List.of(0.0, 0.05));
        bus.renewLatencyMaxMillis = dst.pick(List.of(0L, 2000L));
        this.maxAutoLockRenewDuration = dst.pick(List.of(60_000L, 300_000L));
        this.maxConcurrentCalls = dst.pick(List.of(1, 2, 3));
        this.deadLettering = dst.chance(0.3);
        this.processFailRate = dst.pick(List.of(0.0, 0.05, 0.2));
        bus.onDelivery = this::delivered;
        bus.onRenewFault = renewFaulted::add;
        bus.onSettle = this::settling;
        bus.onSettled = this::settled;
        bus.onLockExpired = this::lockExpired;
        // what is in flight when the processor client stops: the stop happens inside the route stop, after the
        // operator took its own snapshot, and a message can be delivered in between
        bus.onProcessorClosed = () -> heldAtStop.addAll(held.values());
        dst.trace("world: " + config());
    }

    private String config() {
        return "lockDuration=" + bus.lockDurationMillis + " maxDeliveryCount=" + bus.maxDeliveryCount
               + " maxConcurrentCalls=" + maxConcurrentCalls + " maxAutoLockRenewDuration=" + maxAutoLockRenewDuration
               + " enableDeadLettering=" + deadLettering + " renewFail=" + bus.renewFailRate + " renewLatencyMax="
               + bus.renewLatencyMaxMillis + " settleFail=" + bus.settleFailRate + " processFail=" + processFailRate;
    }

    @SuppressWarnings("unchecked")
    void run() throws Exception {
        DefaultCamelContext context = DstCamel.newContext(dst);
        ServiceBusClientFactory factory = mock(ServiceBusClientFactory.class);
        when(factory.createServiceBusProcessorClient(any(ServiceBusConfiguration.class), any(Consumer.class),
                any(Consumer.class))).thenAnswer(
                        i -> bus.newProcessorClient(
                                i.<ServiceBusConfiguration> getArgument(0).getMaxConcurrentCalls(),
                                i.<Consumer<ServiceBusReceivedMessageContext>> getArgument(1),
                                i.<Consumer<ServiceBusErrorContext>> getArgument(2)));
        when(factory.createServiceBusReceiverAsyncClient(any(ServiceBusConfiguration.class)))
                .thenAnswer(i -> bus.newRenewalClient());
        context.addComponent("azure-servicebus", new ServiceBusComponent());
        String uri = "azure-servicebus:" + FakeServiceBus.QUEUE
                     + "?connectionString=RAW(Endpoint=sb://dst.servicebus.windows.net/;SharedAccessKeyName=dst;"
                     + "SharedAccessKey=dst)&serviceBusReceiveMode=PEEK_LOCK&maxConcurrentCalls=" + maxConcurrentCalls
                     + "&enableDeadLettering=" + deadLettering + "&maxAutoLockRenewDuration=" + maxAutoLockRenewDuration;
        DstExecutor downstream = dst.executor("downstream", Integer.MAX_VALUE);
        context.addRoutes(new RouteBuilder() {
            @Override
            public void configure() {
                // the injected processing failures are expected: no error handler log for each of them
                errorHandler(defaultErrorHandler().logExhausted(false));
                from(uri).routeId(ROUTE).autoStartup(false)
                        .process(new AsyncProcessorSupport() {
                            @Override
                            public boolean process(Exchange exchange, AsyncCallback callback) {
                                return downstream(exchange, callback, downstream);
                            }
                        });
            }
        });
        ((ServiceBusEndpoint) context.getEndpoint(uri)).setServiceBusClientFactory(factory);
        context.start();
        DstExecutor operator = dst.executor("operator", 1);
        try {
            operator.schedule("start route", 0, () -> startRoute(context));
            planSends();
            planOperations(context, operator);
            operator.schedule("finisher", 1000, () -> finisher(context, operator, -1));
            dst.runUntil(() -> finished, "the finisher");
            dst.check(
                    Invariants.of("the consumer settles a delivery at most once and renews only what it holds, through an"
                                  + " open client",
                            () -> bus.violations.isEmpty() && violations.isEmpty()
                                    ? null : bus.violations + " " + violations + " (" + config() + ")"),
                    Invariants.of("no lock is lost and no message is delivered again while the consumer holds it,"
                                  + " unless a renewal failed, the route was stopped or maxAutoLockRenewDuration passed",
                            () -> redeliveredWhileHeld.isEmpty() && lostWhileHeld.isEmpty()
                                    ? null : redeliveredWhileHeld + " " + lostWhileHeld + " (" + config() + ")"),
                    Invariants.of("the message of a completed exchange is completed, unless its settlement failed or"
                                  + " was excused as above",
                            () -> notCompleted.isEmpty() ? null : notCompleted + " (" + config() + ")"),
                    Invariants.of("a message is completed only after its exchange completed",
                            () -> wrongSettlements.stream().noneMatch(s -> s.contains("COMPLETED"))
                                    ? null : wrongSettlements + " (" + config() + ")"),
                    Invariants.of("a message is abandoned or dead-lettered only after its exchange failed",
                            () -> wrongSettlements.stream().allMatch(s -> s.contains("COMPLETED"))
                                    ? null : wrongSettlements + " (" + config() + ")"),
                    Invariants.of("every message is completed or dead-lettered once the faults cleared",
                            () -> finalSettled && "Started".equals(finalStatus)
                                    ? null
                                    : "route " + finalStatus + ", unsettled " + bus.unsettled() + " (" + config() + ")"));
        } finally {
            dst.dropPending();
            // with an exchange still in flight (its answer was dropped) the shutdown would wait for it for ever on the
            // one stack
            if (inFlight == 0) {
                context.stop();
            }
        }
    }

    /** The asynchronous step of the route: a downstream call that answers after a latency, and may fail. */
    private boolean downstream(Exchange exchange, AsyncCallback callback, DstExecutor downstream) {
        String id = exchange.getMessage().getHeader(ServiceBusConstants.MESSAGE_ID, String.class);
        Long count = exchange.getMessage().getHeader(ServiceBusConstants.DELIVERY_COUNT, Long.class);
        String delivery = id + "#" + count;
        ledger.record("routed", "q", delivery);
        long latency = faults && dst.chance(0.2)
                ? dst.between(bus.lockDurationMillis / 2, 3 * bus.lockDurationMillis)
                : dst.between(10, 500);
        boolean fail = faults && dst.chance(processFailRate);
        inFlight++;
        downstream.schedule("answer for " + delivery, latency, () -> {
            inFlight--;
            if (fail) {
                failed.add(delivery);
                ledger.record("failed", "q", delivery);
                exchange.setException(Dst.injected(new IllegalStateException("dst: processing failed for " + delivery)));
            } else {
                completed.add(delivery);
                ledger.record("completed", "q", delivery);
            }
            held.remove(id, delivery);
            callback.done(false);
        });
        return false;
    }

    /** The queue delivered a message: from now on the consumer holds it, until its exchange is done. */
    private void delivered(String delivery) {
        String id = delivery.substring(0, delivery.indexOf('#'));
        String previous = held.get(id);
        if (previous != null && !excused(previous)) {
            redeliveredWhileHeld.add(delivery + " at t=" + dst.now() + " while " + previous + " is processed");
        }
        held.put(id, delivery);
        deliveredAt.put(delivery, dst.now());
    }

    /**
     * Why the consumer may lose the lock of a delivery it holds: an injected renewal failure (the consumer stops
     * renewing that message after a renewal error), a route stop while it was processed (the consumer stops renewing
     * once the stop timed out), or a hold longer than maxAutoLockRenewDuration.
     */
    private boolean excused(String delivery) {
        Long at = deliveredAt.get(delivery);
        return !STRICT_RENEW && renewFaulted.contains(delivery) || heldAtStop.contains(delivery)
                || at != null && dst.now() >= at + maxAutoLockRenewDuration;
    }

    /** The queue let the lock of a delivery expire: the consumer must renew the locks of what it holds. */
    private void lockExpired(String delivery) {
        String id = delivery.substring(0, delivery.indexOf('#'));
        if (delivery.equals(held.get(id)) && !excused(delivery)) {
            lostWhileHeld.add("the lock of " + delivery + " expired at t=" + dst.now() + " while it is processed");
        }
    }

    private void settled(String outcome) {
        int space = outcome.indexOf(' ');
        String delivery = outcome.substring(0, space);
        String result = outcome.substring(space + 1);
        // whether a lost lock is excused is decided when the settlement finds it lost
        outcomes.put(delivery, "lockLost".equals(result) && excused(delivery) ? "lockLost (excused)" : result);
    }

    /** A completed exchange must complete its message, unless the settlement failed for an excused reason. */
    private void checkCompletions() {
        for (String delivery : completed.stream().sorted().toList()) {
            String outcome = outcomes.get(delivery);
            boolean ok = "COMPLETED".equals(outcome) || "fault".equals(outcome)
                    || "lockLost (excused)".equals(outcome)
                    // a settlement after the close is one of an exchange that was in flight when the client stopped
                    || "closed".equals(outcome) && !STRICT_CLOSE;
            if (!ok) {
                notCompleted.add(delivery + " completed but its settlement was " + outcome);
            }
        }
    }

    private void settling(String settlement) {
        String delivery = settlement.substring(0, settlement.indexOf(' '));
        boolean complete = settlement.endsWith("COMPLETED");
        if (complete && !completed.contains(delivery) || !complete && !failed.contains(delivery)) {
            wrongSettlements.add(settlement + " at t=" + dst.now());
        }
    }

    private void finisher(DefaultCamelContext context, DstExecutor operator, long deadline) {
        if (deadline == -1 && (sendsLeft > 0 || opsLeft > 0)) {
            operator.schedule("finisher", 500, () -> finisher(context, operator, -1));
            return;
        }
        if (deadline == -1) {
            long slow = dst.between(0, 10_000);
            dst.trace("world: operations done, faults for another " + slow + " ms");
            operator.schedule("finisher", slow, () -> finisher(context, operator, -2));
            return;
        }
        if (deadline == -2) {
            faults = false;
            bus.faults = false;
            dst.trace("world: faults cleared");
            long d = dst.now() + 60 * 60_000L;
            operator.schedule("finisher", 500, () -> finisher(context, operator, d));
            return;
        }
        ServiceStatus status = context.getRouteController().getRouteStatus(ROUTE);
        boolean settled = bus.settled();
        if ((!settled || inFlight > 0 || status != ServiceStatus.Started) && dst.now() < deadline) {
            operator.schedule("finisher", 500, () -> finisher(context, operator, deadline));
            return;
        }
        finalStatus = String.valueOf(status);
        finalSettled = settled;
        if (inFlight > 0) {
            violations.add(inFlight + " exchanges still in flight at the deadline");
        }
        checkCompletions();
        finished = true;
    }

    private void startRoute(DefaultCamelContext context) {
        try {
            context.getRouteController().startRoute(ROUTE);
            ledger.record("routeStarted", "r", ROUTE);
        } catch (Exception e) {
            violations.add("start failed: " + e);
        }
    }

    private void planSends() {
        DstExecutor sender = dst.executor("sender", 1);
        long t = dst.between(0, 500);
        int n = 0;
        int bursts = (int) dst.between(1, 4);
        for (int b = 0; b < bursts; b++) {
            int size = (int) dst.between(1, 12);
            for (int i = 0; i < size; i++) {
                String id = "m" + n++;
                sendsLeft++;
                sender.schedule("send " + id, t, () -> {
                    bus.send(id);
                    sendsLeft--;
                });
                t += dst.between(0, 100);
            }
            t += dst.between(0, 30_000);
        }
    }

    /**
     * Stops the route while messages may be processed (the consumer stops receiving, the shutdown strategy waits for
     * the exchanges in flight, whose asynchronous answers are tasks of their own), then starts it again.
     */
    private void planOperations(DefaultCamelContext context, DstExecutor operator) {
        int ops = (int) dst.between(0, 3);
        long t = dst.between(500, 5000);
        for (int i = 0; i < ops; i++) {
            opsLeft++;
            operator.schedule("stop route", t, () -> {
                if (context.getRouteController().getRouteStatus(ROUTE) == ServiceStatus.Started) {
                    heldAtStop.addAll(held.values());
                    try {
                        context.getRouteController().stopRoute(ROUTE);
                        ledger.record("routeStopped", "r", ROUTE);
                    } catch (Exception e) {
                        violations.add("stop failed: " + e);
                    }
                }
                operator.schedule("start route", dst.between(100, 10_000), () -> {
                    startRoute(context);
                    opsLeft--;
                });
            });
            t += dst.between(5000, 60_000);
        }
    }
}
