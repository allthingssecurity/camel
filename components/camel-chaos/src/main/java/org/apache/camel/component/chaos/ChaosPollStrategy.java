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
package org.apache.camel.component.chaos;

import org.apache.camel.Consumer;
import org.apache.camel.Endpoint;
import org.apache.camel.RuntimeCamelException;
import org.apache.camel.component.chaos.ChaosRule.Hook;
import org.apache.camel.spi.PollingConsumerPollStrategy;
import org.apache.camel.support.DefaultPollingConsumerPollStrategy;
import org.apache.camel.util.TimeSource;

/**
 * Raises the faults of the rules at the polls of a scheduled poll consumer, around the poll strategy of the consumer:
 * {@code drop} skips the poll (the strategy of the consumer is not asked), {@code delay} makes the poll thread wait,
 * through {@link TimeSource}, before the poll, and {@code error} fails the poll after the strategy of the consumer
 * began it, so its {@code rollback} sees the failure as for a poll that failed by itself (an exception that is not a
 * {@link RuntimeException} is wrapped in a {@link RuntimeCamelException}). The first rule that fires decides the fault
 * of a poll.
 */
final class ChaosPollStrategy implements PollingConsumerPollStrategy {

    private final ChaosManager manager;
    private final PollingConsumerPollStrategy delegate;

    ChaosPollStrategy(ChaosManager manager, PollingConsumerPollStrategy delegate) {
        this.manager = manager;
        // a consumer that is not initialized yet has no strategy, and would use the default one
        this.delegate = delegate != null ? delegate : new DefaultPollingConsumerPollStrategy();
    }

    PollingConsumerPollStrategy getDelegate() {
        return delegate;
    }

    @Override
    public boolean begin(Consumer consumer, Endpoint endpoint) {
        ChaosRule[] rules = manager.ruleSet().poll;
        if (rules.length == 0) {
            return delegate.begin(consumer, endpoint);
        }
        long now = TimeSource.currentTimeMillis();
        for (ChaosRule rule : rules) {
            if (now < rule.endsAt && rule.matchesEndpoint(endpoint) && rule.fires(now)) {
                return fault(rule, consumer, endpoint);
            }
        }
        return delegate.begin(consumer, endpoint);
    }

    private boolean fault(ChaosRule rule, Consumer consumer, Endpoint endpoint) {
        switch (rule.fault) {
            case DROP -> {
                rule.fired(Hook.POLL);
                return false;
            }
            case DELAY -> {
                rule.fired(Hook.POLL);
                try {
                    TimeSource.sleep(rule.duration);
                } catch (InterruptedException e) {
                    // the consumer is stopping: skip the poll, and keep the interrupted status
                    Thread.currentThread().interrupt();
                    return false;
                }
                return delegate.begin(consumer, endpoint);
            }
            case ERROR -> {
                if (!delegate.begin(consumer, endpoint)) {
                    // the strategy of the consumer skips this poll anyway
                    rule.skipped();
                    return false;
                }
                rule.fired(Hook.POLL);
                Throwable failure = ChaosFaults.newException(rule);
                if (failure instanceof RuntimeException re) {
                    throw re;
                }
                // the strategy declares no checked exception: the consumer gets it wrapped, as its cause. An Error is
                // wrapped too, as the consumer handles only an Exception (it would stop polling for good otherwise)
                throw new RuntimeCamelException(failure);
            }
            default -> throw new IllegalStateException("The fault " + rule.fault + " does not fire at polls");
        }
    }

    @Override
    public void commit(Consumer consumer, Endpoint endpoint, int polledMessages) {
        delegate.commit(consumer, endpoint, polledMessages);
    }

    @Override
    public boolean rollback(Consumer consumer, Endpoint endpoint, int retryCounter, Exception cause) throws Exception {
        return delegate.rollback(consumer, endpoint, retryCounter, cause);
    }

    @Override
    public String toString() {
        return "ChaosPollStrategy[" + delegate + "]";
    }
}
