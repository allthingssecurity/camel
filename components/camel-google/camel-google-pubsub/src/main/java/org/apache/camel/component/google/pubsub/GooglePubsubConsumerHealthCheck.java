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
package org.apache.camel.component.google.pubsub;

import java.util.Map;

import org.apache.camel.health.HealthCheck;
import org.apache.camel.health.HealthCheckResultBuilder;

/**
 * Whether every subscriber loop of the consumer pulls. A loop that ended with a failure subscribes or pulls again after
 * the retry delay, and this check reports {@code DOWN} with that failure until it does, so a consumer that receives
 * nothing does not look healthy because its route is started.
 */
public class GooglePubsubConsumerHealthCheck implements HealthCheck {

    private final GooglePubsubConsumer consumer;
    private final String id;
    private boolean enabled = true;

    public GooglePubsubConsumerHealthCheck(GooglePubsubConsumer consumer, String routeId) {
        this.consumer = consumer;
        this.id = "consumer:" + routeId;
    }

    @Override
    public String getGroup() {
        return "camel";
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public boolean isEnabled() {
        return enabled;
    }

    @Override
    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    @Override
    public boolean isLiveness() {
        // a subscription that cannot be read for now (permissions, credentials) is no reason to restart the application
        return false;
    }

    @Override
    public Result call(Map<String, Object> options) {
        HealthCheckResultBuilder builder = HealthCheckResultBuilder.on(this)
                .detail(ENDPOINT_URI, consumer.getEndpoint().getEndpointBaseUri());
        Throwable failure = consumer.getSubscriberLoopFailure();
        if (failure == null) {
            return builder.up().build();
        }
        return builder.down()
                .message("A subscriber loop does not pull after a failure: " + failure.getMessage())
                .error(failure)
                .build();
    }
}
