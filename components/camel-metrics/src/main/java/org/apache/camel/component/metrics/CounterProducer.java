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
package org.apache.camel.component.metrics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import org.apache.camel.Exchange;
import org.apache.camel.Message;

import static org.apache.camel.component.metrics.MetricsConstants.HEADER_COUNTER_DECREMENT;
import static org.apache.camel.component.metrics.MetricsConstants.HEADER_COUNTER_INCREMENT;

public class CounterProducer extends AbstractMetricsProducer {

    public CounterProducer(MetricsEndpoint endpoint) {
        super(endpoint);
    }

    @Override
    protected void doProcess(Exchange exchange, MetricsEndpoint endpoint, MetricRegistry registry, String metricsName)
            throws Exception {
        Message in = exchange.getIn();
        Counter counter = registry.counter(metricsName);
        Long increment = endpoint.getIncrement();
        Long decrement = endpoint.getDecrement();
        // a header wins over both options, so that a decrement header also applies when the endpoint sets increment
        Long headerIncrement = getLongHeader(in, HEADER_COUNTER_INCREMENT, null);
        Long headerDecrement = getLongHeader(in, HEADER_COUNTER_DECREMENT, null);
        if (headerIncrement != null) {
            counter.inc(headerIncrement);
        } else if (headerDecrement != null) {
            counter.dec(headerDecrement);
        } else if (increment != null) {
            counter.inc(increment);
        } else if (decrement != null) {
            counter.dec(decrement);
        } else {
            counter.inc();
        }
    }
}
