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

import com.codahale.metrics.MetricRegistry;
import org.apache.camel.BindToRegistry;
import org.apache.camel.CatalogCamelContext;
import org.apache.camel.test.junit6.CamelTestSupport;
import org.apache.camel.util.json.JsonObject;
import org.apache.camel.util.json.Jsoner;
import org.junit.jupiter.api.Test;

import static org.apache.camel.component.metrics.MetricsConstants.HEADER_COUNTER_DECREMENT;
import static org.apache.camel.component.metrics.MetricsConstants.HEADER_COUNTER_INCREMENT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * A counter header wins over both the increment and the decrement option, and the metricsType path parameter is
 * optional with the default meter.
 */
public class MetricsCounterHeadersAndTypeMetadataTest extends CamelTestSupport {

    @BindToRegistry(MetricsComponent.METRIC_REGISTRY_NAME)
    private final MetricRegistry metricRegistry = new MetricRegistry();

    @Test
    public void decrementHeaderWinsOverIncrementOption() {
        template.sendBodyAndHeader("metrics:counter:down?increment=7", null, HEADER_COUNTER_DECREMENT, 3L);

        assertEquals(-3, metricRegistry.counter("down").getCount());
    }

    @Test
    public void incrementHeaderWinsOverDecrementOption() {
        template.sendBodyAndHeader("metrics:counter:up?decrement=5", null, HEADER_COUNTER_INCREMENT, 2L);

        assertEquals(2, metricRegistry.counter("up").getCount());
    }

    @Test
    public void incrementOptionWinsOverDecrementOptionWithoutHeaders() {
        template.sendBody("metrics:counter:both?increment=7&decrement=5", null);

        assertEquals(7, metricRegistry.counter("both").getCount());
    }

    @Test
    public void metricsTypeIsOptionalWithTheDefaultMeter() throws Exception {
        String json = ((CatalogCamelContext) context).getComponentParameterJsonSchema("metrics");
        JsonObject properties = ((JsonObject) Jsoner.deserialize(json)).getMap("properties");
        JsonObject metricsType = properties.getMap("metricsType");
        assertFalse(metricsType.getBoolean("required"), "metricsType has a default, so it must not be required");
        assertEquals("meter", metricsType.getString("defaultValue"));

        template.sendBody("metrics:events", null);

        assertEquals(1, metricRegistry.meter("events").getCount());
    }
}
