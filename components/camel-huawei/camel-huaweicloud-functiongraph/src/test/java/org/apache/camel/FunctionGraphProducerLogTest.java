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
package org.apache.camel;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import com.huaweicloud.sdk.functiongraph.v2.model.InvokeFunctionRequest;
import com.huaweicloud.sdk.functiongraph.v2.model.InvokeFunctionResponse;
import org.apache.camel.constants.FunctionGraphProperties;
import org.apache.camel.support.DefaultExchange;
import org.apache.camel.test.junit6.CamelTestSupport;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The WARN lines of the producer: one at start when the endpoint option is set without the region option, and none for
 * a message without the CamelHwCloudFgXCffLogType exchange property.
 */
public class FunctionGraphProducerLogTest extends CamelTestSupport {

    private static final String OPTIONS = "projectId=proj-1&functionName=fn&functionPackage=pkg&accessKey=ak&secretKey=sk"
                                          + "&functionGraphClient=#functionGraphClient";

    @BindToRegistry("functionGraphClient")
    FunctionGraphMockClient mockClient = new FunctionGraphMockClient(null) {
        @Override
        public InvokeFunctionResponse invokeFunction(InvokeFunctionRequest request) {
            // the mock client of the module needs a log type
            return new InvokeFunctionResponse().withResult("{}").withStatus(200);
        }
    };

    @Test
    public void testWarnAtStartWhenEndpointWithoutRegion() throws Exception {
        List<LogEvent> warnings = warningsOf("hwcloud-functiongraph:invokeFunction?endpoint=https://function.example.com&"
                                             + OPTIONS,
                null);
        assertEquals(1, warnings.size(), warnings.toString());
        assertTrue(warnings.get(0).getMessage().getFormattedMessage().contains("region option"));
    }

    @Test
    public void testNoWarnWithRegionAndWithoutLogType() throws Exception {
        // a message without the CamelHwCloudFgXCffLogType exchange property, which is optional
        assertEquals(List.of(), warningsOf("hwcloud-functiongraph:invokeFunction?region=cn-north-4&" + OPTIONS, "{}"));
        assertEquals(List.of(), warningsOf(
                "hwcloud-functiongraph:invokeFunction?region=cn-north-4&endpoint=https://function.example.com&" + OPTIONS,
                "{}"));
    }

    private List<LogEvent> warningsOf(String uri, String body) throws Exception {
        Endpoint endpoint = context.getEndpoint(uri);
        try (WarnCapture capture = new WarnCapture()) {
            Producer producer = endpoint.createProducer();
            producer.start();
            try {
                if (body != null) {
                    Exchange exchange = new DefaultExchange(context);
                    exchange.getMessage().setBody(body);
                    producer.process(exchange);
                    assertNull(exchange.getException());
                    assertNull(exchange.getProperty(FunctionGraphProperties.XCFFLOGS));
                }
            } finally {
                producer.stop();
            }
            return capture.events;
        }
    }

    /**
     * Captures the WARN events of the logger of the producer while it is open.
     */
    private static final class WarnCapture extends AbstractAppender implements AutoCloseable {
        private final List<LogEvent> events = new CopyOnWriteArrayList<>();
        private final Logger logger = (Logger) LogManager.getLogger(FunctionGraphProducer.class);

        WarnCapture() {
            super("WarnCapture-FunctionGraphProducer", null, null, true, Property.EMPTY_ARRAY);
            start();
            logger.addAppender(this);
        }

        @Override
        public void append(LogEvent event) {
            if (event.getLevel() == Level.WARN) {
                events.add(event.toImmutable());
            }
        }

        @Override
        public void close() {
            logger.removeAppender(this);
            stop();
        }
    }
}
