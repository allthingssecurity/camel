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
package org.apache.camel.component.stream;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import org.apache.camel.Exchange;
import org.apache.camel.support.service.ServiceHelper;
import org.apache.camel.test.junit6.CamelTestSupport;
import org.junit.jupiter.api.Test;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * groupLines with kind in: the lines of System.in are grouped, and the lines left are sent when it ends. The test
 * replaces System.in of the consumer only, not the one of the JVM.
 */
public class StreamInGroupLinesTest extends CamelTestSupport {

    @Test
    public void testGroupLinesOfSystemIn() throws Exception {
        assertEquals(List.of(List.of("a", "b"), List.of("c")), bodies("stream:in?groupLines=2", 2),
                "two lines per exchange, and the line left at the end");
    }

    @Test
    public void testEachLineWithoutGroupLines() throws Exception {
        // unchanged: without groupLines each line is an exchange, which is complete as kind in is not read ahead
        assertEquals(List.of("a", "b", "c"), bodies("stream:in", 3));
    }

    private List<Object> bodies(String uri, int count) throws Exception {
        StreamEndpoint endpoint = context.getEndpoint(uri + (uri.contains("?") ? "&" : "?") + "initialPromptDelay=0",
                StreamEndpoint.class);
        ServiceHelper.startService(endpoint);
        List<Exchange> received = new CopyOnWriteArrayList<>();
        StreamConsumer consumer = new StreamConsumer(endpoint, received::add, endpoint.getEndpointUri()) {
            @Override
            InputStream systemIn() {
                return new ByteArrayInputStream("a\nb\nc\n".getBytes(StandardCharsets.UTF_8));
            }
        };
        consumer.start();
        try {
            await().atMost(10, TimeUnit.SECONDS).until(() -> received.size() >= count);
        } finally {
            consumer.stop();
        }
        assertEquals(count, received.size());
        for (int i = 0; i < count; i++) {
            Exchange exchange = received.get(i);
            assertEquals((long) i, exchange.getMessage().getHeader(StreamConstants.STREAM_INDEX));
            boolean expectedComplete = !uri.contains("groupLines") || i == count - 1;
            assertEquals(expectedComplete, exchange.getMessage().getHeader(StreamConstants.STREAM_COMPLETE),
                    "complete of exchange " + i);
        }
        return received.stream().map(e -> e.getMessage().getBody()).toList();
    }
}
