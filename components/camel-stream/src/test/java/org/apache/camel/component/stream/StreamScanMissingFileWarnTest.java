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

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.test.junit6.CamelTestSupport;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.Test;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A scanned file that cannot be read when the consumer starts is not opened again without retry: the consumer logs a
 * WARN that says so, once, and none with retry.
 */
public class StreamScanMissingFileWarnTest extends CamelTestSupport {

    @Override
    public boolean isUseRouteBuilder() {
        return false;
    }

    @Test
    public void testWarnWithoutRetry() throws Exception {
        List<LogEvent> warnings = warningsOf("target/stream/missing/without-retry.txt", "");
        assertEquals(1, warnings.size(), warnings.toString());
        assertTrue(warnings.get(0).getMessage().getFormattedMessage().contains("retry"));
    }

    @Test
    public void testNoWarnWithRetry() throws Exception {
        assertEquals(List.of(), warningsOf("target/stream/missing/with-retry.txt", "&retry=true"));
    }

    private List<LogEvent> warningsOf(String fileName, String options) throws Exception {
        try (WarnCapture capture = new WarnCapture()) {
            context.addRoutes(new RouteBuilder() {
                @Override
                public void configure() {
                    from("stream:file?fileName=" + fileName + "&scanStream=true&scanStreamDelay=50" + options)
                            .to("mock:result");
                }
            });
            context.start();
            // several scans of the missing file
            await().pollDelay(500, TimeUnit.MILLISECONDS).atMost(5, TimeUnit.SECONDS).until(() -> true);
            context.stop();
            return capture.events;
        }
    }

    /**
     * Captures the WARN events of the logger of the consumer while it is open.
     */
    private static final class WarnCapture extends AbstractAppender implements AutoCloseable {
        private final List<LogEvent> events = new CopyOnWriteArrayList<>();
        private final Logger logger = (Logger) LogManager.getLogger(StreamConsumer.class);

        WarnCapture() {
            super("WarnCapture-StreamConsumer", null, null, true, Property.EMPTY_ARRAY);
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
