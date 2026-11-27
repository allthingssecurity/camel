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

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.test.junit6.CamelTestSupport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * fileWatcher with a fileName without a directory watches the current directory, so the file is read again after it is
 * overwritten. The file is created in the working directory of the test, and deleted after it.
 */
public class StreamFileWatcherCurrentDirectoryTest extends CamelTestSupport {

    private final String fileName = "stream-watcher-" + UUID.randomUUID() + ".txt";

    @Override
    public boolean isUseRouteBuilder() {
        return false;
    }

    @AfterEach
    public void deleteFile() throws Exception {
        Files.deleteIfExists(Path.of(fileName));
    }

    @Test
    public void testOverwrittenFileIsReadAgain() throws Exception {
        Path file = Path.of(fileName);
        Files.writeString(file, "first\n", StandardCharsets.UTF_8);

        MockEndpoint mock = getMockEndpoint("mock:result");
        mock.expectedBodiesReceived("first");
        context.addRoutes(new RouteBuilder() {
            @Override
            public void configure() {
                from("stream:file?fileName=" + fileName
                     + "&scanStream=true&scanStreamDelay=100&retry=true&fileWatcher=true")
                        .to("mock:result");
            }
        });
        context.start();
        mock.assertIsSatisfied();

        // shorter than before, so the stream that is open sees no new data: only a watcher event opens it again
        mock.reset();
        mock.expectedBodiesReceived("new");
        mock.setResultWaitTime(20000);
        Files.writeString(file, "new\n", StandardCharsets.UTF_8);
        mock.assertIsSatisfied();
    }
}
