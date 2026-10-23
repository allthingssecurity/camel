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
package org.apache.camel.component.file;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.camel.CamelContext;
import org.apache.camel.CamelExecutionException;
import org.apache.camel.Exchange;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.impl.DefaultCamelContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * With fileExist=Move, ${file:name} in moveExisting is the name of the existing file in its directory, so the existing
 * file is moved below that directory, and not into a copy of the path of the endpoint.
 */
class FileProducerMoveExistingFileNameTest {

    @TempDir
    Path dir;

    @Test
    void fileNameIsTheNameInTheDirectory() throws Exception {
        Path out = dir.resolve("out");
        write(out.resolve("x.txt"), "old");

        send("file:" + out + "?fileExist=Move&moveExisting=old/${file:name}", "x.txt", "new");

        assertEquals("old", Files.readString(out.resolve("old/x.txt")));
        assertEquals("new", Files.readString(out.resolve("x.txt")));
    }

    @Test
    void fileNameWithAPrefix() throws Exception {
        Path out = dir.resolve("out");
        write(out.resolve("x.txt"), "old");

        send("file:" + out + "?fileExist=Move&moveExisting=old-${file:name}", "x.txt", "new");

        assertEquals("old", Files.readString(out.resolve("old-x.txt")));
    }

    @Test
    void fileNameWithoutExtension() throws Exception {
        Path out = dir.resolve("out");
        write(out.resolve("x.txt"), "old");

        send("file:" + out + "?fileExist=Move&moveExisting=old/${file:name.noext}.bak", "x.txt", "new");

        assertEquals("old", Files.readString(out.resolve("old/x.bak")));
    }

    @Test
    void fileInASubdirectoryOfTheEndpoint() throws Exception {
        Path out = dir.resolve("out");
        write(out.resolve("sub/x.txt"), "old");

        send("file:" + out + "?fileExist=Move&moveExisting=old/${file:name}", "sub/x.txt", "new");

        assertEquals("old", Files.readString(out.resolve("sub/old/x.txt")));
        assertEquals("new", Files.readString(out.resolve("sub/x.txt")));
    }

    @Test
    void existingBackupIsNotOverwrittenWithoutEagerDelete() throws Exception {
        Path out = dir.resolve("out");
        write(out.resolve("x.txt"), "old");
        write(out.resolve("old/x.txt"), "older");

        String uri = "file:" + out + "?fileExist=Move&moveExisting=old/${file:name}&eagerDeleteTargetFile=false";
        CamelExecutionException e = assertThrows(CamelExecutionException.class, () -> send(uri, "x.txt", "new"));

        assertEquals(GenericFileOperationFailedException.class, e.getCause().getClass());
        assertEquals("older", Files.readString(out.resolve("old/x.txt")));
        assertEquals("old", Files.readString(out.resolve("x.txt")));
    }

    @Test
    void fileNameInAnAbsoluteMoveExisting() throws Exception {
        Path out = dir.resolve("out");
        Path archive = dir.resolve("archive");
        write(out.resolve("x.txt"), "old");

        send("file:" + out + "?fileExist=Move&moveExisting=" + archive + "/${file:name}", "x.txt", "new");

        assertEquals("old", Files.readString(archive.resolve("x.txt")));
    }

    @Test
    void nameThatStartsWithTheNameOfARelativeDirectoryIsPutInThatDirectory() throws Exception {
        try (CamelContext context = new DefaultCamelContext()) {
            context.start();
            FileEndpoint endpoint
                    = context.getEndpoint("file:out?fileExist=Move&moveExisting=${file:name}.bak", FileEndpoint.class);
            AtomicReference<String> movedTo = new AtomicReference<>();
            FileOperations operations = new FileOperations(endpoint) {
                @Override
                public boolean buildDirectory(String directory, boolean absolute) {
                    return true;
                }

                @Override
                public boolean existsFile(String name) {
                    return false;
                }

                @Override
                public boolean renameFile(String from, String to) {
                    movedTo.set(to);
                    return true;
                }
            };

            new GenericFileDefaultMoveExistingFileStrategy().moveExistingFile(endpoint, operations, "out/out-1.txt");

            assertEquals("out/out-1.txt.bak", movedTo.get());
        }
    }

    @Test
    void parentAndOnlyNameAreUnchanged() throws Exception {
        Path out = dir.resolve("out");
        write(out.resolve("x.txt"), "old");

        send("file:" + out + "?fileExist=Move&moveExisting=${file:parent}/old/${file:onlyname}", "x.txt", "new");

        assertEquals("old", Files.readString(out.resolve("old/x.txt")));
        assertFalse(Files.exists(out.resolve("old/old")));
    }

    private static void write(Path file, String content) throws Exception {
        Files.createDirectories(file.getParent());
        Files.writeString(file, content);
    }

    private static void send(String uri, String fileName, String body) throws Exception {
        try (CamelContext context = new DefaultCamelContext()) {
            context.start();
            ProducerTemplate template = context.createProducerTemplate();
            template.sendBodyAndHeaders(uri, body, Map.of(Exchange.FILE_NAME, fileName));
        }
    }
}
