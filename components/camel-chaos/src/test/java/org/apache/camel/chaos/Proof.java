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
package org.apache.camel.chaos;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * Writes the evidence of a proof-point test to {@code target/proof/<name>.txt} (or the directory set with
 * {@code -Dchaos.proof.dir}), so it can be cited.
 */
final class Proof {

    private Proof() {
    }

    static Path dir() {
        return Path.of(System.getProperty("chaos.proof.dir", "target/proof"));
    }

    static void start(String name, String title) {
        write(name, "# " + title + System.lineSeparator(), false);
    }

    static void log(String name, String line) {
        System.out.println("[proof " + name + "] " + line);
        write(name, line + System.lineSeparator(), true);
    }

    private static void write(String name, String text, boolean append) {
        try {
            Files.createDirectories(dir());
            Files.writeString(dir().resolve(name + ".txt"), text, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE,
                    append ? StandardOpenOption.APPEND : StandardOpenOption.TRUNCATE_EXISTING);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }
}
