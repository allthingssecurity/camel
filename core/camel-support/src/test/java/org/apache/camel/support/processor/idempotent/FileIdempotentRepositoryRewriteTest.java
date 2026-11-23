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
package org.apache.camel.support.processor.idempotent;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.FileSystemException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import org.apache.camel.RuntimeCamelException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * The file store is rewritten when a key is removed, or when the oldest keys are dropped because the store is too big.
 * Neither a failing write nor a JVM that stops while the store is rewritten may lose the keys that are kept, and a key
 * is appended to the store in a single write.
 */
class FileIdempotentRepositoryRewriteTest {

    private static final String KEYS = "k1\nk2\nk3\nk4\nk5\nk6\n";

    @TempDir
    Path dir;

    private Path store;
    private final List<FileIdempotentRepository> repositories = new ArrayList<>();

    @AfterEach
    void stop() {
        repositories.forEach(FileIdempotentRepository::stop);
    }

    @Test
    void removeKeepsTheStoreWhenTheRewriteFails() throws Exception {
        TestRepository repository = startRepository(KEYS);
        // the rewrite fails after a few bytes (such as when the disk is full)
        repository.rewriteBytesBeforeFailure = 4;

        assertThatThrownBy(() -> repository.remove("k6")).isInstanceOf(RuntimeCamelException.class)
                .hasMessageContaining("No space left on device");

        // the store is unchanged, and has every key that is still in the cache
        assertThat(Files.readString(store)).isEqualTo(KEYS);
        assertThat(lines()).containsAll(repository.getCache().keySet());
        assertThat(keysAfterRestart()).containsExactlyInAnyOrder("k1", "k2", "k3", "k4", "k5", "k6");
        assertThat(tempFile()).doesNotExist();

        // and the remove works once the store can be written again
        repository.rewriteBytesBeforeFailure = Long.MAX_VALUE;
        repository.remove("k6");
        assertThat(Files.readString(store)).isEqualTo("k1\nk2\nk3\nk4\nk5\n");
    }

    @Test
    void removeKeepsTheStoreWhenTheJvmStopsWhileRewriting() throws Exception {
        TestRepository repository = startRepository(KEYS);
        // the JVM stops after the first two lines are rewritten
        repository.rewriteBytesBeforeFailure = 6;
        repository.rewriteCrashes = true;

        assertThatThrownBy(() -> repository.remove("k6")).isInstanceOf(SimulatedCrash.class);

        // after the restart every key is still there (the remove did not complete)
        assertThat(keysAfterRestart()).containsExactlyInAnyOrder("k1", "k2", "k3", "k4", "k5", "k6");
    }

    @Test
    void trunkKeepsTheStoreWhenTheJvmStopsWhileRewriting() throws Exception {
        TestRepository repository = startRepository(KEYS);
        // adding the 7th key (21 bytes) drops the 2 oldest keys
        repository.setMaxFileStoreSize(20);
        repository.setDropOldestFileStore(2);
        // the JVM stops after the first line is rewritten
        repository.rewriteBytesBeforeFailure = 3;
        repository.rewriteCrashes = true;

        assertThatThrownBy(() -> repository.add("k7")).isInstanceOf(SimulatedCrash.class);

        // after the restart no key is lost: the trunk did not complete
        assertThat(keysAfterRestart()).containsExactlyInAnyOrder("k1", "k2", "k3", "k4", "k5", "k6", "k7");
    }

    @Test
    void addWhoseTrunkFailsIsAddedAndStored() throws Exception {
        TestRepository repository = startRepository(KEYS);
        repository.setMaxFileStoreSize(20);
        repository.setDropOldestFileStore(2);
        repository.rewriteBytesBeforeFailure = 3;

        // the key is stored before the store is trunked, so a failing trunk does not fail the add
        assertThat(repository.add("k7")).isTrue();

        // the store is unchanged, has the new key, and has every key that is in the cache
        assertThat(Files.readString(store)).isEqualTo(KEYS + "k7\n");
        assertThat(lines()).containsAll(repository.getCache().keySet());
        assertThat(repository.add("k7")).as("a duplicate").isFalse();
        assertThat(keysAfterRestart()).containsExactlyInAnyOrder("k1", "k2", "k3", "k4", "k5", "k6", "k7");

        // the next add trunks the store
        repository.rewriteBytesBeforeFailure = Long.MAX_VALUE;
        assertThat(repository.add("k8")).isTrue();
        assertThat(Files.readString(store)).isEqualTo("k3\nk4\nk5\nk6\nk7\nk8\n");
    }

    @Test
    void appendWritesTheKeyAndTheDelimiterInOneWrite() throws Exception {
        TestRepository repository = startRepository("");

        repository.add("k1");
        repository.add("k2");
        repository.add("k3");

        assertThat(repository.appendWrites).isEqualTo(3);
        assertThat(Files.readString(store)).isEqualTo("k1\nk2\nk3\n");
    }

    @Test
    void jvmStoppingWhileAppendingDoesNotMergeTheNextKey() throws Exception {
        TestRepository repository = startRepository("k1\n");
        // the JVM stops before a second write to the store
        repository.appendWritesBeforeCrash = 1;

        try {
            repository.add("k2");
        } catch (SimulatedCrash e) {
            // the JVM stopped before the delimiter was written
        }

        // after the restart, the next key is not merged into the previous one
        TestRepository restarted = startRepository(null);
        assertThat(restarted.add("k3")).isTrue();
        assertThat(keysAfterRestart()).contains("k1", "k3");
    }

    @Test
    void staleTempFileIsReplaced() throws Exception {
        TestRepository repository = startRepository(KEYS);
        // left by a JVM that stopped while rewriting
        Files.writeString(tempFile(), "k1\nk2\n");

        repository.remove("k6");

        assertThat(Files.readString(store)).isEqualTo("k1\nk2\nk3\nk4\nk5\n");
        assertThat(tempFile()).doesNotExist();
    }

    @Test
    void storeIsRewrittenInPlaceWhenTheTempFileCannotBeCreated() throws Exception {
        TestRepository repository = startRepository(KEYS);
        // a directory that cannot be deleted where the temporary file would be
        Files.createDirectories(tempFile().resolve("sub"));

        repository.remove("k6");

        assertThat(Files.readString(store)).isEqualTo("k1\nk2\nk3\nk4\nk5\n");
        assertThat(keysAfterRestart()).containsExactlyInAnyOrder("k1", "k2", "k3", "k4", "k5");
    }

    @Test
    void storeIsRewrittenInPlaceWhenTheTempFileCannotBeMoved() throws Exception {
        TestRepository repository = startRepository(KEYS);
        // such as a store that is a single file mounted into a container (rename(2) onto a mount point is EBUSY)
        repository.moveFailure = "Device or resource busy";

        repository.remove("k6");

        assertThat(Files.readString(store)).isEqualTo("k1\nk2\nk3\nk4\nk5\n");
        assertThat(tempFile()).doesNotExist();
        assertThat(keysAfterRestart()).containsExactlyInAnyOrder("k1", "k2", "k3", "k4", "k5");

        // a trunk is rewritten in place too, so an add that drops the oldest keys keeps working
        repository.setMaxFileStoreSize(17);
        repository.setDropOldestFileStore(2);
        assertThat(repository.add("k7")).isTrue();
        assertThat(Files.readString(store)).isEqualTo("k3\nk4\nk5\nk7\n");
        assertThat(tempFile()).doesNotExist();
    }

    @Test
    @DisabledOnOs(OS.WINDOWS)
    void symlinkedStoreIsKeptWhenRewriting() throws Exception {
        Path realStore = Files.createDirectories(dir.resolve("real")).resolve("store.dat");
        Files.writeString(realStore, KEYS);
        store = dir.resolve("store.dat");
        try {
            Files.createSymbolicLink(store, realStore);
        } catch (UnsupportedOperationException | IOException e) {
            assumeTrue(false, "Symbolic links are not supported: " + e.getMessage());
        }
        TestRepository repository = startRepository(null);

        repository.remove("k6");

        assertThat(Files.isSymbolicLink(store)).isTrue();
        assertThat(Files.readSymbolicLink(store)).isEqualTo(realStore);
        assertThat(Files.readString(realStore)).isEqualTo("k1\nk2\nk3\nk4\nk5\n");
        assertThat(Path.of(realStore + ".tmp")).doesNotExist();
        assertThat(Files.exists(Path.of(store + ".tmp"), LinkOption.NOFOLLOW_LINKS)).isFalse();
    }

    @Test
    void storePermissionsAreKeptWhenRewriting() throws Exception {
        assumeTrue(FileSystems.getDefault().supportedFileAttributeViews().contains("posix"),
                "POSIX file permissions are not supported");
        Set<PosixFilePermission> permissions = PosixFilePermissions.fromString("rw-------");
        TestRepository repository = startRepository(KEYS);
        Files.setPosixFilePermissions(store, permissions);

        repository.remove("k6");

        assertThat(repository.rewritePermissions).containsExactly(permissions);
        assertThat(Files.getPosixFilePermissions(store)).isEqualTo(permissions);
        assertThat(Files.readString(store)).isEqualTo("k1\nk2\nk3\nk4\nk5\n");
    }

    private TestRepository startRepository(String content) throws IOException {
        if (store == null) {
            store = dir.resolve("store.dat");
        }
        if (content != null) {
            Files.writeString(store, content);
        }
        TestRepository repository = new TestRepository(store.toFile());
        repository.start();
        repositories.add(repository);
        return repository;
    }

    /**
     * The keys that a repository started on the store has, as after a restart.
     */
    private Set<String> keysAfterRestart() {
        FileIdempotentRepository repository = new FileIdempotentRepository(store.toFile(), new HashMap<>());
        repository.start();
        repositories.add(repository);
        return repository.getCache().keySet();
    }

    private List<String> lines() throws IOException {
        return Files.readAllLines(store);
    }

    private Path tempFile() {
        return Path.of(store + ".tmp");
    }

    /**
     * The JVM stopping at this point (unlike an IOException, nothing handles it).
     */
    private static final class SimulatedCrash extends Error {
        SimulatedCrash() {
            super("Simulated JVM stop");
        }
    }

    private static final class TestRepository extends FileIdempotentRepository {
        long rewriteBytesBeforeFailure = Long.MAX_VALUE;
        boolean rewriteCrashes;
        int appendWritesBeforeCrash = Integer.MAX_VALUE;
        int appendWrites;
        final List<Set<PosixFilePermission>> rewritePermissions = new ArrayList<>();
        String moveFailure;

        TestRepository(File store) {
            super(store, new HashMap<>());
        }

        @Override
        FileOutputStream newOutputStream(File file, boolean append) throws IOException {
            if (append) {
                return new TestStream(this, file, true);
            }
            if (FileSystems.getDefault().supportedFileAttributeViews().contains("posix")) {
                rewritePermissions.add(Files.getPosixFilePermissions(file.toPath()));
            }
            return new TestStream(this, file, false);
        }

        @Override
        void moveTempStore(Path tmp, Path target) throws IOException {
            if (moveFailure != null) {
                // what Files.move throws when rename(2) fails with an error other than EXDEV
                throw new FileSystemException(tmp.toString(), target.toString(), moveFailure);
            }
            super.moveTempStore(tmp, target);
        }
    }

    private static final class TestStream extends FileOutputStream {
        private final TestRepository repository;
        private final boolean append;
        private int writes;
        private long written;

        TestStream(TestRepository repository, File file, boolean append) throws IOException {
            super(file, append);
            this.repository = repository;
            this.append = append;
        }

        @Override
        public void write(int b) throws IOException {
            write(new byte[] { (byte) b }, 0, 1);
        }

        @Override
        public void write(byte[] b) throws IOException {
            write(b, 0, b.length);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            if (append) {
                if (++writes > repository.appendWritesBeforeCrash) {
                    throw new SimulatedCrash();
                }
                repository.appendWrites++;
                super.write(b, off, len);
                return;
            }
            long left = repository.rewriteBytesBeforeFailure - written;
            if (len > left) {
                super.write(b, off, (int) left);
                written += left;
                if (repository.rewriteCrashes) {
                    throw new SimulatedCrash();
                }
                throw new IOException("No space left on device");
            }
            super.write(b, off, len);
            written += len;
        }
    }
}
