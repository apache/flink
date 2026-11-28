/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.webmonitor.history;

import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests publication failures that depend on the filesystem provider. */
class FileArchiveStorageTest {

    @TempDir Path root;

    @ParameterizedTest
    @ValueSource(strings = {"unsupported", "existing-target", "io-error"})
    void testFailedAtomicReplacementKeepsLastGoodEntry(String failure) throws Exception {
        AtomicBoolean rejectReplacement = new AtomicBoolean();
        IOException expected =
                failure.equals("unsupported")
                        ? new AtomicMoveNotSupportedException("source", "target", "unsupported")
                        : failure.equals("existing-target")
                                ? new FileAlreadyExistsException("target")
                                : new IOException("Injected publication failure");
        try (FileArchiveStorage storage =
                new FileArchiveStorage(
                        root.toFile(),
                        (source, target) -> {
                            if (rejectReplacement.get()) {
                                throw expected;
                            }
                            Files.move(source, target, StandardCopyOption.ATOMIC_MOVE);
                        })) {
            String key = "jobs/overview.json";
            String previous = "{\"jobs\":[]}";
            String replacement = "{\"jobs\":[{\"name\":\"new-job\"}]}";
            storage.putArchiveContent(key, previous);
            rejectReplacement.set(true);

            assertThatThrownBy(() -> storage.putArchiveContent(key, replacement))
                    .isSameAs(expected);
            assertThat(storage.readArchiveContent(storage.getEntry(key))).isEqualTo(previous);
            assertThat(root.resolve("jobs")).isDirectoryContaining("glob:**/overview.json");
            try (var entries = Files.list(root.resolve("jobs"))) {
                assertThat(entries).containsExactly(root.resolve(key));
            }

            rejectReplacement.set(false);
            storage.putArchiveContent(key, replacement);
            assertThat(storage.readArchiveContent(storage.getEntry(key))).isEqualTo(replacement);
        }
    }
}
