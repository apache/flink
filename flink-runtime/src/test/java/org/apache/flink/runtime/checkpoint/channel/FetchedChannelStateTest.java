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

package org.apache.flink.runtime.checkpoint.channel;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link FetchedChannelState} lifecycle and file list management. */
class FetchedChannelStateTest {

    @TempDir Path tempDir;

    @Test
    void testInitialStateIsEmpty() {
        FetchedChannelState state = new FetchedChannelState(Collections.emptyList());
        assertThat(state.files()).isEmpty();
        assertThat(state.isClosed()).isFalse();
    }

    @Test
    void testFileListPreservesOrder() throws IOException {
        Path file0 = tempDir.resolve("spill-0.bin");
        Path file1 = tempDir.resolve("spill-1.bin");

        try (FetchedChannelState state = new FetchedChannelState(Arrays.asList(file0, file1))) {
            assertThat(state.files()).containsExactly(file0, file1);
        }
    }

    @Test
    void testFilesListIsUnmodifiable() throws IOException {
        try (FetchedChannelState state =
                new FetchedChannelState(Collections.singletonList(tempDir.resolve("f0.bin")))) {
            assertThatThrownBy(() -> state.files().add(tempDir.resolve("f1.bin")))
                    .isInstanceOf(UnsupportedOperationException.class);
        }
    }

    @Test
    void testAcquireReleaseDoesNotDeleteFilesBeforeLastRelease() throws IOException {
        Path realFile = tempDir.resolve("spill-0.bin");
        realFile.toFile().createNewFile();
        FetchedChannelState state = new FetchedChannelState(Collections.singletonList(realFile));

        state.acquire();
        state.acquire();

        state.release();
        // File must still exist after first release.
        assertThat(realFile.toFile()).exists();

        state.release();
        // Last release should delete the file.
        assertThat(realFile.toFile()).doesNotExist();
        assertThat(state.isClosed()).isTrue();
    }

    @Test
    void testCloseDeletesAllFiles() throws IOException {
        Path file0 = tempDir.resolve("f0.bin");
        Path file1 = tempDir.resolve("f1.bin");
        file0.toFile().createNewFile();
        file1.toFile().createNewFile();

        FetchedChannelState state = new FetchedChannelState(Arrays.asList(file0, file1));

        state.close();

        assertThat(file0.toFile()).doesNotExist();
        assertThat(file1.toFile()).doesNotExist();
        assertThat(state.isClosed()).isTrue();
    }

    @Test
    void testCloseIsIdempotent() throws IOException {
        FetchedChannelState state = new FetchedChannelState(Collections.emptyList());
        state.close();
        assertThat(state.isClosed()).isTrue();
        // Second close must not throw.
        state.close();
        assertThat(state.isClosed()).isTrue();
    }

    @Test
    void testCloseAfterReleaseIsIdempotent() throws IOException {
        FetchedChannelState state = new FetchedChannelState(Collections.emptyList());
        state.acquire();
        state.release();
        assertThat(state.isClosed()).isTrue();
        // close() after last release must be a no-op (no double-delete attempt).
        state.close();
        assertThat(state.isClosed()).isTrue();
    }
}
