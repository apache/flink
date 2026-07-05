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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies the reference counter on {@link FetchedChannelState}: acquire/release pairing,
 * zero-triggered file deletion, idempotency, abort-path equivalence, and forced {@link
 * FetchedChannelState#close()} cleanup.
 */
class FetchedChannelStateRefCountTest {

    @TempDir Path tempDir;

    @Test
    void testAcquireReleaseCountsMatch() throws IOException {
        FetchedChannelState state = newStateWithData();
        List<Path> files = state.files();

        // The produced state already holds one handoff grant.
        state.acquire();
        assertFilesExist(files, true);

        state.release();
        assertFilesExist(files, true);

        state.release();
        assertFilesExist(files, false);
    }

    @Test
    void testReachingZeroDeletesFiles() throws IOException {
        FetchedChannelState state = newStateWithData();
        List<Path> files = state.files();
        // The produced state already holds one handoff grant.
        assertFilesExist(files, true);

        state.release();
        assertFilesExist(files, false);
    }

    @Test
    void testReleaseAfterZeroIsNoOp() throws IOException {
        FetchedChannelState state = newStateWithData();
        List<Path> files = state.files();
        // Release the single handoff grant the produced state already holds.
        state.release();
        assertFilesExist(files, false);

        // Extra releases past zero must be a no-op.
        state.release();
        state.release();
        assertFilesExist(files, false);
    }

    @Test
    void testAbortPathReleasesViaSameRoute() throws IOException {
        FetchedChannelState state = newStateWithData();
        List<Path> files = state.files();

        // The produced state already holds one handoff grant.
        state.acquire();
        state.acquire();

        state.release();
        state.release();
        assertFilesExist(files, true);

        state.release();
        assertFilesExist(files, false);
    }

    @Test
    void testForceCloseStillCleansFiles() throws IOException {
        FetchedChannelState state = newStateWithData();
        List<Path> files = state.files();
        // The produced state already holds one handoff grant.
        state.acquire();
        assertFilesExist(files, true);

        state.close();
        assertFilesExist(files, false);

        // Double close must be a no-op.
        state.close();

        // Late release after close must not re-delete or throw.
        state.release();
        assertFilesExist(files, false);
    }

    // -------------------------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------------------------

    private FetchedChannelState newStateWithData() throws IOException {
        InputChannelInfo ch = new InputChannelInfo(0, 0);
        try (TestSpillWriter writer = new TestSpillWriter(tempDir)) {
            writer.writeRecord(ch, new byte[] {1, 2, 3}, 3);
            writer.writeRecord(new InputChannelInfo(0, 1), new byte[] {4, 5}, 2);
            return writer.getChannelState();
        }
    }

    private static void assertFilesExist(List<Path> files, boolean expected) {
        for (Path file : files) {
            assertThat(Files.exists(file))
                    .as("file " + file + " exists=" + expected)
                    .isEqualTo(expected);
        }
    }
}
