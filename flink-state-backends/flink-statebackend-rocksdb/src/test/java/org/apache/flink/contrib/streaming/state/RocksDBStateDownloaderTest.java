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

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle.HandleAndLocalPath;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.TestStreamStateHandle;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/** Test class for {@link RocksDBStateDownloader}. */
public class RocksDBStateDownloaderTest extends TestLogger {
    @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    /** Test that the exception arose in the thread pool will rethrow to the main thread. */
    @Test
    public void testMultiThreadRestoreThreadPoolExceptionRethrow() {
        SpecifiedException expectedCause =
                new SpecifiedException("throw exception while multi thread restore.");
        StreamStateHandle stateHandle = new ThrowingStateHandle(expectedCause);

        List<HandleAndLocalPath> stateHandles = new ArrayList<>(1);
        stateHandles.add(HandleAndLocalPath.of(stateHandle, "state1"));

        IncrementalRemoteKeyedStateHandle incrementalKeyedStateHandle =
                new IncrementalRemoteKeyedStateHandle(
                        UUID.randomUUID(),
                        KeyGroupRange.EMPTY_KEY_GROUP_RANGE,
                        1,
                        stateHandles,
                        stateHandles,
                        stateHandle);

        try (RocksDBStateDownloader rocksDBStateDownloader = new RocksDBStateDownloader(5)) {
            rocksDBStateDownloader.transferAllStateDataToDirectory(
                    Collections.singletonList(
                            new StateHandleDownloadSpec(
                                    incrementalKeyedStateHandle,
                                    temporaryFolder.newFolder().toPath())),
                    new CloseableRegistry());
            fail();
        } catch (Exception e) {
            assertEquals(expectedCause, e.getCause());
        }
    }

    /** Tests that download files with multi-thread correctly. */
    @Test
    public void testMultiThreadRestoreCorrectly() throws Exception {
        int numRemoteHandles = 3;
        int numSubHandles = 6;
        byte[][][] contents = createContents(numRemoteHandles, numSubHandles);
        List<StateHandleDownloadSpec> downloadRequests = new ArrayList<>(numRemoteHandles);
        for (int i = 0; i < numRemoteHandles; ++i) {
            downloadRequests.add(
                    createDownloadRequestForContent(
                            temporaryFolder.newFolder().toPath(), contents[i], i));
        }

        try (RocksDBStateDownloader rocksDBStateDownloader = new RocksDBStateDownloader(4)) {
            rocksDBStateDownloader.transferAllStateDataToDirectory(
                    downloadRequests, new CloseableRegistry());
        }

        for (int i = 0; i < numRemoteHandles; ++i) {
            StateHandleDownloadSpec downloadRequest = downloadRequests.get(i);
            Path dstPath = downloadRequest.getDownloadDestination();
            Assert.assertTrue(dstPath.toFile().exists());
            for (int j = 0; j < numSubHandles; ++j) {
                assertStateContentEqual(
                        contents[i][j], dstPath.resolve(String.format("sharedState-%d-%d", i, j)));
            }
        }
    }

    /** Tests cleanup on download failures. */
    @Test
    public void testMultiThreadCleanupOnFailure() throws Exception {
        int numRemoteHandles = 3;
        int numSubHandles = 6;
        byte[][][] contents = createContents(numRemoteHandles, numSubHandles);
        List<StateHandleDownloadSpec> downloadRequests = new ArrayList<>(numRemoteHandles);
        for (int i = 0; i < numRemoteHandles; ++i) {
            downloadRequests.add(
                    createDownloadRequestForContent(
                            temporaryFolder.newFolder().toPath(), contents[i], i));
        }

        IncrementalRemoteKeyedStateHandle stateHandle =
                downloadRequests.get(downloadRequests.size() - 1).getStateHandle();

        // Add a state handle that induces an exception
        stateHandle
                .getSharedState()
                .add(
                        HandleAndLocalPath.of(
                                new ThrowingStateHandle(new IOException("Test exception.")),
                                "error-handle"));

        CloseableRegistry closeableRegistry = new CloseableRegistry();
        try (RocksDBStateDownloader rocksDBStateDownloader = new RocksDBStateDownloader(5)) {
            rocksDBStateDownloader.transferAllStateDataToDirectory(
                    downloadRequests, closeableRegistry);
            fail("Exception is expected");
        } catch (IOException ignore) {
        }

        // Check that all download directories have been deleted
        for (StateHandleDownloadSpec downloadRequest : downloadRequests) {
            Assert.assertFalse(downloadRequest.getDownloadDestination().toFile().exists());
        }
        // The passed in closable registry should not be closed by us on failure.
        Assert.assertFalse(closeableRegistry.isClosed());
    }

    private void assertStateContentEqual(byte[] expected, Path path) throws IOException {
        byte[] actual = Files.readAllBytes(Paths.get(path.toUri()));
        assertArrayEquals(expected, actual);
    }

    private static class SpecifiedException extends IOException {
        SpecifiedException(String message) {
            super(message);
        }
    }

    private static class ThrowingStateHandle implements TestStreamStateHandle {
        private static final long serialVersionUID = -2102069659550694805L;

        private final IOException expectedException;

        private ThrowingStateHandle(IOException expectedException) {
            this.expectedException = expectedException;
        }

        @Override
        public FSDataInputStream openInputStream() throws IOException {
            throw expectedException;
        }

        @Override
        public Optional<byte[]> asBytesIfInMemory() {
            return Optional.empty();
        }

        @Override
        public void discardState() {}

        @Override
        public long getStateSize() {
            return 0;
        }
    }

    private byte[][][] createContents(int numRemoteHandles, int numSubHandles) {
        Random random = new Random();
        byte[][][] contents = new byte[numRemoteHandles][numSubHandles][];
        for (int i = 0; i < numRemoteHandles; ++i) {
            for (int j = 0; j < numSubHandles; ++j) {
                contents[i][j] = new byte[random.nextInt(100000) + 1];
                random.nextBytes(contents[i][j]);
            }
        }
        return contents;
    }

    private StateHandleDownloadSpec createDownloadRequestForContent(
            Path dstPath, byte[][] content, int remoteHandleId) {
        int numSubHandles = content.length;
        List<StreamStateHandle> handles = new ArrayList<>(numSubHandles);
        for (int i = 0; i < numSubHandles; ++i) {
            handles.add(
                    new ByteStreamStateHandle(
                            String.format("state-%d-%d", remoteHandleId, i), content[i]));
        }

        List<HandleAndLocalPath> sharedStates = new ArrayList<>(numSubHandles);
        List<HandleAndLocalPath> privateStates = new ArrayList<>(numSubHandles);
        for (int i = 0; i < numSubHandles; ++i) {
            sharedStates.add(
                    HandleAndLocalPath.of(
                            handles.get(i), String.format("sharedState-%d-%d", remoteHandleId, i)));
            privateStates.add(
                    HandleAndLocalPath.of(
                            handles.get(i),
                            String.format("privateState-%d-%d", remoteHandleId, i)));
        }

        IncrementalRemoteKeyedStateHandle incrementalKeyedStateHandle =
                new IncrementalRemoteKeyedStateHandle(
                        UUID.randomUUID(),
                        KeyGroupRange.of(0, 1),
                        1,
                        sharedStates,
                        privateStates,
                        handles.get(0));

        return new StateHandleDownloadSpec(incrementalKeyedStateHandle, dstPath);
    }
}
