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
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.StateHandleID;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.util.TestLogger;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
        SpecifiedException expectedException =
                new SpecifiedException("throw exception while multi thread restore.");
        StreamStateHandle stateHandle =
                new StreamStateHandle() {
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
                };

        Map<StateHandleID, StreamStateHandle> stateHandles = new HashMap<>(1);
        stateHandles.put(new StateHandleID("state1"), stateHandle);

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
                    incrementalKeyedStateHandle,
                    temporaryFolder.newFolder().toPath(),
                    new CloseableRegistry());
            fail();
        } catch (Exception e) {
            assertEquals(expectedException, e);
        }
    }

    /** Tests that download files with multi-thread correctly. */
    @Test
    public void testMultiThreadRestoreCorrectly() throws Exception {
        Random random = new Random();
        int contentNum = 6;
        byte[][] contents = new byte[contentNum][];
        for (int i = 0; i < contentNum; ++i) {
            contents[i] = new byte[random.nextInt(100000) + 1];
            random.nextBytes(contents[i]);
        }

        List<StreamStateHandle> handles = new ArrayList<>(contentNum);
        for (int i = 0; i < contentNum; ++i) {
            handles.add(new ByteStreamStateHandle(String.format("state%d", i), contents[i]));
        }

        Map<StateHandleID, StreamStateHandle> sharedStates = new HashMap<>(contentNum);
        Map<StateHandleID, StreamStateHandle> privateStates = new HashMap<>(contentNum);
        for (int i = 0; i < contentNum; ++i) {
            sharedStates.put(new StateHandleID(String.format("sharedState%d", i)), handles.get(i));
            privateStates.put(
                    new StateHandleID(String.format("privateState%d", i)), handles.get(i));
        }

        IncrementalRemoteKeyedStateHandle incrementalKeyedStateHandle =
                new IncrementalRemoteKeyedStateHandle(
                        UUID.randomUUID(),
                        KeyGroupRange.of(0, 1),
                        1,
                        sharedStates,
                        privateStates,
                        handles.get(0));

        Path dstPath = temporaryFolder.newFolder().toPath();
        try (RocksDBStateDownloader rocksDBStateDownloader = new RocksDBStateDownloader(5)) {
            rocksDBStateDownloader.transferAllStateDataToDirectory(
                    incrementalKeyedStateHandle, dstPath, new CloseableRegistry());
        }

        for (int i = 0; i < contentNum; ++i) {
            assertStateContentEqual(
                    contents[i], dstPath.resolve(String.format("sharedState%d", i)));
        }
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
}
