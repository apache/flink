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

package org.apache.flink.runtime.state;

import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.util.Preconditions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.EnumMap;
import java.util.UUID;

/** Unit tests for {@link IncrementalLocalKeyedStateHandle}. */
public class IncrementalLocalKeyedStateHandleTest {

    @Test
    public void testDirectorySize(@TempDir Path directory) throws IOException {
        final int metaDataBytes = 42;
        ByteStreamStateHandle metaDataStateHandle =
                new ByteStreamStateHandle("MetaDataTest", new byte[metaDataBytes]);

        final int fileOneBytes = 1024;
        File outputFile = new File(directory.toFile(), "out.001");
        try (FileOutputStream outputStream = new FileOutputStream(outputFile)) {
            outputStream.write(new byte[fileOneBytes]);
        }

        File subPath = new File(directory.toFile(), "subdir");
        Preconditions.checkState(subPath.mkdirs());
        final int fileTwoBytes = 128;
        outputFile = new File(subPath, "out.002");
        try (FileOutputStream outputStream = new FileOutputStream(outputFile)) {
            outputStream.write(new byte[fileTwoBytes]);
        }

        DirectoryStateHandle directoryStateHandle = DirectoryStateHandle.forPathWithSize(directory);
        IncrementalLocalKeyedStateHandle handle =
                new IncrementalLocalKeyedStateHandle(
                        UUID.randomUUID(),
                        0,
                        directoryStateHandle,
                        new KeyGroupRange(0, 1),
                        metaDataStateHandle,
                        Collections.emptyList());

        StateObject.StateObjectSizeStatsCollector stats =
                StateObject.StateObjectSizeStatsCollector.create();
        handle.collectSizeStats(stats);
        Assertions.assertEquals(
                metaDataBytes + fileOneBytes + fileTwoBytes, extractLocalStateSizes(stats));
    }

    private long extractLocalStateSizes(StateObject.StateObjectSizeStatsCollector stats) {
        EnumMap<StateObject.StateObjectLocation, Long> statsMap = stats.getStats();
        Assertions.assertFalse(statsMap.containsKey(StateObject.StateObjectLocation.REMOTE));
        Assertions.assertFalse(statsMap.containsKey(StateObject.StateObjectLocation.UNKNOWN));
        // Might be in a byte handle or a file.
        return statsMap.get(StateObject.StateObjectLocation.LOCAL_DISK)
                + statsMap.get(StateObject.StateObjectLocation.LOCAL_MEMORY);
    }
}
