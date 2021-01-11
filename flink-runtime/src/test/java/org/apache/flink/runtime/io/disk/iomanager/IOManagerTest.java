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

package org.apache.flink.runtime.io.disk.iomanager;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.iomanager.FileIOChannel.ID;
import org.apache.flink.runtime.io.network.buffer.Buffer;

import org.junit.Rule;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Timeout;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.hamcrest.MatcherAssert;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class IOManagerTest {

    @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void channelEnumerator() throws Exception {
        File tempPath = temporaryFolder.newFolder();

        String[] tempDirs =
                new String[] {
                    new File(tempPath, "a").getAbsolutePath(),
                    new File(tempPath, "b").getAbsolutePath(),
                    new File(tempPath, "c").getAbsolutePath(),
                    new File(tempPath, "d").getAbsolutePath(),
                    new File(tempPath, "e").getAbsolutePath(),
                };

        int[] counters = new int[tempDirs.length];
        try (IOManager ioMan = new TestIOManager(tempDirs)) {
            FileIOChannel.Enumerator enumerator = ioMan.createChannelEnumerator();

            for (int i = 0; i < 3 * tempDirs.length; i++) {
                FileIOChannel.ID id = enumerator.next();

                File path = id.getPathFile();

                assertTrue(path.isAbsolute(), "Channel IDs must name an absolute path.");
                assertFalse(path.isDirectory(), "Channel IDs must name a file, not a directory.");

                assertTrue(                        tempPath.equals(path.getParentFile().getParentFile().getParentFile()),                        "Path is not in the temp directory.");

                for (int k = 0; k < tempDirs.length; k++) {
                    if (path.getParentFile().getParent().equals(tempDirs[k])) {
                        counters[k]++;
                    }
                }
            }

            for (int k = 0; k < tempDirs.length; k++) {
                assertEquals(3, counters[k]);
            }
        }
    }

    // --------------------------------------------------------------------------------------------

    private static class TestIOManager extends IOManager {

        protected TestIOManager(String[] paths) {
            super(paths);
        }

        @Override
        public BlockChannelWriter<MemorySegment> createBlockChannelWriter(
                ID channelID, LinkedBlockingQueue<MemorySegment> returnQueue) {
            throw new UnsupportedOperationException();
        }

        @Override
        public BlockChannelWriterWithCallback<MemorySegment> createBlockChannelWriter(
                ID channelID, RequestDoneCallback<MemorySegment> callback) {
            throw new UnsupportedOperationException();
        }

        @Override
        public BlockChannelReader<MemorySegment> createBlockChannelReader(
                ID channelID, LinkedBlockingQueue<MemorySegment> returnQueue) {
            throw new UnsupportedOperationException();
        }

        @Override
        public BufferFileWriter createBufferFileWriter(ID channelID) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public BufferFileReader createBufferFileReader(
                ID channelID, RequestDoneCallback<Buffer> callback) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public BufferFileSegmentReader createBufferFileSegmentReader(
                ID channelID, RequestDoneCallback<FileSegment> callback) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public BulkBlockChannelReader createBulkBlockChannelReader(
                ID channelID, List<MemorySegment> targetSegments, int numBlocks) {
            throw new UnsupportedOperationException();
        }
    }
}
