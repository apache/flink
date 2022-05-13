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

package org.apache.flink.runtime.io.disk;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.iomanager.BlockChannelWriter;
import org.apache.flink.runtime.io.disk.iomanager.FileIOChannel;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.memory.MemoryManagerBuilder;
import org.apache.flink.runtime.operators.testutils.DummyInvokable;

import org.junit.Test;

import java.io.EOFException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class SeekableFileChannelInputViewTest {

    @Test
    public void testSeek() {
        final int PAGE_SIZE = 16 * 1024;
        final int NUM_RECORDS = 120000;
        // integers across 7.x pages (7 pages = 114.688 bytes, 8 pages = 131.072 bytes)

        try (IOManager ioManager = new IOManagerAsync()) {
            MemoryManager memMan =
                    MemoryManagerBuilder.newBuilder()
                            .setMemorySize(4 * PAGE_SIZE)
                            .setPageSize(PAGE_SIZE)
                            .build();
            List<MemorySegment> memory = new ArrayList<MemorySegment>();
            memMan.allocatePages(new DummyInvokable(), memory, 4);

            FileIOChannel.ID channel = ioManager.createChannel();
            BlockChannelWriter<MemorySegment> writer = ioManager.createBlockChannelWriter(channel);
            FileChannelOutputView out =
                    new FileChannelOutputView(writer, memMan, memory, memMan.getPageSize());

            // write some integers across 7.5 pages (7 pages = 114.688 bytes, 8 pages = 131.072
            // bytes)
            for (int i = 0; i < NUM_RECORDS; i += 4) {
                out.writeInt(i);
            }
            // close for the first time, make sure all memory returns
            out.close();
            assertTrue(memMan.verifyEmpty());

            memMan.allocatePages(new DummyInvokable(), memory, 4);
            SeekableFileChannelInputView in =
                    new SeekableFileChannelInputView(
                            ioManager, channel, memMan, memory, out.getBytesInLatestSegment());

            // read first, complete
            for (int i = 0; i < NUM_RECORDS; i += 4) {
                assertEquals(i, in.readInt());
            }
            try {
                in.readInt();
                fail("should throw EOF exception");
            } catch (EOFException ignored) {
            }

            // seek to the middle of the 3rd page
            int i = 2 * PAGE_SIZE + PAGE_SIZE / 4;
            in.seek(i);
            for (; i < NUM_RECORDS; i += 4) {
                assertEquals(i, in.readInt());
            }
            try {
                in.readInt();
                fail("should throw EOF exception");
            } catch (EOFException ignored) {
            }

            // seek to the end
            i = 120000 - 4;
            in.seek(i);
            for (; i < NUM_RECORDS; i += 4) {
                assertEquals(i, in.readInt());
            }
            try {
                in.readInt();
                fail("should throw EOF exception");
            } catch (EOFException ignored) {
            }

            // seek to the beginning
            i = 0;
            in.seek(i);
            for (; i < NUM_RECORDS; i += 4) {
                assertEquals(i, in.readInt());
            }
            try {
                in.readInt();
                fail("should throw EOF exception");
            } catch (EOFException ignored) {
            }

            // seek to after a page
            i = PAGE_SIZE;
            in.seek(i);
            for (; i < NUM_RECORDS; i += 4) {
                assertEquals(i, in.readInt());
            }
            try {
                in.readInt();
                fail("should throw EOF exception");
            } catch (EOFException ignored) {
            }

            // seek to after a page
            i = 3 * PAGE_SIZE;
            in.seek(i);
            for (; i < NUM_RECORDS; i += 4) {
                assertEquals(i, in.readInt());
            }
            try {
                in.readInt();
                fail("should throw EOF exception");
            } catch (EOFException ignored) {
            }

            // seek to the end
            i = NUM_RECORDS;
            in.seek(i);
            try {
                in.readInt();
                fail("should throw EOF exception");
            } catch (EOFException ignored) {
            }

            // seek out of bounds
            try {
                in.seek(-10);
                fail("should throw an exception");
            } catch (IllegalArgumentException ignored) {
            }
            try {
                in.seek(NUM_RECORDS + 1);
                fail("should throw an exception");
            } catch (IllegalArgumentException ignored) {
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }
}
