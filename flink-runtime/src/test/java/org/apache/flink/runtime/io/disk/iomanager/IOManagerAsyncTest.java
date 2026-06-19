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
import org.apache.flink.core.memory.MemorySegmentFactory;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

class IOManagerAsyncTest {

    private IOManagerAsync ioManager;

    // ------------------------------------------------------------------------
    //                           Setup & Shutdown
    // ------------------------------------------------------------------------

    @BeforeEach
    void beforeTest() {
        ioManager = new IOManagerAsync();
    }

    @AfterEach
    void afterTest() throws Exception {
        this.ioManager.close();
    }

    // ------------------------------------------------------------------------
    //                           Test Methods
    // ------------------------------------------------------------------------

    @Test
    void channelReadWriteOneSegment() throws Exception {
        final int NUM_IOS = 1111;

        final FileIOChannel.ID channelID = this.ioManager.createChannel();
        final BlockChannelWriter<MemorySegment> writer =
                this.ioManager.createBlockChannelWriter(channelID);

        MemorySegment memSeg = MemorySegmentFactory.allocateUnpooledSegment(32 * 1024);

        for (int i = 0; i < NUM_IOS; i++) {
            for (int pos = 0; pos < memSeg.size(); pos += 4) {
                memSeg.putInt(pos, i);
            }

            writer.writeBlock(memSeg);
            memSeg = writer.getNextReturnedBlock();
        }

        writer.close();

        final BlockChannelReader<MemorySegment> reader =
                this.ioManager.createBlockChannelReader(channelID);
        for (int i = 0; i < NUM_IOS; i++) {
            reader.readBlock(memSeg);
            memSeg = reader.getNextReturnedBlock();

            for (int pos = 0; pos < memSeg.size(); pos += 4) {
                assertThat(memSeg.getInt(pos))
                        .withFailMessage("Read memory segment contains invalid data.")
                        .isEqualTo(i);
            }
        }

        reader.closeAndDelete();
    }

    @Test
    void channelReadWriteMultipleSegments() throws Exception {
        final int NUM_IOS = 1111;
        final int NUM_SEGS = 16;

        final List<MemorySegment> memSegs = new ArrayList<MemorySegment>();
        for (int i = 0; i < NUM_SEGS; i++) {
            memSegs.add(MemorySegmentFactory.allocateUnpooledSegment(32 * 1024));
        }

        final FileIOChannel.ID channelID = this.ioManager.createChannel();
        final BlockChannelWriter<MemorySegment> writer =
                this.ioManager.createBlockChannelWriter(channelID);

        for (int i = 0; i < NUM_IOS; i++) {
            final MemorySegment memSeg =
                    memSegs.isEmpty()
                            ? writer.getNextReturnedBlock()
                            : memSegs.remove(memSegs.size() - 1);

            for (int pos = 0; pos < memSeg.size(); pos += 4) {
                memSeg.putInt(pos, i);
            }

            writer.writeBlock(memSeg);
        }
        writer.close();

        // get back the memory
        while (memSegs.size() < NUM_SEGS) {
            memSegs.add(writer.getNextReturnedBlock());
        }

        final BlockChannelReader<MemorySegment> reader =
                this.ioManager.createBlockChannelReader(channelID);
        while (!memSegs.isEmpty()) {
            reader.readBlock(memSegs.remove(0));
        }

        for (int i = 0; i < NUM_IOS; i++) {
            final MemorySegment memSeg = reader.getNextReturnedBlock();

            for (int pos = 0; pos < memSeg.size(); pos += 4) {
                assertThat(memSeg.getInt(pos))
                        .withFailMessage("Read memory segment contains invalid data.")
                        .isEqualTo(i);
            }
            reader.readBlock(memSeg);
        }

        reader.closeAndDelete();

        // get back the memory
        while (memSegs.size() < NUM_SEGS) {
            memSegs.add(reader.getNextReturnedBlock());
        }
    }

    @Test
    void testExceptionPropagationReader() throws Exception {
        // use atomic boolean as a boolean reference
        final AtomicBoolean handlerCalled = new AtomicBoolean();
        final AtomicBoolean exceptionForwarded = new AtomicBoolean();

        ReadRequest req =
                new ReadRequest() {

                    @Override
                    public void requestDone(IOException ioex) {
                        if (ioex instanceof TestIOException) {
                            exceptionForwarded.set(true);
                        }

                        synchronized (handlerCalled) {
                            handlerCalled.set(true);
                            handlerCalled.notifyAll();
                        }
                    }

                    @Override
                    public void read() throws IOException {
                        throw new TestIOException();
                    }
                };

        // test the read queue
        RequestQueue<ReadRequest> rq = ioManager.getReadRequestQueue(ioManager.createChannel());
        rq.add(req);

        // wait until the asynchronous request has been handled
        synchronized (handlerCalled) {
            while (!handlerCalled.get()) {
                handlerCalled.wait();
            }
        }

        assertThat(exceptionForwarded).isTrue();
    }

    @Test
    void testExceptionPropagationWriter() throws Exception {
        // use atomic boolean as a boolean reference
        final AtomicBoolean handlerCalled = new AtomicBoolean();
        final AtomicBoolean exceptionForwarded = new AtomicBoolean();

        WriteRequest req =
                new WriteRequest() {

                    @Override
                    public void requestDone(IOException ioex) {
                        if (ioex instanceof TestIOException) {
                            exceptionForwarded.set(true);
                        }

                        synchronized (handlerCalled) {
                            handlerCalled.set(true);
                            handlerCalled.notifyAll();
                        }
                    }

                    @Override
                    public void write() throws IOException {
                        throw new TestIOException();
                    }
                };

        // test the read queue
        RequestQueue<WriteRequest> rq = ioManager.getWriteRequestQueue(ioManager.createChannel());
        rq.add(req);

        // wait until the asynchronous request has been handled
        synchronized (handlerCalled) {
            while (!handlerCalled.get()) {
                handlerCalled.wait();
            }
        }

        assertThat(exceptionForwarded).isTrue();
    }

    @Test
    void testExceptionInCallbackRead() throws Exception {
        final AtomicBoolean handlerCalled = new AtomicBoolean();

        ReadRequest regularRequest =
                new ReadRequest() {

                    @Override
                    public void requestDone(IOException ioex) {
                        synchronized (handlerCalled) {
                            handlerCalled.set(true);
                            handlerCalled.notifyAll();
                        }
                    }

                    @Override
                    public void read() {}
                };

        ReadRequest exceptionThrower =
                new ReadRequest() {

                    @Override
                    public void requestDone(IOException ioex) {
                        throw new RuntimeException();
                    }

                    @Override
                    public void read() {}
                };

        RequestQueue<ReadRequest> rq = ioManager.getReadRequestQueue(ioManager.createChannel());

        // queue first an exception thrower, then a regular request.
        // we check that the regular request gets successfully handled
        rq.add(exceptionThrower);
        rq.add(regularRequest);

        synchronized (handlerCalled) {
            while (!handlerCalled.get()) {
                handlerCalled.wait();
            }
        }
    }

    @Test
    void testExceptionInCallbackWrite() throws Exception {
        final AtomicBoolean handlerCalled = new AtomicBoolean();

        WriteRequest regularRequest =
                new WriteRequest() {

                    @Override
                    public void requestDone(IOException ioex) {
                        synchronized (handlerCalled) {
                            handlerCalled.set(true);
                            handlerCalled.notifyAll();
                        }
                    }

                    @Override
                    public void write() {}
                };

        WriteRequest exceptionThrower =
                new WriteRequest() {

                    @Override
                    public void requestDone(IOException ioex) {
                        throw new RuntimeException();
                    }

                    @Override
                    public void write() {}
                };

        RequestQueue<WriteRequest> rq = ioManager.getWriteRequestQueue(ioManager.createChannel());

        // queue first an exception thrower, then a regular request.
        // we check that the regular request gets successfully handled
        rq.add(exceptionThrower);
        rq.add(regularRequest);

        synchronized (handlerCalled) {
            while (!handlerCalled.get()) {
                handlerCalled.wait();
            }
        }
    }

    private static final class TestIOException extends IOException {
        private static final long serialVersionUID = -814705441998024472L;
    }
}
