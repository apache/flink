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

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.memory.MemoryManagerBuilder;
import org.apache.flink.runtime.operators.testutils.DummyInvokable;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/** Integration test case for the I/O manager. */
class IOManagerITCase {

    private static final long SEED = 649180756312423613L;

    private static final int MAXIMUM_NUMBER_OF_SEGMENTS_PER_CHANNEL = 10;

    private static final int MEMORY_SIZE = 10 * 1024 * 1024; // 10 MB

    private final int NUM_CHANNELS = 29;

    private final int NUMBERS_TO_BE_WRITTEN = NUM_CHANNELS * 1000000;

    private IOManager ioManager;

    private MemoryManager memoryManager;

    @BeforeEach
    void beforeTest() {
        memoryManager = MemoryManagerBuilder.newBuilder().setMemorySize(MEMORY_SIZE).build();
        ioManager = new IOManagerAsync();
    }

    @AfterEach
    void afterTest() throws Exception {
        ioManager.close();

        assertThat(memoryManager.verifyEmpty())
                .withFailMessage("Not all memory was returned to the memory manager in the test.")
                .isTrue();
        memoryManager.shutdown();
        memoryManager = null;
    }

    // ------------------------------------------------------------------------

    /**
     * This test instantiates multiple channels and writes to them in parallel and re-reads the data
     * in parallel. It is designed to check the ability of the IO manager to correctly handle
     * multiple threads.
     */
    @Test
    @SuppressWarnings("unchecked")
    void parallelChannelsTest() throws Exception {
        final Random rnd = new Random(SEED);
        final AbstractInvokable memOwner = new DummyInvokable();

        FileIOChannel.ID[] ids = new FileIOChannel.ID[NUM_CHANNELS];
        BlockChannelWriter<MemorySegment>[] writers = new BlockChannelWriter[NUM_CHANNELS];
        BlockChannelReader<MemorySegment>[] readers = new BlockChannelReader[NUM_CHANNELS];
        ChannelWriterOutputView[] outs = new ChannelWriterOutputView[NUM_CHANNELS];
        ChannelReaderInputView[] ins = new ChannelReaderInputView[NUM_CHANNELS];

        int[] writingCounters = new int[NUM_CHANNELS];
        int[] readingCounters = new int[NUM_CHANNELS];

        // instantiate the channels and writers
        for (int i = 0; i < NUM_CHANNELS; i++) {
            ids[i] = this.ioManager.createChannel();
            writers[i] = this.ioManager.createBlockChannelWriter(ids[i]);

            List<MemorySegment> memSegs =
                    this.memoryManager.allocatePages(
                            memOwner, rnd.nextInt(MAXIMUM_NUMBER_OF_SEGMENTS_PER_CHANNEL - 1) + 1);
            outs[i] =
                    new ChannelWriterOutputView(
                            writers[i], memSegs, this.memoryManager.getPageSize());
        }

        Value val = new Value();

        // write a lot of values unevenly distributed over the channels

        for (int i = 0; i < NUMBERS_TO_BE_WRITTEN; i++) {
            int channel = skewedSample(rnd, NUM_CHANNELS - 1);

            val.value = String.valueOf(writingCounters[channel]++);
            val.write(outs[channel]);
        }

        // close all writers
        for (int i = 0; i < NUM_CHANNELS; i++) {
            this.memoryManager.release(outs[i].close());
        }
        outs = null;
        writers = null;

        // instantiate the readers for sequential read
        for (int i = 0; i < NUM_CHANNELS; i++) {

            List<MemorySegment> memSegs =
                    this.memoryManager.allocatePages(
                            memOwner, rnd.nextInt(MAXIMUM_NUMBER_OF_SEGMENTS_PER_CHANNEL - 1) + 1);

            final BlockChannelReader<MemorySegment> reader =
                    this.ioManager.createBlockChannelReader(ids[i]);
            final ChannelReaderInputView in = new ChannelReaderInputView(reader, memSegs, false);
            int nextVal = 0;

            try {
                while (true) {
                    val.read(in);
                    int intValue = Integer.parseInt(val.value);

                    assertThat(intValue)
                            .withFailMessage(
                                    "Written and read values do not match during sequential read.")
                            .isEqualTo(nextVal);
                    nextVal++;
                }
            } catch (EOFException eofex) {
                // expected
            }

            assertThat(nextVal)
                    .withFailMessage(
                            "NUmber of written numbers differs from number of read numbers.")
                    .isEqualTo(writingCounters[i]);

            this.memoryManager.release(in.close());
        }

        // instantiate the readers
        for (int i = 0; i < NUM_CHANNELS; i++) {

            List<MemorySegment> memSegs =
                    this.memoryManager.allocatePages(
                            memOwner, rnd.nextInt(MAXIMUM_NUMBER_OF_SEGMENTS_PER_CHANNEL - 1) + 1);

            readers[i] = this.ioManager.createBlockChannelReader(ids[i]);
            ins[i] = new ChannelReaderInputView(readers[i], memSegs, false);
        }

        // read a lot of values in a mixed order from the channels
        for (int i = 0; i < NUMBERS_TO_BE_WRITTEN; i++) {

            while (true) {
                final int channel = skewedSample(rnd, NUM_CHANNELS - 1);
                if (ins[channel] != null) {
                    try {
                        val.read(ins[channel]);
                        int intValue = Integer.parseInt(val.value);

                        assertThat(intValue)
                                .withFailMessage("Written and read values do not match.")
                                .isEqualTo(readingCounters[channel]++);

                        break;
                    } catch (EOFException eofex) {
                        this.memoryManager.release(ins[channel].close());
                        ins[channel] = null;
                    }
                }
            }
        }

        // close all readers
        for (int i = 0; i < NUM_CHANNELS; i++) {
            if (ins[i] != null) {
                this.memoryManager.release(ins[i].close());
            }
            readers[i].closeAndDelete();
        }

        ins = null;
        readers = null;

        // check that files are deleted
        for (int i = 0; i < NUM_CHANNELS; i++) {
            File f = new File(ids[i].getPath());
            assertThat(f).withFailMessage("Channel file has not been deleted.").doesNotExist();
        }
    }

    private static int skewedSample(Random rnd, int max) {
        double uniform = rnd.nextDouble();
        double var = Math.pow(uniform, 8.0);
        double pareto = 0.2 / var;

        int val = (int) pareto;
        return val > max ? val % max : val;
    }

    // ------------------------------------------------------------------------

    protected static class Value implements IOReadableWritable {

        private String value;

        public Value() {}

        public Value(String val) {
            this.value = val;
        }

        @Override
        public void read(DataInputView in) throws IOException {
            value = in.readUTF();
        }

        @Override
        public void write(DataOutputView out) throws IOException {
            out.writeUTF(this.value);
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + ((value == null) ? 0 : value.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            Value other = (Value) obj;

            if (value == null) {
                if (other.value != null) {
                    return false;
                }
            } else if (!value.equals(other.value)) {
                return false;
            }
            return true;
        }
    }
}
