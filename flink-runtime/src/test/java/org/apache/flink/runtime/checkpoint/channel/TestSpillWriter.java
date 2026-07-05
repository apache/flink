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

import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptor;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;

/**
 * Test-only helper that produces spill files in the {@link AbstractSpillingHandler} on-disk format,
 * exposing the same {@code writeRecord} / {@code writePassThrough} surface the readers and drainers
 * are tested against. It drives a minimal concrete {@link AbstractSpillingHandler} so tests need
 * not stand up real input gates or run the recovery loop.
 */
final class TestSpillWriter implements Closeable {

    private final FormatHandler handler;

    TestSpillWriter(Path baseDir) {
        this(baseDir, AbstractSpillingHandler.DEFAULT_SPILL_FILE_SIZE_BYTES);
    }

    TestSpillWriter(Path baseDir, long maxFileSizeBytes) {
        this.handler = new FormatHandler(new String[] {baseDir.toString()}, maxFileSizeBytes);
    }

    /** Appends one length-prefixed record, mirroring the filtering path. */
    void writeRecord(InputChannelInfo channelInfo, byte[] record, int recordLength)
            throws IOException {
        DataOutputSerializer segment = handler.segmentSerializerFor(channelInfo);
        segment.writeInt(recordLength);
        segment.write(record, 0, recordLength);
    }

    /** Appends verbatim bytes, mirroring the pass-through path. */
    void writePassThrough(InputChannelInfo channelInfo, byte[] data, int offset, int length)
            throws IOException {
        handler.segmentSerializerFor(channelInfo).write(data, offset, length);
    }

    /** Opens (or switches to) a segment without writing any body, to exercise empty segments. */
    void openSegment(InputChannelInfo channelInfo) throws IOException {
        handler.segmentSerializerFor(channelInfo);
    }

    /**
     * Seals the spilled segments and returns the produced state, already holding one lifecycle
     * grant for the caller. Returns {@code null} if nothing was ever written.
     */
    FetchedChannelState getChannelState() throws IOException {
        handler.close();
        return handler.getProducedChannelState();
    }

    @Override
    public void close() throws IOException {
        handler.close();
    }

    private static final class FormatHandler extends AbstractSpillingHandler {

        FormatHandler(String[] spillTmpDirectories, long maxFileSizeBytes) {
            super(
                    new InputGate[0],
                    InflightDataRescalingDescriptor.NO_RESCALE,
                    spillTmpDirectories,
                    maxFileSizeBytes);
        }

        @Override
        public void recover(
                InputChannelInfo info,
                int oldSubtaskIndex,
                BufferWithContext<org.apache.flink.runtime.io.network.buffer.Buffer> ctx) {
            throw new UnsupportedOperationException("not used in format tests");
        }
    }
}
