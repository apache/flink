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

package org.apache.flink.runtime.io.network.api.writer;

import org.apache.flink.runtime.plugable.SerializationDelegate;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * An abstract record-oriented runtime result writer, used in Stream Pipeline mode.
 *
 * <p>The RecordWriter wraps the runtime's {@link ResultPartitionWriter} and takes care of channel
 * selection.
 *
 * @param <T> the type of the record that can be emitted with this record writer, it's usually of
 *     type StreamElement, such as StreamRecord, Watermark
 */
public abstract class StreamRecordWriter<T> extends RecordWriter<SerializationDelegate<T>> {

    StreamRecordWriter(ResultPartitionWriter writer, long timeout, String taskName) {
        super(writer, timeout, taskName);
    }

    @Override
    protected void emit(SerializationDelegate<T> record, int targetSubpartition)
            throws IOException {
        checkErroneous();

        targetPartition.emitRecord(record, targetSubpartition);

        if (flushAlways) {
            targetPartition.flush(targetSubpartition);
        }
    }

    @Override
    public void emit(
            SerializationDelegate<T> record, ByteBuffer recordBuffer, int targetSubpartition)
            throws IOException {
        checkErroneous();

        targetPartition.emitRecord(record, recordBuffer, targetSubpartition);

        if (flushAlways) {
            targetPartition.flush(targetSubpartition);
        }
    }

    @Override
    public void randomEmit(SerializationDelegate<T> record) throws IOException {
        checkErroneous();

        int targetSubpartition = rng.nextInt(numberOfChannels);
        emit(record, targetSubpartition);
    }
}
