/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.base.sink;

import org.apache.flink.connector.base.sink.writer.AsyncSinkWriter;
import org.apache.flink.connector.base.sink.writer.AsyncSinkWriterStateSerializer;
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.config.AsyncSinkWriterConfiguration;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

/** Dummy destination that records write events. */
public class ArrayListAsyncSink extends AsyncSinkBase<String, Integer> {

    public ArrayListAsyncSink() {
        this(25, 1, 100, 100_000, 1000, 100_000);
    }

    public ArrayListAsyncSink(
            int maxBatchSize,
            int maxInFlightRequests,
            int maxBufferedRequests,
            long maxBatchSizeInBytes,
            long maxTimeInBufferMS,
            long maxRecordSizeInBytes) {
        super(
                (element, x) -> Integer.parseInt(element),
                maxBatchSize,
                maxInFlightRequests,
                maxBufferedRequests,
                maxBatchSizeInBytes,
                maxTimeInBufferMS,
                maxRecordSizeInBytes);
    }

    @Override
    public StatefulSinkWriter<String, BufferedRequestState<Integer>> createWriter(
            InitContext context) throws IOException {
        return new AsyncSinkWriter<String, Integer>(
                getElementConverter(),
                context,
                AsyncSinkWriterConfiguration.builder()
                        .setMaxBatchSize(getMaxBatchSize())
                        .setMaxBatchSizeInBytes(getMaxBatchSizeInBytes())
                        .setMaxInFlightRequests(getMaxInFlightRequests())
                        .setMaxBufferedRequests(getMaxBufferedRequests())
                        .setMaxTimeInBufferMS(getMaxTimeInBufferMS())
                        .setMaxRecordSizeInBytes(getMaxRecordSizeInBytes())
                        .build(),
                Collections.emptyList()) {

            @Override
            protected void submitRequestEntries(
                    List<Integer> requestEntries, Consumer<List<Integer>> requestResult) {
                try {
                    ArrayListDestination.putRecords(requestEntries);
                } catch (RuntimeException e) {
                    getFatalExceptionCons().accept(e);
                }
                requestResult.accept(Arrays.asList());
            }

            @Override
            protected long getSizeInBytes(Integer requestEntry) {
                return 4;
            }
        };
    }

    @Override
    public StatefulSinkWriter<String, BufferedRequestState<Integer>> restoreWriter(
            InitContext context, Collection<BufferedRequestState<Integer>> recoveredState)
            throws IOException {
        return createWriter(context);
    }

    @Override
    public SimpleVersionedSerializer<BufferedRequestState<Integer>> getWriterStateSerializer() {
        return new AsyncSinkWriterStateSerializer<Integer>() {
            @Override
            protected void serializeRequestToStream(Integer request, DataOutputStream out)
                    throws IOException {
                out.writeInt(request);
            }

            @Override
            protected Integer deserializeRequestFromStream(long requestSize, DataInputStream in)
                    throws IOException {
                return in.readInt();
            }

            @Override
            public int getVersion() {
                return 0;
            }
        };
    }
}
