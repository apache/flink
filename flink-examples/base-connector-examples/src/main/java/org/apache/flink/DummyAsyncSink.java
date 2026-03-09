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

package org.apache.flink;

import org.apache.flink.api.connector.sink2.StatefulSinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.connector.base.sink.AsyncSinkBase;
import org.apache.flink.connector.base.sink.writer.AsyncSinkWriter;
import org.apache.flink.connector.base.sink.writer.AsyncSinkWriterStateSerializer;
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.ResultHandler;
import org.apache.flink.connector.base.sink.writer.config.AsyncSinkWriterConfiguration;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class DummyAsyncSink extends AsyncSinkBase<String, String> {

    private final TokenBucketRateLimitingStrategy rateLimitingStrategy;

    public DummyAsyncSink(TokenBucketRateLimitingStrategy rateLimitingStrategy) {
        super((element, context) -> element, 10, 1, 100, 1024 * 1024, 1000, 1024);
        this.rateLimitingStrategy = rateLimitingStrategy;
    }

    @Override
    public StatefulSinkWriter<String, BufferedRequestState<String>> createWriter(
            WriterInitContext context) throws IOException {
        return new AsyncSinkWriter<String, String>(
                getElementConverter(),
                context,
                AsyncSinkWriterConfiguration.builder()
                        .setMaxBatchSize(getMaxBatchSize())
                        .setMaxBatchSizeInBytes(getMaxBatchSizeInBytes())
                        .setMaxInFlightRequests(getMaxInFlightRequests())
                        .setMaxBufferedRequests(getMaxBufferedRequests())
                        .setMaxTimeInBufferMS(getMaxTimeInBufferMS())
                        .setMaxRecordSizeInBytes(getMaxRecordSizeInBytes())
                        .setRateLimitingStrategy(rateLimitingStrategy)
                        .build(),
                Collections.emptyList()) {

            @Override
            protected void submitRequestEntries(
                    List<String> requestEntries, ResultHandler<String> resultHandler) {
                // Simulate async request - complete immediately
                resultHandler.complete();
            }

            @Override
            protected long getSizeInBytes(String requestEntry) {
                return requestEntry.length();
            }
        };
    }

    @Override
    public StatefulSinkWriter<String, BufferedRequestState<String>> restoreWriter(
            WriterInitContext context, Collection<BufferedRequestState<String>> recoveredState)
            throws IOException {
        return createWriter(context);
    }

    @Override
    public SimpleVersionedSerializer<BufferedRequestState<String>> getWriterStateSerializer() {
        return new AsyncSinkWriterStateSerializer<String>() {
            @Override
            protected void serializeRequestToStream(String request, DataOutputStream out)
                    throws IOException {
                out.writeUTF(request);
            }

            @Override
            protected String deserializeRequestFromStream(long requestSize, DataInputStream in)
                    throws IOException {
                return in.readUTF();
            }

            @Override
            public int getVersion() {
                return 1;
            }
        };
    }
}
