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

package org.apache.flink.connector.kinesis.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.base.sink.writer.AsyncSinkWriterStateSerializer;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/** Kinesis Streams implementation {@link AsyncSinkWriterStateSerializer}. */
@Internal
public class KinesisStreamsStateSerializer
        extends AsyncSinkWriterStateSerializer<PutRecordsRequestEntry> {
    @Override
    protected void serializeRequestToStream(PutRecordsRequestEntry request, DataOutputStream out)
            throws IOException {
        out.write(request.data().asByteArrayUnsafe());
        serializePartitionKeyToStream(request.partitionKey(), out);
        validateExplicitHashKey(request);
    }

    protected void serializePartitionKeyToStream(String partitionKey, DataOutputStream out)
            throws IOException {
        out.writeInt(partitionKey.length());
        out.write(partitionKey.getBytes(StandardCharsets.UTF_8));
    }

    protected void validateExplicitHashKey(PutRecordsRequestEntry request) {
        if (request.explicitHashKey() != null) {
            throw new IllegalStateException(
                    String.format(
                            "KinesisStreamsStateSerializer is incompatible with ElementConverter."
                                    + "Serializer version %d  does not support explicit hash key.",
                            getVersion()));
        }
    }

    @Override
    protected PutRecordsRequestEntry deserializeRequestFromStream(
            long requestSize, DataInputStream in) throws IOException {
        byte[] requestData = new byte[(int) requestSize];
        in.read(requestData);

        return PutRecordsRequestEntry.builder()
                .data(SdkBytes.fromByteArray(requestData))
                .partitionKey(deserializePartitionKeyFromStream(in))
                .build();
    }

    protected String deserializePartitionKeyFromStream(DataInputStream in) throws IOException {
        int partitionKeyLength = in.readInt();
        byte[] requestPartitionKeyData = new byte[(int) partitionKeyLength];
        in.read(requestPartitionKeyData);
        return new String(requestPartitionKeyData, StandardCharsets.UTF_8);
    }

    @Override
    public int getVersion() {
        return 1;
    }
}
