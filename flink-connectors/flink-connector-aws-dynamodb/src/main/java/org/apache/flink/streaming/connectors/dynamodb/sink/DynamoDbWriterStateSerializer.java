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

package org.apache.flink.streaming.connectors.dynamodb.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.RequestEntryWrapper;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/** A serializer used to serialize a collection of {@link DynamoDbWriteRequest}. */
@Internal
public class DynamoDbWriterStateSerializer
        implements SimpleVersionedSerializer<BufferedRequestState<DynamoDbWriteRequest>> {
    private static final long DATA_IDENTIFIER = -2;

    @Override
    public int getVersion() {
        return 1;
    }

    @Override
    public byte[] serialize(BufferedRequestState<DynamoDbWriteRequest> obj) throws IOException {
        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
                final ObjectOutputStream out = new ObjectOutputStream(baos)) {
            Collection<RequestEntryWrapper<DynamoDbWriteRequest>> bufferState =
                    obj.getBufferedRequestEntries();

            out.writeLong(DATA_IDENTIFIER);
            out.writeInt(bufferState.size());
            for (RequestEntryWrapper<DynamoDbWriteRequest> wrapper : bufferState) {
                out.writeLong(wrapper.getSize());
                out.writeObject(wrapper.getRequestEntry());
            }
            // out.flush();
            return baos.toByteArray();
        }
    }

    @Override
    public BufferedRequestState<DynamoDbWriteRequest> deserialize(int version, byte[] serialized)
            throws IOException {
        try (final ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                final ObjectInputStream in = new ObjectInputStream(bais)) {

            validateIdentifier(in);
            int size = in.readInt();
            List<RequestEntryWrapper<DynamoDbWriteRequest>> serializedState = new ArrayList<>();

            for (int i = 0; i < size; i++) {
                long requestSize = in.readLong();
                DynamoDbWriteRequest request = (DynamoDbWriteRequest) in.readObject();
                serializedState.add(new RequestEntryWrapper<>(request, requestSize));
            }

            return new BufferedRequestState<>(serializedState);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    private void validateIdentifier(ObjectInputStream in) throws IOException {
        if (in.readLong() != DATA_IDENTIFIER) {
            throw new IllegalStateException("Corrupted data to deserialize");
        }
    }
}
