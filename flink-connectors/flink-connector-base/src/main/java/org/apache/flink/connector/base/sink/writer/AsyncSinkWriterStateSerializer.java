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

package org.apache.flink.connector.base.sink.writer;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Serializer class for {@link AsyncSinkWriter} state.
 *
 * @param <RequestEntryT> Writer Request Entry type
 */
@PublicEvolving
public abstract class AsyncSinkWriterStateSerializer<RequestEntryT extends Serializable>
        implements SimpleVersionedSerializer<BufferedRequestState<RequestEntryT>> {
    private static final long DATA_IDENTIFIER = -1;

    /**
     * Serializes state in form of
     * [DATA_IDENTIFIER,NUM_OF_ELEMENTS,SIZE1,REQUEST1,SIZE2,REQUEST2....].
     */
    @Override
    public byte[] serialize(BufferedRequestState<RequestEntryT> obj) throws IOException {
        Collection<RequestEntryWrapper<RequestEntryT>> bufferState =
                obj.getBufferedRequestEntries();

        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
                final DataOutputStream out = new DataOutputStream(baos)) {

            out.writeLong(DATA_IDENTIFIER);
            out.writeInt(bufferState.size());

            for (RequestEntryWrapper<RequestEntryT> wrapper : bufferState) {
                out.writeLong(wrapper.getSize());
                serializeRequestToStream(wrapper.getRequestEntry(), out);
            }

            return baos.toByteArray();
        }
    }

    @Override
    public BufferedRequestState<RequestEntryT> deserialize(int version, byte[] serialized)
            throws IOException {
        try (final ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                final DataInputStream in = new DataInputStream(bais)) {

            validateIdentifier(in);

            int size = in.readInt();
            List<RequestEntryWrapper<RequestEntryT>> serializedState = new ArrayList<>();

            for (int i = 0; i < size; i++) {
                long requestSize = in.readLong();
                RequestEntryT request = deserializeRequestFromStream(requestSize, in);
                serializedState.add(new RequestEntryWrapper<>(request, requestSize));
            }

            return new BufferedRequestState<>(serializedState);
        }
    }

    protected abstract void serializeRequestToStream(RequestEntryT request, DataOutputStream out)
            throws IOException;

    protected abstract RequestEntryT deserializeRequestFromStream(
            long requestSize, DataInputStream in) throws IOException;

    private void validateIdentifier(DataInputStream in) throws IOException {
        if (in.readLong() != DATA_IDENTIFIER) {
            throw new IllegalStateException("Corrupted data to deserialize");
        }
    }
}
