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

package org.apache.flink.connector.base.source.hybrid;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/** The {@link SimpleVersionedSerializer Serializer} for the enumerator state. */
public class HybridSourceEnumeratorStateSerializer
        implements SimpleVersionedSerializer<HybridSourceEnumeratorState> {

    private static final int CURRENT_VERSION = 0;

    public HybridSourceEnumeratorStateSerializer() {}

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(HybridSourceEnumeratorState enumState) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(baos)) {
            out.writeInt(enumState.getCurrentSourceIndex());
            out.writeInt(enumState.getWrappedStateSerializerVersion());
            out.writeInt(enumState.getWrappedState().length);
            out.write(enumState.getWrappedState());
            out.flush();
            return baos.toByteArray();
        }
    }

    @Override
    public HybridSourceEnumeratorState deserialize(int version, byte[] serialized)
            throws IOException {
        if (version == 0) {
            return deserializeV0(serialized);
        }
        throw new IOException(
                String.format(
                        "The bytes are serialized with version %d, "
                                + "while this deserializer only supports version up to %d",
                        version, CURRENT_VERSION));
    }

    private HybridSourceEnumeratorState deserializeV0(byte[] serialized) throws IOException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                DataInputStream in = new DataInputStream(bais)) {
            int sourceIndex = in.readInt();
            int nestedVersion = in.readInt();
            int length = in.readInt();
            byte[] nestedBytes = new byte[length];
            in.readFully(nestedBytes);
            return new HybridSourceEnumeratorState(sourceIndex, nestedBytes, nestedVersion);
        }
    }
}
