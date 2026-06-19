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

/** Serializes splits by delegating to the source-indexed underlying split serializer. */
public class HybridSourceSplitSerializer implements SimpleVersionedSerializer<HybridSourceSplit> {

    public HybridSourceSplitSerializer() {}

    @Override
    public int getVersion() {
        return 0;
    }

    @Override
    public byte[] serialize(HybridSourceSplit split) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(baos)) {
            out.writeInt(split.sourceIndex());
            out.writeUTF(split.splitId());
            out.writeInt(split.wrappedSplitSerializerVersion());
            out.writeInt(split.wrappedSplitBytes().length);
            out.write(split.wrappedSplitBytes());
            out.flush();
            return baos.toByteArray();
        }
    }

    @Override
    public HybridSourceSplit deserialize(int version, byte[] serialized) throws IOException {
        if (version == 0) {
            return deserializeV0(serialized);
        }
        throw new IOException(String.format("Invalid version %d", version));
    }

    private HybridSourceSplit deserializeV0(byte[] serialized) throws IOException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                DataInputStream in = new DataInputStream(bais)) {
            int sourceIndex = in.readInt();
            String splitId = in.readUTF();
            int nestedVersion = in.readInt();
            int length = in.readInt();
            byte[] splitBytes = new byte[length];
            in.readFully(splitBytes);
            return new HybridSourceSplit(sourceIndex, splitBytes, nestedVersion, splitId);
        }
    }
}
