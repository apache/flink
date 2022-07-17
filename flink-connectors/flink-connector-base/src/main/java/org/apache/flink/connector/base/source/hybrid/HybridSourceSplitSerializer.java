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

    private static final int VERSION_0 = 0;
    private static final int VERSION_1 = 1;

    private static final int CURRENT_VERSION = VERSION_1;

    public HybridSourceSplitSerializer() {}

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
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
            out.writeBoolean(split.isFinished);
            out.flush();
            return baos.toByteArray();
        }
    }

    @Override
    public HybridSourceSplit deserialize(int version, byte[] serialized) throws IOException {
        switch (version) {
            case VERSION_0:
                return deserializeV0or1(serialized, false);
            case VERSION_1:
                return deserializeV0or1(serialized, true);
            default:
                throw new IOException(String.format("Invalid version %d", version));
        }
    }

    private HybridSourceSplit deserializeV0or1(byte[] serialized, boolean isVersion1)
            throws IOException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                DataInputStream in = new DataInputStream(bais)) {
            int sourceIndex = in.readInt();
            String splitId = in.readUTF();
            int nestedVersion = in.readInt();
            int length = in.readInt();
            byte[] splitBytes = new byte[length];
            in.readFully(splitBytes);
            // isFinished is default to false for version 0 split
            boolean isFinished = isVersion1 && in.readBoolean();
            return new HybridSourceSplit(
                    sourceIndex, splitBytes, nestedVersion, splitId, isFinished);
        }
    }
}
