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

import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.util.Preconditions;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/** Serializes splits by delegating to the source-indexed underlying split serializer. */
public class HybridSourceSplitSerializer implements SimpleVersionedSerializer<HybridSourceSplit> {

    final Map<Integer, SimpleVersionedSerializer<SourceSplit>> cachedSerializers;
    final Map<Integer, Source> switchedSources;

    public HybridSourceSplitSerializer(Map<Integer, Source> switchedSources) {
        this.cachedSerializers = new HashMap<>();
        this.switchedSources = switchedSources;
    }

    @Override
    public int getVersion() {
        return 0;
    }

    @Override
    public byte[] serialize(HybridSourceSplit split) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(baos)) {
            out.writeInt(split.sourceIndex());
            out.writeInt(serializerOf(split.sourceIndex()).getVersion());
            byte[] serializedSplit =
                    serializerOf(split.sourceIndex()).serialize(split.getWrappedSplit());
            out.writeInt(serializedSplit.length);
            out.write(serializedSplit);
            return baos.toByteArray();
        }
    }

    @Override
    public HybridSourceSplit deserialize(int version, byte[] serialized) throws IOException {
        if (version == 0) {
            return deserializeV0(serialized);
        }
        throw new AssertionError(String.format("Invalid version %d", version));
    }

    private HybridSourceSplit deserializeV0(byte[] serialized) throws IOException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                DataInputStream in = new DataInputStream(bais)) {
            int sourceIndex = in.readInt();
            int nestedVersion = in.readInt();
            int length = in.readInt();
            byte[] splitBytes = new byte[length];
            in.readFully(splitBytes);
            SourceSplit split = serializerOf(sourceIndex).deserialize(nestedVersion, splitBytes);
            return new HybridSourceSplit(sourceIndex, split);
        }
    }

    private SimpleVersionedSerializer<SourceSplit> serializerOf(int sourceIndex) {
        return cachedSerializers.computeIfAbsent(
                sourceIndex,
                (k -> {
                    Source source =
                            Preconditions.checkNotNull(
                                    switchedSources.get(k),
                                    "Source for index=%s not available",
                                    sourceIndex);
                    return source.getSplitSerializer();
                }));
    }
}
