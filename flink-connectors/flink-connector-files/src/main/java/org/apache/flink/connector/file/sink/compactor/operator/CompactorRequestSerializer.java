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

package org.apache.flink.connector.file.sink.compactor.operator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.file.sink.FileSinkCommittable;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputSerializer;

import java.io.IOException;
import java.util.List;

/** Versioned serializer for {@link CompactorRequest}. */
@Internal
public class CompactorRequestSerializer implements SimpleVersionedSerializer<CompactorRequest> {

    private final SimpleVersionedSerializer<FileSinkCommittable> committableSerializer;

    private static final int MAGIC_NUMBER = 0x2fc61e19;

    public CompactorRequestSerializer(
            SimpleVersionedSerializer<FileSinkCommittable> committableSerializer) {
        this.committableSerializer = committableSerializer;
    }

    @Override
    public int getVersion() {
        return 1;
    }

    @Override
    public byte[] serialize(CompactorRequest request) throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(256);
        out.writeInt(MAGIC_NUMBER);
        serializeV1(request, out);
        return out.getCopyOfBuffer();
    }

    @Override
    public CompactorRequest deserialize(int version, byte[] serialized) throws IOException {
        DataInputDeserializer in = new DataInputDeserializer(serialized);

        switch (version) {
            case 1:
                validateMagicNumber(in);
                return deserializeV1(in);
            default:
                throw new IOException("Unrecognized version or corrupt state: " + version);
        }
    }

    private void serializeV1(CompactorRequest request, DataOutputSerializer out)
            throws IOException {
        out.writeUTF(request.getBucketId());
        SimpleVersionedSerialization.writeVersionAndSerializeList(
                committableSerializer, request.getCommittableToCompact(), out);
        SimpleVersionedSerialization.writeVersionAndSerializeList(
                committableSerializer, request.getCommittableToPassthrough(), out);
    }

    private CompactorRequest deserializeV1(DataInputDeserializer in) throws IOException {
        String bucketId = in.readUTF();
        List<FileSinkCommittable> committableToCompact =
                SimpleVersionedSerialization.readVersionAndDeserializeList(
                        committableSerializer, in);
        List<FileSinkCommittable> committableToPassthrough =
                SimpleVersionedSerialization.readVersionAndDeserializeList(
                        committableSerializer, in);
        return new CompactorRequest(bucketId, committableToCompact, committableToPassthrough);
    }

    private static void validateMagicNumber(DataInputView in) throws IOException {
        int magicNumber = in.readInt();
        if (magicNumber != MAGIC_NUMBER) {
            throw new IOException(
                    String.format("Corrupt data: Unexpected magic number %08X", magicNumber));
        }
    }
}
