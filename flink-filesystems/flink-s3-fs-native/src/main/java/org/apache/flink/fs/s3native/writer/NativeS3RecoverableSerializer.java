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

package org.apache.flink.fs.s3native.writer;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.fs.s3native.writer.NativeS3Recoverable.PartETag;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Serializer for {@link NativeS3Recoverable} state used during checkpointing and recovery.
 *
 * <p><b>Schema Evolution Strategy:</b> This serializer uses an explicit versioning scheme via
 * {@link SimpleVersionedSerializer}. The approach is intentionally simple (version-based binary
 * format) rather than using schema evolution frameworks like Avro or Protobuf because:
 *
 * <ul>
 *   <li>The recoverable state is relatively simple and stable
 *   <li>Changes are infrequent and typically additive
 *   <li>The format is internal to the filesystem implementation
 *   <li>Minimizes external dependencies
 * </ul>
 *
 * <p><b>Version Compatibility Rules:</b>
 *
 * <ul>
 *   <li>New fields should be added at the end of the serialized form
 *   <li>Use optional markers (like the boolean flag for incomplete parts) for nullable fields
 *   <li>When bumping the version, add handling for previous versions in {@link #deserialize}
 *   <li>Never remove or reorder existing fields in the current version
 * </ul>
 *
 * <p><b>Current Format (Version 1):</b>
 *
 * <pre>
 * - objectName: UTF string
 * - uploadId: UTF string
 * - numBytesInParts: long
 * - partsCount: int
 * - parts[]: { partNumber: int, eTag: UTF string } repeated partsCount times
 * - hasIncompletePart: boolean
 * - if hasIncompletePart:
 *   - incompleteObjectName: UTF string
 *   - incompleteObjectLength: long
 * </pre>
 */
public class NativeS3RecoverableSerializer
        implements SimpleVersionedSerializer<NativeS3Recoverable> {

    public static final NativeS3RecoverableSerializer INSTANCE =
            new NativeS3RecoverableSerializer();

    private static final int CURRENT_VERSION = 1;

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(NativeS3Recoverable recoverable) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(outputStream);
        out.writeUTF(recoverable.getObjectName());
        out.writeUTF(recoverable.uploadId());
        out.writeLong(recoverable.numBytesInParts());
        List<PartETag> parts = recoverable.parts();
        out.writeInt(parts.size());
        for (PartETag part : parts) {
            out.writeInt(part.getPartNumber());
            out.writeUTF(part.getETag());
        }
        String incompleteObject = recoverable.incompleteObjectName();
        if (incompleteObject == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeUTF(incompleteObject);
            out.writeLong(recoverable.incompleteObjectLength());
        }
        out.flush();
        return outputStream.toByteArray();
    }

    @Override
    public NativeS3Recoverable deserialize(int version, byte[] serialized) throws IOException {
        if (version != CURRENT_VERSION) {
            throw new IOException("Unsupported version: " + version);
        }
        ByteArrayInputStream inputStream = new ByteArrayInputStream(serialized);
        DataInputStream in = new DataInputStream(inputStream);
        String objectName = in.readUTF();
        String uploadId = in.readUTF();
        long numBytesInParts = in.readLong();
        int numParts = in.readInt();
        List<PartETag> parts = new ArrayList<>(numParts);
        for (int i = 0; i < numParts; i++) {
            int partNumber = in.readInt();
            String eTag = in.readUTF();
            parts.add(new PartETag(partNumber, eTag));
        }
        boolean hasIncompletePart = in.readBoolean();
        String incompleteObjectName = null;
        long incompleteObjectLength = -1;
        if (hasIncompletePart) {
            incompleteObjectName = in.readUTF();
            incompleteObjectLength = in.readLong();
        }
        return new NativeS3Recoverable(
                objectName,
                uploadId,
                parts,
                numBytesInParts,
                incompleteObjectName,
                incompleteObjectLength);
    }
}
