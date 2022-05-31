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

package org.apache.flink.fs.osshadoop.writer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import com.aliyun.oss.model.PartETag;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

/** Serializer implementation for a {@link OSSRecoverable}. */
@Internal
public class OSSRecoverableSerializer implements SimpleVersionedSerializer<OSSRecoverable> {
    static final OSSRecoverableSerializer INSTANCE = new OSSRecoverableSerializer();

    private static final Charset CHARSET = StandardCharsets.UTF_8;

    private static final int MAGIC_NUMBER = 0x98761234;

    private OSSRecoverableSerializer() {}

    @Override
    public int getVersion() {
        return 1;
    }

    @Override
    public byte[] serialize(OSSRecoverable ossRecoverable) throws IOException {
        final byte[] objectBytes = ossRecoverable.getObjectName().getBytes(CHARSET);
        final byte[] uploadIdBytes = ossRecoverable.getUploadId().getBytes(CHARSET);

        final byte[][] etags = new byte[ossRecoverable.getPartETags().size()][];

        int partEtagBytes = 0;
        for (int i = 0; i < ossRecoverable.getPartETags().size(); i++) {
            etags[i] = ossRecoverable.getPartETags().get(i).getETag().getBytes(CHARSET);
            partEtagBytes += etags[i].length + 2 * Integer.BYTES;
        }

        final String lastObjectKey = ossRecoverable.getLastPartObject();
        final byte[] lastPartBytes = lastObjectKey == null ? null : lastObjectKey.getBytes(CHARSET);

        /**
         * magic number object name length + object name bytes upload id length + upload id bytes
         * etags length + (part number + etag length + etag bytes)? number bytes of parts last part
         * bytes length + last part bytes last part object length
         */
        final byte[] resultBytes =
                new byte
                        [Integer.BYTES
                                + Integer.BYTES
                                + objectBytes.length
                                + Integer.BYTES
                                + uploadIdBytes.length
                                + Integer.BYTES
                                + partEtagBytes
                                + Long.BYTES
                                + Integer.BYTES
                                + (lastPartBytes == null ? 0 : lastPartBytes.length)
                                + Long.BYTES];

        ByteBuffer byteBuffer = ByteBuffer.wrap(resultBytes).order(ByteOrder.LITTLE_ENDIAN);

        byteBuffer.putInt(MAGIC_NUMBER);

        byteBuffer.putInt(objectBytes.length);
        byteBuffer.put(objectBytes);

        byteBuffer.putInt(uploadIdBytes.length);
        byteBuffer.put(uploadIdBytes);

        byteBuffer.putInt(etags.length);
        for (int i = 0; i < ossRecoverable.getPartETags().size(); i++) {
            PartETag pe = ossRecoverable.getPartETags().get(i);
            byteBuffer.putInt(pe.getPartNumber());
            byteBuffer.putInt(etags[i].length);
            byteBuffer.put(etags[i]);
        }

        byteBuffer.putLong(ossRecoverable.getNumBytesInParts());

        if (lastPartBytes == null) {
            byteBuffer.putInt(0);
        } else {
            byteBuffer.putInt(lastPartBytes.length);
            byteBuffer.put(lastPartBytes);
        }

        byteBuffer.putLong(ossRecoverable.getLastPartObjectLength());

        return resultBytes;
    }

    @Override
    public OSSRecoverable deserialize(int version, byte[] serialized) throws IOException {
        switch (version) {
            case 1:
                return deserializeV1(serialized);
            default:
                throw new IOException("Unrecognized version or corrupt state: " + version);
        }
    }

    private OSSRecoverable deserializeV1(byte[] serialized) throws IOException {
        final ByteBuffer byteBuffer = ByteBuffer.wrap(serialized).order(ByteOrder.LITTLE_ENDIAN);

        if (byteBuffer.getInt() != MAGIC_NUMBER) {
            throw new IOException("Corrupt data: Unexpected magic number " + byteBuffer.getInt());
        }

        final byte[] objectBytes = new byte[byteBuffer.getInt()];
        byteBuffer.get(objectBytes);

        final byte[] uploadIdBytes = new byte[byteBuffer.getInt()];
        byteBuffer.get(uploadIdBytes);

        final int numParts = byteBuffer.getInt();
        final ArrayList<PartETag> parts = new ArrayList<>(numParts);
        for (int i = 0; i < numParts; i++) {
            final int partNum = byteBuffer.getInt();
            final byte[] buffer = new byte[byteBuffer.getInt()];
            byteBuffer.get(buffer);
            parts.add(new PartETag(partNum, new String(buffer, CHARSET)));
        }

        final long numBytes = byteBuffer.getLong();

        final String lastPart;
        final int lastObjectArraySize = byteBuffer.getInt();
        if (lastObjectArraySize == 0) {
            lastPart = null;
        } else {
            byte[] lastPartBytes = new byte[lastObjectArraySize];
            byteBuffer.get(lastPartBytes);
            lastPart = new String(lastPartBytes, CHARSET);
        }

        final long lastPartLength = byteBuffer.getLong();

        return new OSSRecoverable(
                new String(uploadIdBytes, CHARSET),
                new String(objectBytes, CHARSET),
                parts,
                lastPart,
                numBytes,
                lastPartLength);
    }
}
