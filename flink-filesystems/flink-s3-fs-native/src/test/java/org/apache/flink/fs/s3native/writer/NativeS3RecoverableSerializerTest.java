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

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link NativeS3RecoverableSerializer}. */
class NativeS3RecoverableSerializerTest {

    @Test
    void testSerializeAndDeserializeWithParts() throws IOException {
        NativeS3RecoverableSerializer serializer = NativeS3RecoverableSerializer.INSTANCE;

        List<NativeS3Recoverable.PartETag> parts = new ArrayList<>();
        parts.add(new NativeS3Recoverable.PartETag(1, "etag1"));
        parts.add(new NativeS3Recoverable.PartETag(2, "etag2"));
        parts.add(new NativeS3Recoverable.PartETag(3, "etag3"));

        NativeS3Recoverable original =
                new NativeS3Recoverable("test-object-key", "test-upload-id", parts, 12345L);

        byte[] serialized = serializer.serialize(original);
        assertThat(serialized).isNotNull();
        assertThat(serialized.length).isGreaterThan(0);

        NativeS3Recoverable deserialized =
                serializer.deserialize(serializer.getVersion(), serialized);

        assertThat(deserialized.getObjectName()).isEqualTo(original.getObjectName());
        assertThat(deserialized.uploadId()).isEqualTo(original.uploadId());
        assertThat(deserialized.numBytesInParts()).isEqualTo(original.numBytesInParts());
        assertThat(deserialized.parts()).hasSize(original.parts().size());
        assertThat(deserialized.incompleteObjectName()).isNull();

        for (int i = 0; i < parts.size(); i++) {
            assertThat(deserialized.parts().get(i).getPartNumber())
                    .isEqualTo(original.parts().get(i).getPartNumber());
            assertThat(deserialized.parts().get(i).getETag())
                    .isEqualTo(original.parts().get(i).getETag());
        }
    }

    @Test
    void testSerializeAndDeserializeWithIncompleteObject() throws IOException {
        NativeS3RecoverableSerializer serializer = NativeS3RecoverableSerializer.INSTANCE;

        List<NativeS3Recoverable.PartETag> parts = new ArrayList<>();
        parts.add(new NativeS3Recoverable.PartETag(1, "etag1"));

        NativeS3Recoverable original =
                new NativeS3Recoverable(
                        "test-object-key",
                        "test-upload-id",
                        parts,
                        5242880L,
                        "incomplete-object-key",
                        1024L);

        byte[] serialized = serializer.serialize(original);
        NativeS3Recoverable deserialized =
                serializer.deserialize(serializer.getVersion(), serialized);

        assertThat(deserialized.getObjectName()).isEqualTo(original.getObjectName());
        assertThat(deserialized.uploadId()).isEqualTo(original.uploadId());
        assertThat(deserialized.numBytesInParts()).isEqualTo(original.numBytesInParts());
        assertThat(deserialized.incompleteObjectName()).isEqualTo(original.incompleteObjectName());
    }

    @Test
    void testSerializeAndDeserializeEmptyParts() throws IOException {
        NativeS3RecoverableSerializer serializer = NativeS3RecoverableSerializer.INSTANCE;

        NativeS3Recoverable original =
                new NativeS3Recoverable("test-object-key", "test-upload-id", new ArrayList<>(), 0L);

        byte[] serialized = serializer.serialize(original);
        NativeS3Recoverable deserialized =
                serializer.deserialize(serializer.getVersion(), serialized);

        assertThat(deserialized.getObjectName()).isEqualTo(original.getObjectName());
        assertThat(deserialized.uploadId()).isEqualTo(original.uploadId());
        assertThat(deserialized.numBytesInParts()).isEqualTo(0L);
        assertThat(deserialized.parts()).isEmpty();
    }

    @Test
    void testVersionIsConsistent() {
        NativeS3RecoverableSerializer serializer = NativeS3RecoverableSerializer.INSTANCE;
        assertThat(serializer.getVersion()).isGreaterThanOrEqualTo(1);
    }

    /**
     * Checkpoints and savepoints hold the tail key as an opaque string, so state written earlier
     * must stay readable whatever the key looks like, and the bytes written must not change.
     */
    @Test
    void testVersion1BytesWithTailKeyAtBucketRoot() throws IOException {
        NativeS3RecoverableSerializer serializer = NativeS3RecoverableSerializer.INSTANCE;
        String tailKey = ".incomplete/upload-id/0f8fad5b-d9cb-469f-a165-70867728950e";

        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bytes);
        out.writeUTF("dir/out.txt");
        out.writeUTF("upload-id");
        out.writeLong(5242880L);
        out.writeInt(1);
        out.writeInt(1);
        out.writeUTF("etag1");
        out.writeBoolean(true);
        out.writeUTF(tailKey);
        out.writeLong(1024L);
        out.flush();
        byte[] version1 = bytes.toByteArray();

        NativeS3Recoverable deserialized = serializer.deserialize(1, version1);

        assertThat(deserialized.getObjectName()).isEqualTo("dir/out.txt");
        assertThat(deserialized.uploadId()).isEqualTo("upload-id");
        assertThat(deserialized.numBytesInParts()).isEqualTo(5242880L);
        assertThat(deserialized.parts()).hasSize(1);
        assertThat(deserialized.parts().get(0).getPartNumber()).isEqualTo(1);
        assertThat(deserialized.parts().get(0).getETag()).isEqualTo("etag1");
        assertThat(deserialized.incompleteObjectName()).isEqualTo(tailKey);
        assertThat(deserialized.incompleteObjectLength()).isEqualTo(1024L);
        assertThat(serializer.serialize(deserialized)).isEqualTo(version1);
    }
}
