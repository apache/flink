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
}
