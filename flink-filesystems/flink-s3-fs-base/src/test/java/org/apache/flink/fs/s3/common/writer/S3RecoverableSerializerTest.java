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

package org.apache.flink.fs.s3.common.writer;

import com.amazonaws.services.s3.model.PartETag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link S3RecoverableSerializer}. */
class S3RecoverableSerializerTest {

    private final S3RecoverableSerializer serializer = S3RecoverableSerializer.INSTANCE;

    private static final String TEST_OBJECT_NAME = "TEST-OBJECT";

    private static final String TEST_UPLOAD_ID = "TEST-UPLOAD-ID";

    private static final String INCOMPLETE_OBJECT_NAME = "TEST-INCOMPLETE-PART";

    private static final String ETAG_PREFIX = "TEST-ETAG-";

    @Test
    void serializeEmptyS3Recoverable() throws IOException {
        S3Recoverable originalEmptyRecoverable = createTestS3Recoverable(false);

        byte[] serializedRecoverable = serializer.serialize(originalEmptyRecoverable);
        S3Recoverable copiedEmptyRecoverable = serializer.deserialize(1, serializedRecoverable);

        assertThatIsEqualTo(originalEmptyRecoverable, copiedEmptyRecoverable);
    }

    @Test
    void serializeS3RecoverableWithoutIncompleteObject() throws IOException {
        S3Recoverable originalNoIncompletePartRecoverable = createTestS3Recoverable(false, 1, 5, 9);

        byte[] serializedRecoverable = serializer.serialize(originalNoIncompletePartRecoverable);
        S3Recoverable copiedNoIncompletePartRecoverable =
                serializer.deserialize(1, serializedRecoverable);

        assertThatIsEqualTo(originalNoIncompletePartRecoverable, copiedNoIncompletePartRecoverable);
    }

    @Test
    void serializeS3RecoverableOnlyWithIncompleteObject() throws IOException {
        S3Recoverable originalOnlyIncompletePartRecoverable = createTestS3Recoverable(true);

        byte[] serializedRecoverable = serializer.serialize(originalOnlyIncompletePartRecoverable);
        S3Recoverable copiedOnlyIncompletePartRecoverable =
                serializer.deserialize(1, serializedRecoverable);

        assertThatIsEqualTo(
                originalOnlyIncompletePartRecoverable, copiedOnlyIncompletePartRecoverable);
    }

    @Test
    void serializeS3RecoverableWithCompleteAndIncompleteParts() throws IOException {
        S3Recoverable originalFullRecoverable = createTestS3Recoverable(true, 1, 5, 9);

        byte[] serializedRecoverable = serializer.serialize(originalFullRecoverable);
        S3Recoverable copiedFullRecoverable = serializer.deserialize(1, serializedRecoverable);

        assertThatIsEqualTo(originalFullRecoverable, copiedFullRecoverable);
    }

    private static void assertThatIsEqualTo(
            S3Recoverable actualRecoverable, S3Recoverable expectedRecoverable) {
        assertThat(actualRecoverable.getObjectName())
                .isEqualTo(expectedRecoverable.getObjectName());
        assertThat(actualRecoverable.uploadId()).isEqualTo(expectedRecoverable.uploadId());
        assertThat(actualRecoverable.numBytesInParts())
                .isEqualTo(expectedRecoverable.numBytesInParts());
        assertThat(actualRecoverable.incompleteObjectName())
                .isEqualTo(expectedRecoverable.incompleteObjectName());
        assertThat(actualRecoverable.incompleteObjectLength())
                .isEqualTo(expectedRecoverable.incompleteObjectLength());
        assertThat(actualRecoverable.parts().stream().map(PartETag::getETag).toArray())
                .isEqualTo(expectedRecoverable.parts().stream().map(PartETag::getETag).toArray());
    }

    // --------------------------------- Test Utils ---------------------------------

    private static S3Recoverable createTestS3Recoverable(
            boolean withIncompletePart, int... partNumbers) {
        List<PartETag> etags = new ArrayList<>();
        for (int i : partNumbers) {
            etags.add(createEtag(i));
        }

        if (withIncompletePart) {
            return new S3Recoverable(
                    TEST_OBJECT_NAME,
                    TEST_UPLOAD_ID,
                    etags,
                    12345L,
                    INCOMPLETE_OBJECT_NAME,
                    54321L);
        } else {
            return new S3Recoverable(TEST_OBJECT_NAME, TEST_UPLOAD_ID, etags, 12345L);
        }
    }

    private static PartETag createEtag(int partNumber) {
        return new PartETag(partNumber, ETAG_PREFIX + partNumber);
    }
}
