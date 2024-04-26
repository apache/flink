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

import com.aliyun.oss.model.PartETag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link OSSRecoverableSerializer}. */
class OSSRecoverableSerializerTest {

    private final OSSRecoverableSerializer serializer = OSSRecoverableSerializer.INSTANCE;

    private static final String TEST_OBJECT_NAME = "TEST-OBJECT";

    private static final String TEST_UPLOAD_ID = "TEST-UPLOAD-ID";

    private static final String INCOMPLETE_OBJECT_NAME = "TEST-INCOMPLETE-PART";

    private static final String ETAG_PREFIX = "TEST-ETAG-";

    @Test
    void testSerializeEmptyOSSRecoverable() throws IOException {
        OSSRecoverable originalEmptyRecoverable = createOSSRecoverable(false);

        byte[] serializedRecoverable = serializer.serialize(originalEmptyRecoverable);
        OSSRecoverable copiedEmptyRecoverable = serializer.deserialize(1, serializedRecoverable);

        assertIsEqualTo(originalEmptyRecoverable, copiedEmptyRecoverable);
    }

    @Test
    void testSerializeOSSRecoverableOnlyWithIncompleteObject() throws IOException {
        OSSRecoverable originalEmptyRecoverable = createOSSRecoverable(true);

        byte[] serializedRecoverable = serializer.serialize(originalEmptyRecoverable);
        OSSRecoverable copiedEmptyRecoverable = serializer.deserialize(1, serializedRecoverable);

        assertIsEqualTo(originalEmptyRecoverable, copiedEmptyRecoverable);
    }

    @Test
    void testSerializeOSSRecoverableWithIncompleteObject() throws IOException {
        OSSRecoverable originalEmptyRecoverable = createOSSRecoverable(true, 2, 4, 6);

        byte[] serializedRecoverable = serializer.serialize(originalEmptyRecoverable);
        OSSRecoverable copiedEmptyRecoverable = serializer.deserialize(1, serializedRecoverable);

        assertIsEqualTo(originalEmptyRecoverable, copiedEmptyRecoverable);
    }

    @Test
    void testSerializeOSSRecoverableWithoutIncompleteObject() throws IOException {
        OSSRecoverable originalEmptyRecoverable = createOSSRecoverable(false, 2, 4, 6);

        byte[] serializedRecoverable = serializer.serialize(originalEmptyRecoverable);
        OSSRecoverable copiedEmptyRecoverable = serializer.deserialize(1, serializedRecoverable);

        assertIsEqualTo(originalEmptyRecoverable, copiedEmptyRecoverable);
    }

    // --------------------------------- Test Utils ---------------------------------

    private static OSSRecoverable createOSSRecoverable(
            boolean withIncompletePart, int... partNumbers) {
        List<PartETag> etags = new ArrayList<>();
        for (int i : partNumbers) {
            etags.add(createEtag(i));
        }

        if (withIncompletePart) {
            return new OSSRecoverable(
                    TEST_UPLOAD_ID,
                    TEST_OBJECT_NAME,
                    etags,
                    INCOMPLETE_OBJECT_NAME,
                    12345L,
                    54321L);
        } else {
            return new OSSRecoverable(TEST_UPLOAD_ID, TEST_OBJECT_NAME, etags, null, 12345L, 0L);
        }
    }

    private static void assertIsEqualTo(OSSRecoverable actual, OSSRecoverable expected) {
        assertThat(actual.getObjectName()).isEqualTo(expected.getObjectName());
        assertThat(actual.getUploadId()).isEqualTo(expected.getUploadId());
        assertThat(actual.getNumBytesInParts()).isEqualTo(expected.getNumBytesInParts());
        assertThat(actual.getLastPartObject()).isEqualTo(expected.getLastPartObject());
        assertThat(actual.getLastPartObjectLength()).isEqualTo(expected.getLastPartObjectLength());
        assertThat(actual.getPartETags().stream().map(PartETag::getETag).toArray())
                .isEqualTo(expected.getPartETags().stream().map(PartETag::getETag).toArray());
    }

    private static PartETag createEtag(int partNumber) {
        return new PartETag(partNumber, ETAG_PREFIX + partNumber);
    }
}
