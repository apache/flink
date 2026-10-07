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

import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.RecoverableFsDataOutputStream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests how {@link NativeS3RecoverableWriter} persists, restores and cleans up the tail of an
 * in-progress file, i.e. the buffered bytes that are too small to be a multipart upload part.
 */
class NativeS3RecoverableWriterTest {

    private static final long MIN_PART_SIZE = 10L;
    private static final String KEY = "dir/out.txt";
    private static final String UUID_REGEX =
            "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}";

    @TempDir java.nio.file.Path tmp;

    private FakeNativeS3Operations s3;

    @BeforeEach
    void setUp() {
        s3 = new FakeNativeS3Operations();
    }

    /** Recovery reads the tail by the key stored in the recoverable, whatever its layout. */
    @ParameterizedTest
    @ValueSource(strings = {".incomplete/U1/tail", "dir/.incomplete/U1/tail"})
    void recoverResumesFromTailAtStoredKey(String tailKey) throws Exception {
        final String uploadId = s3.startMultiPartUpload(KEY);
        s3.objects.put(tailKey, ascii("AAAAA"));
        final NativeS3Recoverable recoverable =
                new NativeS3Recoverable(KEY, uploadId, Collections.emptyList(), 0L, tailKey, 5L);

        final RecoverableFsDataOutputStream resumed = writer().recover(recoverable);
        resumed.write(ascii("BBB"), 0, 3);
        resumed.closeForCommit().commit();

        assertThat(s3.committedObjects.get(KEY)).isEqualTo(ascii("AAAAABBB"));
    }

    @ParameterizedTest
    @ValueSource(strings = {".incomplete/U1/tail", "dir/.incomplete/U1/tail"})
    void cleanupDeletesOnlyTailAtStoredKey(String tailKey) throws Exception {
        s3.objects.put(tailKey, ascii("AAAAA"));
        s3.objects.put("dir/other", ascii("other"));
        final NativeS3Recoverable recoverable =
                new NativeS3Recoverable(KEY, "U1", Collections.emptyList(), 0L, tailKey, 5L);

        assertThat(writer().cleanupRecoverableState(recoverable)).isTrue();

        assertThat(s3.objects).containsOnlyKeys("dir/other");
    }

    @Test
    void cleanupWithoutTailDeletesNothing() throws Exception {
        s3.objects.put("dir/other", ascii("other"));
        final NativeS3Recoverable recoverable =
                new NativeS3Recoverable(KEY, "U1", Collections.emptyList(), 0L);

        assertThat(writer().cleanupRecoverableState(recoverable)).isFalse();

        assertThat(s3.objects).containsOnlyKeys("dir/other");
    }

    @Test
    void persistWithoutBufferedBytesStoresNoTail() throws Exception {
        final RecoverableFsDataOutputStream stream = writer().open(targetPath(KEY));
        // A full part is uploaded right away, which leaves nothing buffered.
        stream.write(ascii("AAAAAAAAAA"), 0, 10);

        final NativeS3Recoverable recoverable = (NativeS3Recoverable) stream.persist();
        stream.close();

        assertThat(recoverable.incompleteObjectName()).isNull();
        assertThat(s3.objects).isEmpty();
    }

    @Test
    void persistStoresTailUnderTargetDirectory() throws Exception {
        final NativeS3Recoverable recoverable = persistFiveBytes("a/b/out.txt");

        assertThat(recoverable.incompleteObjectName())
                .matches(
                        Pattern.quote("a/b/.incomplete/" + recoverable.uploadId() + "/")
                                + UUID_REGEX);
        assertTailStored(recoverable, "AAAAA");
    }

    @Test
    void persistStoresTailAtBucketRootForRootLevelTarget() throws Exception {
        final NativeS3Recoverable recoverable = persistFiveBytes("out.txt");

        assertThat(recoverable.incompleteObjectName())
                .matches(Pattern.quote(".incomplete/" + recoverable.uploadId() + "/") + UUID_REGEX);
        assertTailStored(recoverable, "AAAAA");
    }

    private NativeS3Recoverable persistFiveBytes(String key) throws IOException {
        final RecoverableFsDataOutputStream stream = writer().open(targetPath(key));
        stream.write(ascii("AAAAA"), 0, 5);
        final NativeS3Recoverable recoverable = (NativeS3Recoverable) stream.persist();
        stream.close();
        return recoverable;
    }

    private void assertTailStored(NativeS3Recoverable recoverable, String expectedContent) {
        assertThat(recoverable.incompleteObjectLength()).isEqualTo(expectedContent.length());
        assertThat(s3.objects.get(recoverable.incompleteObjectName()))
                .isEqualTo(ascii(expectedContent));
    }

    private NativeS3RecoverableWriter writer() {
        return NativeS3RecoverableWriter.writer(s3, tmp.toString(), MIN_PART_SIZE, 1);
    }

    private static Path targetPath(String key) {
        return new Path("s3://test-bucket/" + key);
    }

    private static byte[] ascii(String value) {
        return value.getBytes(StandardCharsets.US_ASCII);
    }
}
