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

package org.apache.flink.fs.s3native;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link NativeS3FileSystem}. */
class NativeS3FileSystemTest {

    @Test
    void createsEmptyDirectoryMarker() {
        final StubS3Client s3Client = new StubS3Client();

        NativeS3FileSystem.putDirectoryMarker(
                s3Client, "test-bucket", "catalog/", S3EncryptionConfig.none());

        assertThat(s3Client.request.bucket()).isEqualTo("test-bucket");
        assertThat(s3Client.request.key()).isEqualTo("catalog/");
        assertThat(s3Client.contentLength).isZero();
    }

    @Test
    void directoryMarkerKeysAreDetected() {
        assertThat(NativeS3FileSystem.isDirectoryMarkerKey("")).isTrue();
        assertThat(NativeS3FileSystem.isDirectoryMarkerKey("dir/")).isTrue();
        assertThat(NativeS3FileSystem.isDirectoryMarkerKey("a/b/c/")).isTrue();
    }

    @Test
    void regularKeysAreNotDirectoryMarkers() {
        assertThat(NativeS3FileSystem.isDirectoryMarkerKey("chk-1/_metadata")).isFalse();
        assertThat(NativeS3FileSystem.isDirectoryMarkerKey("empty-file")).isFalse();
        assertThat(NativeS3FileSystem.isDirectoryMarkerKey("a/b/part-0-0")).isFalse();
    }

    @Test
    void parentDirectoryMarkerIsDerivedFromKey() {
        assertThat(NativeS3FileSystem.parentDirectoryMarkerKey("a/b/part-0-0")).isEqualTo("a/b/");
        assertThat(NativeS3FileSystem.parentDirectoryMarkerKey("a/b/c/")).isEqualTo("a/b/");
    }

    @Test
    void topLevelKeysHaveNoParentDirectoryMarker() {
        assertThat(NativeS3FileSystem.parentDirectoryMarkerKey("file")).isNull();
        assertThat(NativeS3FileSystem.parentDirectoryMarkerKey("dir/")).isNull();
        assertThat(NativeS3FileSystem.parentDirectoryMarkerKey("")).isNull();
    }

    private static final class StubS3Client implements S3Client {
        private PutObjectRequest request;
        private long contentLength;

        @Override
        public PutObjectResponse putObject(PutObjectRequest request, RequestBody requestBody) {
            this.request = request;
            this.contentLength = requestBody.contentLength();
            return PutObjectResponse.builder().build();
        }

        @Override
        public String serviceName() {
            return "s3";
        }

        @Override
        public void close() {}
    }
}
