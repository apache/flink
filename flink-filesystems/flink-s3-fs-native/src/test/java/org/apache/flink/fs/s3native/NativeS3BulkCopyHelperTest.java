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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

/** Tests for {@link NativeS3BulkCopyHelper}. */
class NativeS3BulkCopyHelperTest {

    private static final NativeS3BulkCopyHelper helper = new NativeS3BulkCopyHelper(null, 1);

    @ParameterizedTest
    @CsvSource({
        "s3://my-bucket/file.txt,my-bucket",
        "s3a://my-bucket/file.txt,my-bucket",
        "s3://bucket-name/path/to/file.txt,bucket-name",
        "s3://my.bucket.name/file.txt,my.bucket.name",
        "s3://bucket,bucket",
        "s3://bucket/,bucket",
        "s3://a/file,a",
        "s3://my-bucket//path//to//file.txt,my-bucket",
    })
    void testExtractBucket(String uri, String expectedBucket) {
        assertThat(helper.extractBucket(uri)).isEqualTo(expectedBucket);
    }

    @Test
    void testExtractBucketWithNullThrowsException() {
        assertThatNullPointerException().isThrownBy(() -> helper.extractBucket(null));
    }

    @ParameterizedTest
    @CsvSource({
        "s3://bucket/file.txt,file.txt",
        "s3a://bucket/file.txt,file.txt",
        "s3://bucket/path/to/file.txt,path/to/file.txt",
        "s3://bucket/a/b/c/d/e/file.txt,a/b/c/d/e/file.txt",
        "s3://bucket//double//slash//path/file.txt,/double//slash//path/file.txt",
        "s3://bucket/path/to/directory/,path/to/directory/",
        "s3://bucket/path/file.tar.gz.backup,path/file.tar.gz.backup",
    })
    void testExtractKey(String uri, String expectedKey) {
        assertThat(helper.extractKey(uri)).isEqualTo(expectedKey);
    }

    @ParameterizedTest
    @CsvSource({
        "s3://bucket",
        "s3://bucket/",
        "s3a://bucket",
    })
    void testExtractKeyReturnsEmptyForBucketOnly(String uri) {
        assertThat(helper.extractKey(uri)).isEmpty();
    }

    @Test
    void testExtractKeyWithNullThrowsException() {
        assertThatNullPointerException().isThrownBy(() -> helper.extractKey(null));
    }

    @Test
    void testBothSchemesYieldSameResult() {
        assertThat(helper.extractBucket("s3://test-bucket/path/file.txt"))
                .isEqualTo(helper.extractBucket("s3a://test-bucket/path/file.txt"))
                .isEqualTo("test-bucket");
        assertThat(helper.extractKey("s3://bucket/deep/path/file.txt"))
                .isEqualTo(helper.extractKey("s3a://bucket/deep/path/file.txt"))
                .isEqualTo("deep/path/file.txt");
    }

    @Test
    void testBucketAndKeyRoundTrip() {
        String uri = "s3://my-bucket/path/to/file.txt";
        String reconstructed = "s3://" + helper.extractBucket(uri) + "/" + helper.extractKey(uri);
        assertThat(reconstructed).isEqualTo(uri);
    }

    @Test
    void testExtractKeyVeryLongPath() {
        StringBuilder path = new StringBuilder();
        for (int i = 0; i < 50; i++) {
            path.append("level").append(i).append("/");
        }
        path.append("file.txt");
        assertThat(helper.extractKey("s3://bucket/" + path)).isEqualTo(path.toString());
    }
}
