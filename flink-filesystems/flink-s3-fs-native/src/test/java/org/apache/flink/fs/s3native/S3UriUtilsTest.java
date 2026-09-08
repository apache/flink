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

import org.apache.flink.core.fs.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

/** Tests for {@link S3UriUtils}. */
class S3UriUtilsTest {

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
        assertThat(S3UriUtils.extractBucket(uri)).isEqualTo(expectedBucket);
    }

    @Test
    void testExtractBucketWithNullThrowsException() {
        assertThatNullPointerException().isThrownBy(() -> S3UriUtils.extractBucket(null));
    }

    @ParameterizedTest
    @CsvSource({
        "https://s3.amazonaws.com/bucket/key",
        "https://bucket.s3.amazonaws.com/key",
        "hdfs://bucket/key",
        "not-a-uri",
        "bucket/key",
    })
    void testExtractBucketRejectsUnsupportedUri(String uri) {
        assertThatIllegalArgumentException().isThrownBy(() -> S3UriUtils.extractBucket(uri));
    }

    @ParameterizedTest
    @CsvSource({"S3://my-bucket/file.txt,my-bucket", "S3A://my-bucket/file.txt,my-bucket"})
    void testExtractBucketIsCaseInsensitiveToScheme(String uri, String expectedBucket) {
        assertThat(S3UriUtils.extractBucket(uri)).isEqualTo(expectedBucket);
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
        assertThat(S3UriUtils.extractKey(uri)).isEqualTo(expectedKey);
    }

    @ParameterizedTest
    @CsvSource({
        "s3://bucket",
        "s3://bucket/",
        "s3a://bucket",
    })
    void testExtractKeyReturnsEmptyForBucketOnly(String uri) {
        assertThat(S3UriUtils.extractKey(uri)).isEmpty();
    }

    @Test
    void testExtractKeyWithNullThrowsException() {
        assertThatNullPointerException().isThrownBy(() -> S3UriUtils.extractKey((String) null));
    }

    @ParameterizedTest
    @CsvSource({
        "https://s3.amazonaws.com/bucket/key",
        "https://bucket.s3.amazonaws.com/key",
        "hdfs://bucket/key",
        "not-a-uri",
        "bucket/key",
    })
    void testExtractKeyRejectsUnsupportedUri(String uri) {
        assertThatIllegalArgumentException().isThrownBy(() -> S3UriUtils.extractKey(uri));
    }

    @ParameterizedTest
    @CsvSource({"S3://bucket/file.txt,file.txt", "S3A://bucket/file.txt,file.txt"})
    void testExtractKeyIsCaseInsensitiveToScheme(String uri, String expectedKey) {
        assertThat(S3UriUtils.extractKey(uri)).isEqualTo(expectedKey);
    }

    @Test
    void testBothSchemesYieldSameResult() {
        assertThat(S3UriUtils.extractBucket("s3://test-bucket/path/file.txt"))
                .isEqualTo(S3UriUtils.extractBucket("s3a://test-bucket/path/file.txt"))
                .isEqualTo("test-bucket");
        assertThat(S3UriUtils.extractKey("s3://bucket/deep/path/file.txt"))
                .isEqualTo(S3UriUtils.extractKey("s3a://bucket/deep/path/file.txt"))
                .isEqualTo("deep/path/file.txt");
    }

    @Test
    void testBucketAndKeyRoundTrip() {
        String uri = "s3://my-bucket/path/to/file.txt";
        String reconstructed =
                "s3://" + S3UriUtils.extractBucket(uri) + "/" + S3UriUtils.extractKey(uri);
        assertThat(reconstructed).isEqualTo(uri);
    }

    @Test
    void testExtractKeyVeryLongPath() {
        StringBuilder path = new StringBuilder();
        for (int i = 0; i < 50; i++) {
            path.append("level").append(i).append("/");
        }
        path.append("file.txt");
        assertThat(S3UriUtils.extractKey("s3://bucket/" + path)).isEqualTo(path.toString());
    }

    @Test
    void testExtractKeyFromPath() {
        assertThat(S3UriUtils.extractKey(new Path("s3://bucket/path/to/file.txt")))
                .isEqualTo("path/to/file.txt");
    }

    @Test
    void testExtractKeyFromPathWithNoKey() {
        assertThat(S3UriUtils.extractKey(new Path("s3://bucket"))).isEmpty();
    }

    @Test
    void testExtractBucketNameFromPath() {
        assertThat(S3UriUtils.extractBucketName(new Path("s3://my-bucket/path/to/file.txt")))
                .isEqualTo("my-bucket");
    }

    @ParameterizedTest
    @CsvSource({"file:///tmp/foo", "hdfs://bucket/key", "gs://bucket/key"})
    void testExtractKeyFromPathRejectsUnsupportedScheme(String uri) {
        Path path = new Path(uri);
        assertThatIllegalArgumentException().isThrownBy(() -> S3UriUtils.extractKey(path));
    }

    @ParameterizedTest
    @CsvSource({"file:///tmp/foo", "hdfs://bucket/key", "gs://bucket/key"})
    void testExtractBucketNameFromPathRejectsUnsupportedScheme(String uri) {
        Path path = new Path(uri);
        assertThatIllegalArgumentException().isThrownBy(() -> S3UriUtils.extractBucketName(path));
    }

    @ParameterizedTest
    @CsvSource({"s3://bucket/key", "s3a://bucket/key", "S3://bucket/key", "S3A://bucket/key"})
    void testIsSupportedS3Scheme(String uri) {
        assertThat(S3UriUtils.isSupportedS3Scheme(new Path(uri))).isTrue();
    }

    @ParameterizedTest
    @CsvSource({"file:///tmp/foo", "hdfs://bucket/key", "gs://bucket/key"})
    void testIsSupportedS3SchemeRejectsOtherSchemes(String uri) {
        assertThat(S3UriUtils.isSupportedS3Scheme(new Path(uri))).isFalse();
    }

    @Test
    void testIsSupportedLocalSchemeAcceptsNoScheme() {
        assertThat(S3UriUtils.isSupportedLocalScheme(new Path("/tmp/foo"))).isTrue();
    }

    @ParameterizedTest
    @CsvSource({"file:///tmp/foo", "FILE:///tmp/foo"})
    void testIsSupportedLocalSchemeAcceptsFileScheme(String uri) {
        assertThat(S3UriUtils.isSupportedLocalScheme(new Path(uri))).isTrue();
    }

    @ParameterizedTest
    @CsvSource({"s3://bucket/key", "s3a://bucket/key", "hdfs://bucket/key"})
    void testIsSupportedLocalSchemeRejectsOtherSchemes(String uri) {
        assertThat(S3UriUtils.isSupportedLocalScheme(new Path(uri))).isFalse();
    }
}
