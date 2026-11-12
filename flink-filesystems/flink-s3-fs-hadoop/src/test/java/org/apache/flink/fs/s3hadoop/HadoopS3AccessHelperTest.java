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

package org.apache.flink.fs.s3hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import software.amazon.awssdk.core.interceptor.Context;
import software.amazon.awssdk.core.interceptor.ExecutionAttributes;
import software.amazon.awssdk.core.interceptor.ExecutionInterceptor;
import software.amazon.awssdk.http.SdkHttpRequest;

import java.io.File;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.apache.hadoop.fs.s3a.Constants.ACCESS_KEY;
import static org.apache.hadoop.fs.s3a.Constants.AWS_CREDENTIALS_PROVIDER;
import static org.apache.hadoop.fs.s3a.Constants.AWS_REGION;
import static org.apache.hadoop.fs.s3a.Constants.CHECKSUM_ALGORITHM;
import static org.apache.hadoop.fs.s3a.Constants.CHECKSUM_GENERATION;
import static org.apache.hadoop.fs.s3a.Constants.ENDPOINT;
import static org.apache.hadoop.fs.s3a.Constants.MAX_ERROR_RETRIES;
import static org.apache.hadoop.fs.s3a.Constants.PATH_STYLE_ACCESS;
import static org.apache.hadoop.fs.s3a.Constants.RETRY_LIMIT;
import static org.apache.hadoop.fs.s3a.Constants.S3A_BUCKET_PROBE;
import static org.apache.hadoop.fs.s3a.Constants.SECRET_KEY;
import static org.apache.hadoop.fs.s3a.audit.S3AAuditConstants.AUDIT_EXECUTION_INTERCEPTORS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/** Tests for {@link HadoopS3AccessHelper}. */
class HadoopS3AccessHelperTest {

    /** Base64 MD5 of "hello". */
    private static final String HELLO_MD5 = "XUFAKrxLKna5cZ2REBfFkg==";

    @TempDir private Path tempDir;

    @BeforeEach
    void clearCapturedRequests() {
        RequestCapture.REQUESTS.clear();
    }

    @Test
    void uploadPartSendsContentMd5() throws Exception {
        final File part = writeFile("hello");
        try (S3AFileSystem s3a = newFileSystem()) {
            final HadoopS3AccessHelper helper = new HadoopS3AccessHelper(s3a, s3a.getConf());

            catchThrowable(() -> helper.uploadPart("key", "upload-id", 1, part, part.length()));
        }

        assertThat(capturedContentMd5()).containsExactly(HELLO_MD5);
    }

    @Test
    void putObjectSendsContentMd5() throws Exception {
        final File object = writeFile("hello");
        try (S3AFileSystem s3a = newFileSystem()) {
            final HadoopS3AccessHelper helper = new HadoopS3AccessHelper(s3a, s3a.getConf());

            catchThrowable(() -> helper.putObject("key", object));
        }

        assertThat(capturedContentMd5()).containsExactly(HELLO_MD5);
    }

    private File writeFile(String content) throws Exception {
        final Path file = tempDir.resolve("data");
        Files.write(file, content.getBytes(StandardCharsets.UTF_8));
        return file.toFile();
    }

    private static List<String> capturedContentMd5() {
        assertThat(RequestCapture.REQUESTS).hasSize(1);
        return RequestCapture.REQUESTS.get(0).matchingHeaders("Content-MD5");
    }

    /** An S3A filesystem whose requests stop at {@link RequestCapture}, before the network. */
    private static S3AFileSystem newFileSystem() throws Exception {
        final Configuration conf = new Configuration(false);
        conf.set(ENDPOINT, "http://127.0.0.1:1");
        conf.set(AWS_REGION, "us-east-1");
        conf.setBoolean(PATH_STYLE_ACCESS, true);
        conf.set(AWS_CREDENTIALS_PROVIDER, SimpleAWSCredentialsProvider.class.getName());
        conf.set(ACCESS_KEY, "access-key");
        conf.set(SECRET_KEY, "secret-key");
        conf.setInt(S3A_BUCKET_PROBE, 0);
        conf.setInt(MAX_ERROR_RETRIES, 0);
        conf.setInt(RETRY_LIMIT, 0);
        conf.set(AUDIT_EXECUTION_INTERCEPTORS, RequestCapture.class.getName());
        final S3AFileSystem s3a = new S3AFileSystem();
        s3a.initialize(new URI("s3a://bucket/"), conf);
        return s3a;
    }

    /** Records each signed HTTP request and fails it, so no request leaves the JVM. */
    public static final class RequestCapture implements ExecutionInterceptor {

        static final List<SdkHttpRequest> REQUESTS = new CopyOnWriteArrayList<>();

        @Override
        public void beforeTransmission(
                Context.BeforeTransmission context, ExecutionAttributes executionAttributes) {
            REQUESTS.add(context.httpRequest());
            throw new IllegalStateException("Request captured");
        }
    }

    @Test
    void partChecksumsAreDisabledByDefault() {
        assertThat(HadoopS3AccessHelper.partChecksumsEnabled(new Configuration(false))).isFalse();
    }

    @Test
    void partChecksumsAreDisabledForAlgorithmNone() {
        final Configuration conf = new Configuration(false);
        conf.set(CHECKSUM_ALGORITHM, " none ");

        assertThat(HadoopS3AccessHelper.partChecksumsEnabled(conf)).isFalse();
    }

    @Test
    void partChecksumsAreDisabledForEmptyAlgorithm() {
        final Configuration conf = new Configuration(false);
        conf.set(CHECKSUM_ALGORITHM, "");

        assertThat(HadoopS3AccessHelper.partChecksumsEnabled(conf)).isFalse();
    }

    @Test
    void partChecksumsAreEnabledByAlgorithm() {
        final Configuration conf = new Configuration(false);
        conf.set(CHECKSUM_ALGORITHM, "CRC32C");

        assertThat(HadoopS3AccessHelper.partChecksumsEnabled(conf)).isTrue();
    }

    @Test
    void partChecksumsAreEnabledByGeneration() {
        final Configuration conf = new Configuration(false);
        conf.setBoolean(CHECKSUM_GENERATION, true);

        assertThat(HadoopS3AccessHelper.partChecksumsEnabled(conf)).isTrue();
    }
}
