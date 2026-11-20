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

import org.apache.flink.testutils.logging.LoggerAuditingExtension;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider;
import org.apache.hadoop.fs.s3a.audit.impl.LoggingAuditor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.event.Level;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.hadoop.fs.s3a.Constants.ACCESS_KEY;
import static org.apache.hadoop.fs.s3a.Constants.AWS_CREDENTIALS_PROVIDER;
import static org.apache.hadoop.fs.s3a.Constants.AWS_REGION;
import static org.apache.hadoop.fs.s3a.Constants.CHECKSUM_ALGORITHM;
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

    @TempDir private Path tempDir;

    @RegisterExtension
    private final LoggerAuditingExtension auditLog =
            new LoggerAuditingExtension(LoggingAuditor.class, Level.INFO);

    @BeforeEach
    void clearCapturedRequests() {
        RequestCapture.REQUESTS.clear();
    }

    @Test
    void uploadPartSendsChecksumTrailer() throws Exception {
        final File part = writeFile("hello");
        try (S3AFileSystem s3a = newFileSystem("CRC32")) {
            final HadoopS3AccessHelper helper = new HadoopS3AccessHelper(s3a);

            assertThat(
                            catchThrowable(
                                    () ->
                                            helper.uploadPart(
                                                    "key", "upload-id", 1, part, part.length())))
                    .hasStackTraceContaining(RequestCapture.CAPTURED);
        }

        assertCrc32Trailer(capturedRequest());
    }

    @Test
    void putObjectSendsChecksumTrailer() throws Exception {
        final File object = writeFile("hello");
        try (S3AFileSystem s3a = newFileSystem("CRC32")) {
            final HadoopS3AccessHelper helper = new HadoopS3AccessHelper(s3a);

            assertThat(catchThrowable(() -> helper.putObject("key", object)))
                    .hasStackTraceContaining(RequestCapture.CAPTURED);
        }

        assertCrc32Trailer(capturedRequest());
    }

    /**
     * flink-s3-fs-native completes restored uploads with part numbers and ETags only, which S3
     * rejects for an upload that was created with a checksum algorithm.
     */
    @Test
    void multipartUploadIsCreatedWithoutChecksumAlgorithm() throws Exception {
        try (S3AFileSystem s3a = newFileSystem("CRC32")) {
            final HadoopS3AccessHelper helper = new HadoopS3AccessHelper(s3a);

            assertThat(catchThrowable(() -> helper.startMultiPartUpload("key")))
                    .hasStackTraceContaining(RequestCapture.CAPTURED);
        }

        final SdkHttpRequest request = capturedRequest();
        assertThat(request.rawQueryParameters()).containsKey("uploads");
        assertThat(request.firstMatchingHeader("x-amz-checksum-algorithm")).isEmpty();
    }

    /**
     * The JobManager creates an upload for every checkpoint's metadata, on another thread than the
     * one that created the filesystem, so a request built outside the audit span logs once per
     * checkpoint.
     */
    @Test
    void multipartUploadIsCreatedInsideTheAuditSpan() throws Exception {
        try (S3AFileSystem s3a = newFileSystem("CRC32")) {
            final HadoopS3AccessHelper helper = new HadoopS3AccessHelper(s3a);

            final ExecutorService executor = Executors.newSingleThreadExecutor();
            try {
                final CompletableFuture<Throwable> failure =
                        CompletableFuture.supplyAsync(
                                () -> catchThrowable(() -> helper.startMultiPartUpload("key")),
                                executor);
                assertThat(failure.get()).hasStackTraceContaining(RequestCapture.CAPTURED);
            } finally {
                executor.shutdownNow();
            }
        }

        assertThat(auditLog.getMessages()).noneMatch(m -> m.contains("outside an audit span"));
    }

    @Test
    void uploadPartSendsNoChecksumForAlgorithmNone() throws Exception {
        final File part = writeFile("hello");
        try (S3AFileSystem s3a = newFileSystem("NONE")) {
            final HadoopS3AccessHelper helper = new HadoopS3AccessHelper(s3a);

            assertThat(
                            catchThrowable(
                                    () ->
                                            helper.uploadPart(
                                                    "key", "upload-id", 1, part, part.length())))
                    .hasStackTraceContaining(RequestCapture.CAPTURED);
        }

        assertNoChecksum(capturedRequest());
    }

    @Test
    void putObjectSendsNoChecksumForAlgorithmNone() throws Exception {
        final File object = writeFile("hello");
        try (S3AFileSystem s3a = newFileSystem("NONE")) {
            final HadoopS3AccessHelper helper = new HadoopS3AccessHelper(s3a);

            assertThat(catchThrowable(() -> helper.putObject("key", object)))
                    .hasStackTraceContaining(RequestCapture.CAPTURED);
        }

        assertNoChecksum(capturedRequest());
    }

    private static void assertNoChecksum(SdkHttpRequest request) {
        assertThat(request.firstMatchingHeader("x-amz-sdk-checksum-algorithm")).isEmpty();
        assertThat(request.firstMatchingHeader("x-amz-trailer")).isEmpty();
    }

    private static void assertCrc32Trailer(SdkHttpRequest request) {
        assertThat(request.firstMatchingHeader("x-amz-sdk-checksum-algorithm")).hasValue("CRC32");
        assertThat(request.firstMatchingHeader("x-amz-trailer")).hasValue("x-amz-checksum-crc32");
    }

    private File writeFile(String content) throws Exception {
        final Path file = tempDir.resolve("data");
        Files.write(file, content.getBytes(StandardCharsets.UTF_8));
        return file.toFile();
    }

    private static SdkHttpRequest capturedRequest() {
        assertThat(RequestCapture.REQUESTS).hasSize(1);
        return RequestCapture.REQUESTS.get(0);
    }

    /** An S3A filesystem whose requests stop at {@link RequestCapture}, before the network. */
    private static S3AFileSystem newFileSystem(String checksumAlgorithm) throws Exception {
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
        conf.set(CHECKSUM_ALGORITHM, checksumAlgorithm);
        conf.set(AUDIT_EXECUTION_INTERCEPTORS, RequestCapture.class.getName());
        final S3AFileSystem s3a = new S3AFileSystem();
        s3a.initialize(new URI("s3a://bucket/"), conf);
        return s3a;
    }

    /** Records each signed HTTP request and fails it, so no request leaves the JVM. */
    public static final class RequestCapture implements ExecutionInterceptor {

        static final String CAPTURED = "Request captured";

        static final List<SdkHttpRequest> REQUESTS = new CopyOnWriteArrayList<>();

        @Override
        public void beforeTransmission(
                Context.BeforeTransmission context, ExecutionAttributes executionAttributes) {
            REQUESTS.add(context.httpRequest());
            throw new IllegalStateException(CAPTURED);
        }
    }
}
