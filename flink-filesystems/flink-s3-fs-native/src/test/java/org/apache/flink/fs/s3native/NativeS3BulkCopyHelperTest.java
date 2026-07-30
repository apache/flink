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

import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.PathsCopyingFileSystem.CopyRequest;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link NativeS3BulkCopyHelper}. */
class NativeS3BulkCopyHelperTest {

    private static final NativeS3BulkCopyHelper helper =
            new NativeS3BulkCopyHelper(null, 1, 1, 256 * 1024);

    // --- URI parsing tests ---

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

    private static Stream<Arguments> connectionPoolExhaustedCases() {
        return Stream.of(
                Arguments.of(
                        "direct message match",
                        SdkClientException.builder()
                                .message(
                                        "Unable to execute HTTP request: "
                                                + "Acquire operation took longer than the configured maximum time.")
                                .build(),
                        true),
                Arguments.of(
                        "nested causal chain",
                        SdkClientException.builder()
                                .message("Unable to execute HTTP request")
                                .cause(
                                        new RuntimeException(
                                                "channel acquisition failed",
                                                new TimeoutException(
                                                        "Acquire operation took longer than 10000 milliseconds.")))
                                .build(),
                        true),
                Arguments.of(
                        "unrelated error",
                        SdkClientException.builder().message("Access Denied").build(),
                        false),
                Arguments.of("null message", new RuntimeException((String) null), false),
                Arguments.of("null throwable", null, false));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("connectionPoolExhaustedCases")
    void testConnectionPoolExhaustedDetection(
            String description, Throwable throwable, boolean expected) {
        assertThat(NativeS3BulkCopyHelper.isConnectionPoolExhausted(throwable)).isEqualTo(expected);
    }

    @Test
    void testEmptyRequestListIsNoOp() throws Exception {
        NativeS3BulkCopyHelper noOpHelper = new NativeS3BulkCopyHelper(null, 16, 50, 256 * 1024);
        noOpHelper.copyFiles(Collections.emptyList(), null);
    }

    // --- download buffer size tests ---

    @Test
    void testDownloadBufferSizeIsExposed() {
        NativeS3BulkCopyHelper h = new NativeS3BulkCopyHelper(null, 1, 1, 128 * 1024);
        assertThat(h.getDownloadBufferSize()).isEqualTo(128 * 1024);
    }

    @Test
    void testNonPositiveDownloadBufferSizeRejected() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> new NativeS3BulkCopyHelper(null, 1, 1, 0));
    }

    @Test
    void testCopyFilesCancellationClosesActiveResponseStream(@TempDir Path tempDir)
            throws Exception {
        BlockingInputStream blockingStream = new BlockingInputStream();
        ResponseInputStream<GetObjectResponse> responseStream =
                new ResponseInputStream<>(GetObjectResponse.builder().build(), blockingStream);
        NativeS3BulkCopyHelper h =
                new NativeS3BulkCopyHelper(
                        asyncClientReturning(CompletableFuture.completedFuture(responseStream)),
                        1,
                        1,
                        1024);
        CloseableRegistry registry = new CloseableRegistry();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CompletableFuture<Throwable> copyFailure = new CompletableFuture<>();

        try {
            executor.submit(
                    () -> {
                        try {
                            h.copyFiles(
                                    Collections.singletonList(
                                            CopyRequest.of(
                                                    new org.apache.flink.core.fs.Path(
                                                            "s3://bucket/key"),
                                                    new org.apache.flink.core.fs.Path(
                                                            tempDir.resolve("out").toUri()),
                                                    1L)),
                                    registry);
                            copyFailure.complete(null);
                        } catch (Throwable t) {
                            copyFailure.complete(t);
                        }
                    });

            assertThat(blockingStream.awaitReadStarted()).isTrue();
            registry.close();

            assertThat(copyFailure.get(10, TimeUnit.SECONDS)).isInstanceOf(IOException.class);
            assertThat(blockingStream.awaitClosed()).isTrue();
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void testCopyFilesClosesResponseStreamCompletedAfterCancellation(@TempDir Path tempDir)
            throws Exception {
        BlockingInputStream blockingStream = new BlockingInputStream();
        ResponseInputStream<GetObjectResponse> responseStream =
                new ResponseInputStream<>(GetObjectResponse.builder().build(), blockingStream);
        NonCancellingCompletableFuture<ResponseInputStream<GetObjectResponse>> responseFuture =
                new NonCancellingCompletableFuture<>();
        CountDownLatch getObjectCalled = new CountDownLatch(1);
        NativeS3BulkCopyHelper h =
                new NativeS3BulkCopyHelper(
                        asyncClientReturning(responseFuture, getObjectCalled), 1, 1, 1024);
        CloseableRegistry registry = new CloseableRegistry();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CompletableFuture<Throwable> copyFailure = new CompletableFuture<>();

        try {
            executor.submit(
                    () -> {
                        try {
                            h.copyFiles(
                                    Collections.singletonList(
                                            CopyRequest.of(
                                                    new org.apache.flink.core.fs.Path(
                                                            "s3://bucket/key"),
                                                    new org.apache.flink.core.fs.Path(
                                                            tempDir.resolve("out").toUri()),
                                                    1L)),
                                    registry);
                            copyFailure.complete(null);
                        } catch (Throwable t) {
                            copyFailure.complete(t);
                        }
                    });

            assertThat(getObjectCalled.await(10, TimeUnit.SECONDS)).isTrue();
            registry.close();
            responseFuture.complete(responseStream);

            assertThat(copyFailure.get(10, TimeUnit.SECONDS)).isInstanceOf(IOException.class);
            assertThat(blockingStream.awaitClosed()).isTrue();
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void testCopyFilesDeletesTemporaryFileWhenRequestFails(@TempDir Path tempDir) {
        NativeS3BulkCopyHelper h =
                new NativeS3BulkCopyHelper(
                        asyncClientReturning(
                                CompletableFuture.failedFuture(new IOException("boom"))),
                        1,
                        1,
                        1024);

        assertThatThrownBy(
                        () ->
                                h.copyFiles(
                                        Collections.singletonList(
                                                CopyRequest.of(
                                                        new org.apache.flink.core.fs.Path(
                                                                "s3://bucket/key"),
                                                        new org.apache.flink.core.fs.Path(
                                                                tempDir.resolve("out").toUri()),
                                                        1L)),
                                        new CloseableRegistry()))
                .isInstanceOf(IOException.class);

        assertThat(tempDir).isEmptyDirectory();
    }

    @Test
    void testCopyFilesSupportsShortDestinationFileNames(@TempDir Path tempDir) throws Exception {
        ResponseInputStream<GetObjectResponse> responseStream =
                new ResponseInputStream<>(
                        GetObjectResponse.builder().build(),
                        new ByteArrayInputStream("data".getBytes()));
        NativeS3BulkCopyHelper h =
                new NativeS3BulkCopyHelper(
                        asyncClientReturning(CompletableFuture.completedFuture(responseStream)),
                        1,
                        1,
                        1024);
        Path destination = tempDir.resolve("x");

        h.copyFiles(
                Collections.singletonList(
                        CopyRequest.of(
                                new org.apache.flink.core.fs.Path("s3://bucket/key"),
                                new org.apache.flink.core.fs.Path(destination.toUri()),
                                4L)),
                new CloseableRegistry());

        assertThat(destination).hasContent("data");
    }

    @Test
    void testCopyFilesFailFastAbortsInFlightStreamsOnFirstFailure(@TempDir Path tempDir)
            throws Exception {
        BlockingInputStream blockingStream = new BlockingInputStream();
        ResponseInputStream<GetObjectResponse> blockingResponse =
                new ResponseInputStream<>(GetObjectResponse.builder().build(), blockingStream);
        java.util.concurrent.ConcurrentLinkedQueue<
                        CompletableFuture<ResponseInputStream<GetObjectResponse>>>
                responses = new java.util.concurrent.ConcurrentLinkedQueue<>();
        responses.add(CompletableFuture.completedFuture(blockingResponse));
        responses.add(CompletableFuture.failedFuture(new IOException("boom")));

        S3AsyncClient client =
                (S3AsyncClient)
                        Proxy.newProxyInstance(
                                S3AsyncClient.class.getClassLoader(),
                                new Class<?>[] {S3AsyncClient.class},
                                (proxy, method, args) -> {
                                    switch (method.getName()) {
                                        case "getObject":
                                            return responses.poll();
                                        case "close":
                                            return null;
                                        case "serviceName":
                                            return "s3";
                                        case "toString":
                                            return "test-s3-async-client";
                                        default:
                                            throw new UnsupportedOperationException(
                                                    method.toString());
                                    }
                                });
        NativeS3BulkCopyHelper h = new NativeS3BulkCopyHelper(client, 2, 2, 1024);
        CloseableRegistry registry = new CloseableRegistry();

        assertThatThrownBy(
                        () ->
                                h.copyFiles(
                                        java.util.Arrays.asList(
                                                CopyRequest.of(
                                                        new org.apache.flink.core.fs.Path(
                                                                "s3://bucket/blocking"),
                                                        new org.apache.flink.core.fs.Path(
                                                                tempDir.resolve("a").toUri()),
                                                        1L),
                                                CopyRequest.of(
                                                        new org.apache.flink.core.fs.Path(
                                                                "s3://bucket/failing"),
                                                        new org.apache.flink.core.fs.Path(
                                                                tempDir.resolve("b").toUri()),
                                                        1L)),
                                        registry))
                .isInstanceOf(IOException.class);

        assertThat(blockingStream.awaitClosed()).isTrue();
    }

    private static S3AsyncClient asyncClientReturning(
            CompletableFuture<ResponseInputStream<GetObjectResponse>> responseFuture) {
        return asyncClientReturning(responseFuture, null);
    }

    private static S3AsyncClient asyncClientReturning(
            CompletableFuture<ResponseInputStream<GetObjectResponse>> responseFuture,
            CountDownLatch getObjectCalled) {
        return (S3AsyncClient)
                Proxy.newProxyInstance(
                        S3AsyncClient.class.getClassLoader(),
                        new Class<?>[] {S3AsyncClient.class},
                        (proxy, method, args) -> {
                            if (method.getName().equals("getObject")
                                    && args != null
                                    && args.length == 2) {
                                if (getObjectCalled != null) {
                                    getObjectCalled.countDown();
                                }
                                return responseFuture;
                            }
                            if (method.getName().equals("close")) {
                                return null;
                            }
                            if (method.getName().equals("serviceName")) {
                                return "s3";
                            }
                            if (method.getName().equals("toString")) {
                                return "test-s3-async-client";
                            }
                            throw new UnsupportedOperationException(method.toString());
                        });
    }

    private static final class NonCancellingCompletableFuture<T> extends CompletableFuture<T> {
        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return false;
        }
    }

    private static final class BlockingInputStream extends InputStream {
        private boolean closed;
        private boolean readStarted;

        @Override
        public synchronized int read() throws IOException {
            byte[] buffer = new byte[1];
            return read(buffer, 0, 1);
        }

        @Override
        public synchronized int read(byte[] b, int off, int len) throws IOException {
            readStarted = true;
            notifyAll();
            while (!closed) {
                try {
                    wait();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("interrupted", e);
                }
            }
            throw new IOException("closed");
        }

        @Override
        public synchronized void close() {
            closed = true;
            notifyAll();
        }

        synchronized boolean awaitReadStarted() throws InterruptedException {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (!readStarted && System.nanoTime() < deadline) {
                TimeUnit.MILLISECONDS.timedWait(this, 10);
            }
            return readStarted;
        }

        synchronized boolean awaitClosed() throws InterruptedException {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (!closed && System.nanoTime() < deadline) {
                TimeUnit.MILLISECONDS.timedWait(this, 10);
            }
            return closed;
        }
    }
}
