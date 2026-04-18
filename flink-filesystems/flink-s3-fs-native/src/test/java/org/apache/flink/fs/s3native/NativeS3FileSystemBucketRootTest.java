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

import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.Path;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.s3.DelegatingS3Client;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchBucketException;
import software.amazon.awssdk.transfer.s3.S3TransferManager;

import java.io.FileNotFoundException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for bucket-root paths (empty S3 object key) in {@link NativeS3FileSystem}. */
class NativeS3FileSystemBucketRootTest {

    @Test
    void getFileStatus_bucketRoot_usesHeadBucketNotHeadObject() throws Exception {
        TrackingBucketRootS3Client s3Client = new TrackingBucketRootS3Client(false);
        S3TransferManager transferManager = newNoOpTransferManager();

        S3ClientProvider provider =
                S3ClientProvider.createForTesting(
                        s3Client,
                        transferManager,
                        Duration.ofSeconds(30),
                        Duration.ofSeconds(60),
                        Duration.ofSeconds(60),
                        Duration.ofSeconds(60));

        NativeS3FileSystem fs =
                new NativeS3FileSystem(
                        provider,
                        URI.create("s3://warehouse-bucket"),
                        null,
                        0,
                        System.getProperty("java.io.tmpdir"),
                        5L * 1024 * 1024,
                        4,
                        null,
                        false,
                        256 * 1024,
                        Duration.ofSeconds(30));
        try {
            FileStatus status = fs.getFileStatus(new Path("s3://warehouse-bucket"));
            assertThat(status.isDir()).isTrue();

            assertThat(s3Client.getHeadBucketCallCount()).isEqualTo(1);
            assertThat(s3Client.getHeadObjectCallCount()).isZero();
        } finally {
            fs.closeAsync().join();
        }
    }

    @Test
    void getFileStatus_bucketRoot_missingBucket_throwsFileNotFound() {
        TrackingBucketRootS3Client s3Client = new TrackingBucketRootS3Client(true);
        S3TransferManager transferManager = newNoOpTransferManager();

        S3ClientProvider provider =
                S3ClientProvider.createForTesting(
                        s3Client,
                        transferManager,
                        Duration.ofSeconds(30),
                        Duration.ofSeconds(60),
                        Duration.ofSeconds(60),
                        Duration.ofSeconds(60));

        NativeS3FileSystem fs =
                new NativeS3FileSystem(
                        provider,
                        URI.create("s3://missing-bucket"),
                        null,
                        0,
                        System.getProperty("java.io.tmpdir"),
                        5L * 1024 * 1024,
                        4,
                        null,
                        false,
                        256 * 1024,
                        Duration.ofSeconds(30));
        try {
            assertThatThrownBy(() -> fs.getFileStatus(new Path("s3://missing-bucket")))
                    .isInstanceOf(FileNotFoundException.class);
        } finally {
            fs.closeAsync().join();
        }
    }

    /**
     * {@link DelegatingS3Client} requires a non-null delegate; this proxy only implements {@link
     * S3Client#close()} so the delegate is never exercised for S3 calls we override.
     */
    private static S3Client newUnsupportedDelegateExceptClose() {
        InvocationHandler handler =
                (proxy, method, args) -> {
                    if ("close".equals(method.getName())) {
                        return null;
                    }
                    if (method.getDeclaringClass() == Object.class) {
                        if ("equals".equals(method.getName())) {
                            return proxy == args[0];
                        }
                        if ("hashCode".equals(method.getName())) {
                            return System.identityHashCode(proxy);
                        }
                        if ("toString".equals(method.getName())) {
                            return "noopS3Client";
                        }
                    }
                    throw new UnsupportedOperationException(method.getName());
                };
        return (S3Client)
                Proxy.newProxyInstance(
                        S3Client.class.getClassLoader(), new Class<?>[] {S3Client.class}, handler);
    }

    private static S3TransferManager newNoOpTransferManager() {
        InvocationHandler handler =
                (proxy, method, args) -> {
                    if ("close".equals(method.getName())) {
                        return null;
                    }
                    if (method.getDeclaringClass() == Object.class) {
                        if ("equals".equals(method.getName())) {
                            return proxy == args[0];
                        }
                        if ("hashCode".equals(method.getName())) {
                            return System.identityHashCode(proxy);
                        }
                        if ("toString".equals(method.getName())) {
                            return "noopTransferManager";
                        }
                    }
                    throw new UnsupportedOperationException(method.getName());
                };
        return (S3TransferManager)
                Proxy.newProxyInstance(
                        S3TransferManager.class.getClassLoader(),
                        new Class<?>[] {S3TransferManager.class},
                        handler);
    }

    private static final class TrackingBucketRootS3Client extends DelegatingS3Client {

        private final AtomicInteger headBucketCalls = new AtomicInteger();
        private final AtomicInteger headObjectCalls = new AtomicInteger();
        private final boolean missingBucket;

        private TrackingBucketRootS3Client(boolean missingBucket) {
            super(newUnsupportedDelegateExceptClose());
            this.missingBucket = missingBucket;
        }

        int getHeadBucketCallCount() {
            return headBucketCalls.get();
        }

        int getHeadObjectCallCount() {
            return headObjectCalls.get();
        }

        @Override
        public HeadBucketResponse headBucket(HeadBucketRequest request) {
            headBucketCalls.incrementAndGet();
            if (missingBucket) {
                throw NoSuchBucketException.builder().message("no bucket").build();
            }
            return HeadBucketResponse.builder().build();
        }

        @Override
        public HeadObjectResponse headObject(HeadObjectRequest request) {
            headObjectCalls.incrementAndGet();
            throw new IllegalStateException("HeadObject must not be used for bucket root");
        }

        @Override
        public void close() {
            // Do not delegate to the unsupported proxy beyond close().
        }
    }
}
