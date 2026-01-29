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

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.fs.Path;
import org.apache.flink.fs.s3native.S3EncryptionConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CompletedMultipartUpload;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.NoSuchUploadException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.CompletedFileUpload;
import software.amazon.awssdk.transfer.s3.model.FileUpload;
import software.amazon.awssdk.transfer.s3.model.UploadFileRequest;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@Internal
public class NativeS3AccessHelper {

    private static final Logger LOG = LoggerFactory.getLogger(NativeS3AccessHelper.class);

    private final S3Client s3Client;
    private final S3AsyncClient s3AsyncClient;
    private final S3TransferManager transferManager;
    private final String bucketName;
    private final boolean useAsyncOperations;
    private final S3EncryptionConfig encryptionConfig;

    public NativeS3AccessHelper(S3Client s3Client, String bucketName) {
        this(s3Client, null, null, bucketName, false, null);
    }

    public NativeS3AccessHelper(
            S3Client s3Client,
            S3AsyncClient s3AsyncClient,
            S3TransferManager transferManager,
            String bucketName,
            boolean useAsyncOperations) {
        this(s3Client, s3AsyncClient, transferManager, bucketName, useAsyncOperations, null);
    }

    public NativeS3AccessHelper(
            S3Client s3Client,
            S3AsyncClient s3AsyncClient,
            S3TransferManager transferManager,
            String bucketName,
            boolean useAsyncOperations,
            S3EncryptionConfig encryptionConfig) {
        this.s3Client = s3Client;
        this.s3AsyncClient = s3AsyncClient;
        this.transferManager = transferManager;
        this.bucketName = bucketName;
        this.useAsyncOperations = useAsyncOperations && transferManager != null;
        this.encryptionConfig =
                encryptionConfig != null ? encryptionConfig : S3EncryptionConfig.none();
    }

    public String startMultiPartUpload(String key) throws IOException {
        try {
            CreateMultipartUploadRequest.Builder requestBuilder =
                    CreateMultipartUploadRequest.builder().bucket(bucketName).key(key);
            applyEncryption(requestBuilder);

            CreateMultipartUploadResponse response =
                    s3Client.createMultipartUpload(requestBuilder.build());
            return response.uploadId();
        } catch (S3Exception e) {
            throw new IOException("Failed to start multipart upload for key: " + key, e);
        }
    }

    private void applyEncryption(CreateMultipartUploadRequest.Builder requestBuilder) {
        if (!encryptionConfig.isEnabled()) {
            return;
        }
        requestBuilder.serverSideEncryption(encryptionConfig.getServerSideEncryption());
        if (encryptionConfig.getEncryptionType() == S3EncryptionConfig.EncryptionType.SSE_KMS
                && encryptionConfig.getKmsKeyId() != null) {
            requestBuilder.ssekmsKeyId(encryptionConfig.getKmsKeyId());
        }
    }

    private void applyEncryption(PutObjectRequest.Builder requestBuilder) {
        if (!encryptionConfig.isEnabled()) {
            return;
        }
        requestBuilder.serverSideEncryption(encryptionConfig.getServerSideEncryption());
        if (encryptionConfig.getEncryptionType() == S3EncryptionConfig.EncryptionType.SSE_KMS
                && encryptionConfig.getKmsKeyId() != null) {
            requestBuilder.ssekmsKeyId(encryptionConfig.getKmsKeyId());
        }
    }

    public UploadPartResult uploadPart(
            String key, String uploadId, int partNumber, File inputFile, long length)
            throws IOException {
        try {
            UploadPartRequest request =
                    UploadPartRequest.builder()
                            .bucket(bucketName)
                            .key(key)
                            .uploadId(uploadId)
                            .partNumber(partNumber)
                            .build();

            UploadPartResponse response =
                    s3Client.uploadPart(request, RequestBody.fromFile(inputFile));

            return new UploadPartResult(partNumber, response.eTag());
        } catch (S3Exception e) {
            throw new IOException(
                    String.format(
                            "Failed to upload part %d for key: %s, uploadId: %s",
                            partNumber, key, uploadId),
                    e);
        }
    }

    public PutObjectResult putObject(String key, File inputFile) throws IOException {
        if (useAsyncOperations && transferManager != null) {
            return putObjectAsync(key, inputFile);
        }

        try {
            PutObjectRequest.Builder requestBuilder =
                    PutObjectRequest.builder().bucket(bucketName).key(key);
            applyEncryption(requestBuilder);

            PutObjectResponse response =
                    s3Client.putObject(requestBuilder.build(), RequestBody.fromFile(inputFile));
            return new PutObjectResult(response.eTag());
        } catch (S3Exception e) {
            throw new IOException("Failed to put object for key: " + key, e);
        }
    }

    private PutObjectResult putObjectAsync(String key, File inputFile) throws IOException {
        try {
            UploadFileRequest uploadRequest =
                    UploadFileRequest.builder()
                            .putObjectRequest(
                                    req -> {
                                        req.bucket(bucketName).key(key);
                                        if (encryptionConfig.isEnabled()) {
                                            req.serverSideEncryption(
                                                    encryptionConfig.getServerSideEncryption());
                                            if (encryptionConfig.getEncryptionType()
                                                            == S3EncryptionConfig.EncryptionType
                                                                    .SSE_KMS
                                                    && encryptionConfig.getKmsKeyId() != null) {
                                                req.ssekmsKeyId(encryptionConfig.getKmsKeyId());
                                            }
                                        }
                                    })
                            .source(inputFile.toPath())
                            .build();

            FileUpload fileUpload = transferManager.uploadFile(uploadRequest);
            CompletedFileUpload completedUpload = fileUpload.completionFuture().join();
            return new PutObjectResult(completedUpload.response().eTag());
        } catch (Exception e) {
            throw new IOException("Failed to async upload object for key: " + key, e);
        }
    }

    public CompleteMultipartUploadResult commitMultiPartUpload(
            String key,
            String uploadId,
            List<UploadPartResult> partResults,
            long length,
            AtomicInteger errorCount)
            throws IOException {
        try {
            List<CompletedPart> completedParts =
                    partResults.stream()
                            .map(
                                    result ->
                                            CompletedPart.builder()
                                                    .partNumber(result.getPartNumber())
                                                    .eTag(result.getETag())
                                                    .build())
                            .collect(Collectors.toList());

            CompleteMultipartUploadRequest request =
                    CompleteMultipartUploadRequest.builder()
                            .bucket(bucketName)
                            .key(key)
                            .uploadId(uploadId)
                            .multipartUpload(
                                    CompletedMultipartUpload.builder()
                                            .parts(completedParts)
                                            .build())
                            .build();

            CompleteMultipartUploadResponse response = s3Client.completeMultipartUpload(request);
            return new CompleteMultipartUploadResult(
                    bucketName, key, response.eTag(), response.location());
        } catch (NoSuchUploadException e) {
            try {
                ObjectMetadata metadata = getObjectMetadata(key);
                return new CompleteMultipartUploadResult(bucketName, key, metadata.getETag(), null);
            } catch (IOException checkEx) {
                errorCount.incrementAndGet();
                throw new IOException("Failed to complete multipart upload for key: " + key, e);
            }
        } catch (S3Exception e) {
            errorCount.incrementAndGet();
            throw new IOException("Failed to complete multipart upload for key: " + key, e);
        }
    }

    public void abortMultiPartUpload(String key, String uploadId) throws IOException {
        try {
            AbortMultipartUploadRequest request =
                    AbortMultipartUploadRequest.builder()
                            .bucket(bucketName)
                            .key(key)
                            .uploadId(uploadId)
                            .build();

            s3Client.abortMultipartUpload(request);
        } catch (S3Exception e) {
            throw new IOException(
                    String.format(
                            "Failed to abort multipart upload for key: %s, uploadId: %s",
                            key, uploadId),
                    e);
        }
    }

    public boolean deleteObject(String key) throws IOException {
        try {
            DeleteObjectRequest request =
                    DeleteObjectRequest.builder().bucket(bucketName).key(key).build();

            s3Client.deleteObject(request);
            return true;
        } catch (S3Exception e) {
            if (e.statusCode() == 404) {
                return false;
            }
            throw new IOException("Failed to delete object for key: " + key, e);
        }
    }

    public long getObject(String key, File targetLocation) throws IOException {
        try {
            GetObjectRequest request =
                    GetObjectRequest.builder().bucket(bucketName).key(key).build();
            ResponseTransformer<GetObjectResponse, GetObjectResponse> responseTransformer =
                    ResponseTransformer.toFile(targetLocation.toPath());
            s3Client.getObject(request, responseTransformer);
            return Files.size(targetLocation.toPath());
        } catch (S3Exception e) {
            throw new IOException("Failed to get object for key: " + key, e);
        }
    }

    public ObjectMetadata getObjectMetadata(String key) throws IOException {
        try {
            HeadObjectRequest request =
                    HeadObjectRequest.builder().bucket(bucketName).key(key).build();
            HeadObjectResponse response = s3Client.headObject(request);
            return new ObjectMetadata(
                    response.contentLength(), response.eTag(), response.lastModified());
        } catch (S3Exception e) {
            throw new IOException("Failed to get metadata for key: " + key, e);
        }
    }

    public String getBucketName() {
        return bucketName;
    }

    public static String extractKey(Path path) {
        String pathStr = path.toUri().getPath();
        if (pathStr.startsWith("/")) {
            pathStr = pathStr.substring(1);
        }
        return pathStr;
    }

    public static String extractBucketName(Path path) {
        return path.toUri().getHost();
    }

    public static class UploadPartResult {
        private final int partNumber;
        private final String eTag;

        public UploadPartResult(int partNumber, String eTag) {
            this.partNumber = partNumber;
            this.eTag = eTag;
        }

        public int getPartNumber() {
            return partNumber;
        }

        public String getETag() {
            return eTag;
        }
    }

    public static class PutObjectResult {
        private final String eTag;

        public PutObjectResult(String eTag) {
            this.eTag = eTag;
        }

        public String getETag() {
            return eTag;
        }
    }

    public static class CompleteMultipartUploadResult {
        private final String bucketName;
        private final String key;
        private final String eTag;
        private final String location;

        public CompleteMultipartUploadResult(
                String bucketName, String key, String eTag, String location) {
            this.bucketName = bucketName;
            this.key = key;
            this.eTag = eTag;
            this.location = location;
        }

        public String getBucketName() {
            return bucketName;
        }

        public String getKey() {
            return key;
        }

        public String getETag() {
            return eTag;
        }

        public String getLocation() {
            return location;
        }
    }

    public static class ObjectMetadata {
        private final long contentLength;
        private final String eTag;
        private final java.time.Instant lastModified;

        public ObjectMetadata(long contentLength, String eTag, java.time.Instant lastModified) {
            this.contentLength = contentLength;
            this.eTag = eTag;
            this.lastModified = lastModified;
        }

        public long getContentLength() {
            return contentLength;
        }

        public String getETag() {
            return eTag;
        }

        public java.time.Instant getLastModified() {
            return lastModified;
        }
    }
}
