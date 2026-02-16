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

/**
 * Helper class for S3 operations including multipart uploads, object storage, and retrieval.
 *
 * <p><b>Retry Handling:</b> This class relies on the AWS SDK's built-in retry mechanism which is
 * configured in {@link org.apache.flink.fs.s3native.S3ClientProvider}. The SDK automatically
 * handles retries for transient errors including:
 *
 * <ul>
 *   <li>5xx server errors (including S3 throttling which manifests as 503 Service Unavailable)
 *   <li>Network timeouts and connection errors
 *   <li>Request timeouts
 * </ul>
 *
 * <p>Terminal (non-retriable) errors include:
 *
 * <ul>
 *   <li>4xx client errors (400 Bad Request, 403 Forbidden, 404 Not Found)
 *   <li>Authentication/authorization failures
 *   <li>Invalid bucket or key names
 * </ul>
 *
 * <p><b>Encryption Support:</b> Currently supports SSE-S3 and SSE-KMS encryption modes. The
 * encryption logic is applied through the {@link S3EncryptionConfig} class. Future enhancements may
 * include:
 *
 * <ul>
 *   <li>SSE-C (customer-provided keys) via a KeyProvider interface
 *   <li>Client-side encryption via an EncryptionHandler interface
 *   <li>Encryption context for SSE-KMS (see HADOOP-19197)
 * </ul>
 *
 * <p><b>S3 URI Handling:</b> The {@link #extractKey(Path)} and {@link #extractBucketName(Path)}
 * methods expect URIs in the standard {@code s3://bucket/key} format. Other formats like path-style
 * ({@code https://s3.amazonaws.com/bucket/key}) or virtual-hosted-style ({@code
 * https://bucket.s3.amazonaws.com/key}) are not currently supported.
 */
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
        if (encryptionConfig.getEncryptionType() == S3EncryptionConfig.EncryptionType.SSE_KMS) {
            if (encryptionConfig.getKmsKeyId() != null) {
                requestBuilder.ssekmsKeyId(encryptionConfig.getKmsKeyId());
            }
            if (encryptionConfig.hasEncryptionContext()) {
                requestBuilder.ssekmsEncryptionContext(
                        serializeEncryptionContext(encryptionConfig.getEncryptionContext()));
            }
        }
    }

    private void applyEncryption(PutObjectRequest.Builder requestBuilder) {
        if (!encryptionConfig.isEnabled()) {
            return;
        }
        requestBuilder.serverSideEncryption(encryptionConfig.getServerSideEncryption());
        if (encryptionConfig.getEncryptionType() == S3EncryptionConfig.EncryptionType.SSE_KMS) {
            if (encryptionConfig.getKmsKeyId() != null) {
                requestBuilder.ssekmsKeyId(encryptionConfig.getKmsKeyId());
            }
            if (encryptionConfig.hasEncryptionContext()) {
                requestBuilder.ssekmsEncryptionContext(
                        serializeEncryptionContext(encryptionConfig.getEncryptionContext()));
            }
        }
    }

    /**
     * Serializes the encryption context map to a Base64-encoded JSON string as required by S3 API.
     */
    private String serializeEncryptionContext(java.util.Map<String, String> context) {
        StringBuilder json = new StringBuilder("{");
        boolean first = true;
        for (java.util.Map.Entry<String, String> entry : context.entrySet()) {
            if (!first) {
                json.append(",");
            }
            json.append("\"")
                    .append(escapeJson(entry.getKey()))
                    .append("\":\"")
                    .append(escapeJson(entry.getValue()))
                    .append("\"");
            first = false;
        }
        json.append("}");
        return java.util.Base64.getEncoder().encodeToString(json.toString().getBytes());
    }

    private String escapeJson(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
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
            return putObjectViaTransferManager(key, inputFile);
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

    /**
     * Uploads an object using the S3TransferManager for better throughput.
     *
     * <p>The transfer manager provides optimizations over the basic S3 client:
     *
     * <ul>
     *   <li>Automatic multipart upload handling for large files
     *   <li>Optimized part sizes and parallelism
     *   <li>Better memory management through streaming
     * </ul>
     *
     * <p>This method blocks until the upload completes.
     */
    private PutObjectResult putObjectViaTransferManager(String key, File inputFile)
            throws IOException {
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
                                                    == S3EncryptionConfig.EncryptionType.SSE_KMS) {
                                                if (encryptionConfig.getKmsKeyId() != null) {
                                                    req.ssekmsKeyId(encryptionConfig.getKmsKeyId());
                                                }
                                                if (encryptionConfig.hasEncryptionContext()) {
                                                    req.ssekmsEncryptionContext(
                                                            serializeEncryptionContext(
                                                                    encryptionConfig
                                                                            .getEncryptionContext()));
                                                }
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

    /**
     * Completes a multipart upload by assembling previously uploaded parts.
     *
     * <p><b>Recovery Scenario:</b> If a {@link NoSuchUploadException} is thrown, this may indicate
     * that the upload was already completed (possibly by a previous attempt during recovery). In
     * this case, we check if the object exists and return its metadata. This handles the scenario
     * where:
     *
     * <ol>
     *   <li>A multipart upload was started and parts were uploaded
     *   <li>The complete request succeeded but the response was lost (e.g., network issue)
     *   <li>A retry attempt finds the upload already completed and the object present
     * </ol>
     *
     * <p>The object metadata returned in this recovery path can be trusted as S3 guarantees strong
     * consistency for read-after-write operations.
     */
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

    /**
     * Deletes an object from S3.
     *
     * <p><b>Permission Considerations:</b> Note that S3 may return different error codes depending
     * on permissions:
     *
     * <ul>
     *   <li>With ListBucket permission: Returns 404 if object doesn't exist
     *   <li>Without ListBucket permission: Returns 403 (Access Denied) even for non-existent
     *       objects (for security reasons, to prevent object enumeration)
     * </ul>
     *
     * <p>This method treats 404 as "object not found" (returns false), while 403 and other errors
     * are propagated as IOException. If your IAM policy doesn't include ListBucket, be aware that
     * attempting to delete non-existent objects will throw an exception.
     *
     * @param key the S3 object key to delete
     * @return true if the object was deleted, false if it didn't exist (requires ListBucket
     *     permission)
     * @throws IOException if the deletion fails due to permissions, throttling, or other errors
     */
    public boolean deleteObject(String key) throws IOException {
        try {
            DeleteObjectRequest request =
                    DeleteObjectRequest.builder().bucket(bucketName).key(key).build();

            s3Client.deleteObject(request);
            return true;
        } catch (S3Exception e) {
            if (e.statusCode() == 404) {
                LOG.debug("Object not found during delete for key: {}", key);
                return false;
            }
            throw new IOException(
                    String.format(
                            "Failed to delete object for key: %s (HTTP %d: %s)",
                            key, e.statusCode(), e.awsErrorDetails().errorMessage()),
                    e);
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

    /**
     * Extracts the S3 object key from a Flink Path.
     *
     * <p>Expected URI format: {@code s3://bucket-name/path/to/object}
     *
     * <p><b>Limitations:</b> This method only supports the standard S3 URI format. Other URI
     * formats are NOT supported:
     *
     * <ul>
     *   <li>{@code https://bucket.s3.amazonaws.com/path/to/object} (virtual-hosted style)
     *   <li>{@code https://s3.amazonaws.com/bucket/path/to/object} (path style)
     *   <li>{@code s3a://} or {@code s3n://} schemes (Hadoop-specific)
     * </ul>
     *
     * @param path the Flink Path with s3:// scheme
     * @return the object key (path portion without leading slash)
     */
    public static String extractKey(Path path) {
        String pathStr = path.toUri().getPath();
        if (pathStr.startsWith("/")) {
            pathStr = pathStr.substring(1);
        }
        return pathStr;
    }

    /**
     * Extracts the S3 bucket name from a Flink Path.
     *
     * <p>Expected URI format: {@code s3://bucket-name/path/to/object}
     *
     * @param path the Flink Path with s3:// scheme
     * @return the bucket name (host portion of the URI)
     * @see #extractKey(Path) for URI format limitations
     */
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
