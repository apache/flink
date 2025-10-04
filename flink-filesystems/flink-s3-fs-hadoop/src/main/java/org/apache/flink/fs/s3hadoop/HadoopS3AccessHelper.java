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

import org.apache.flink.fs.s3.common.adapter.v2.AwsSdkV2Adapters;
import org.apache.flink.fs.s3.common.model.FlinkCompleteMultipartUploadResult;
import org.apache.flink.fs.s3.common.model.FlinkObjectMetadata;
import org.apache.flink.fs.s3.common.model.FlinkPartETag;
import org.apache.flink.fs.s3.common.model.FlinkPutObjectResult;
import org.apache.flink.fs.s3.common.model.FlinkUploadPartResult;
import org.apache.flink.fs.s3.common.writer.S3AccessHelper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.S3AUtils;
import org.apache.hadoop.fs.s3a.WriteOperationHelper;
import org.apache.hadoop.fs.s3a.impl.PutObjectOptions;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An implementation of the {@link S3AccessHelper} for the Hadoop S3A filesystem.
 *
 * <p>This implementation uses the AWS SDK v2 S3Client from Hadoop's S3AFileSystem to perform
 * low-level S3 operations required for Flink's recoverable writers. The S3Client is accessed via
 * {@link org.apache.hadoop.fs.s3a.S3AInternals#getAmazonS3Client(String)}, which is the recommended
 * approach for operations not available through the standard FileSystem API.
 */
public class HadoopS3AccessHelper implements S3AccessHelper {

    private final S3AFileSystem s3a;

    private final S3Client s3Client;

    private final String bucket;

    private final WriteOperationHelper writeHelper;

    private final PutObjectOptions putOptions;

    public HadoopS3AccessHelper(S3AFileSystem s3a, Configuration conf) {
        this.s3a = checkNotNull(s3a);
        // Use the S3AInternals API to get the S3Client for low-level operations
        // This bypasses some S3A operations (like auditing) but is necessary for
        // multipart upload operations that aren't exposed through the FileSystem API
        this.s3Client =
                s3a.getS3AInternals()
                        .getAmazonS3Client("Flink recoverable writer multipart upload operations");
        this.bucket = s3a.getBucket();
        this.writeHelper = s3a.getWriteOperationHelper();
        this.putOptions = PutObjectOptions.defaultOptions();
    }

    @Override
    public String startMultiPartUpload(String key) throws IOException {
        try {
            CreateMultipartUploadRequest request =
                    CreateMultipartUploadRequest.builder().bucket(bucket).key(key).build();

            CreateMultipartUploadResponse response = s3Client.createMultipartUpload(request);
            return response.uploadId();
        } catch (SdkException e) {
            throw new IOException("Failed to start multipart upload for key: " + key, e);
        }
    }

    @Override
    public FlinkUploadPartResult uploadPart(
            String key, String uploadId, int partNumber, File inputFile, long length)
            throws IOException {
        try {
            UploadPartRequest request =
                    UploadPartRequest.builder()
                            .bucket(bucket)
                            .key(key)
                            .uploadId(uploadId)
                            .partNumber(partNumber)
                            .contentLength(length)
                            .build();

            RequestBody requestBody = RequestBody.fromFile(Paths.get(inputFile.getAbsolutePath()));
            UploadPartResponse response = s3Client.uploadPart(request, requestBody);

            return AwsSdkV2Adapters.toFlinkUploadPartResult(response, partNumber);
        } catch (SdkException e) {
            throw new IOException("Failed to upload part " + partNumber + " for key: " + key, e);
        }
    }

    @Override
    public FlinkPutObjectResult putObject(String key, File inputFile) throws IOException {
        try {
            PutObjectRequest request = PutObjectRequest.builder().bucket(bucket).key(key).build();

            RequestBody requestBody = RequestBody.fromFile(Paths.get(inputFile.getAbsolutePath()));
            PutObjectResponse response = s3Client.putObject(request, requestBody);

            return AwsSdkV2Adapters.toFlinkPutObjectResult(response);
        } catch (SdkException e) {
            throw new IOException("Failed to put object for key: " + key, e);
        }
    }

    @Override
    public FlinkCompleteMultipartUploadResult commitMultiPartUpload(
            String destKey,
            String uploadId,
            List<FlinkPartETag> partETags,
            long length,
            AtomicInteger errorCount)
            throws IOException {
        // Convert Flink part ETags to AWS SDK v2 CompletedPart
        final List<CompletedPart> completedParts =
                partETags.stream()
                        .map(AwsSdkV2Adapters::toAwsCompletedPart)
                        .collect(Collectors.toList());

        // Use Hadoop's WriteOperationHelper which provides retry logic, error handling,
        // and integration with S3A statistics and auditing
        CompleteMultipartUploadResponse response =
                writeHelper.completeMPUwithRetries(
                        destKey, uploadId, completedParts, length, errorCount, putOptions);

        return AwsSdkV2Adapters.toFlinkCompleteMultipartUploadResult(response);
    }

    @Override
    public boolean deleteObject(String key) throws IOException {
        try {
            DeleteObjectRequest request =
                    DeleteObjectRequest.builder().bucket(bucket).key(key).build();
            s3Client.deleteObject(request);
            return true;
        } catch (SdkException e) {
            throw new IOException("Failed to delete object for key: " + key, e);
        }
    }

    @Override
    public long getObject(String key, File targetLocation) throws IOException {
        long numBytes = 0L;
        try (final OutputStream outStream = new FileOutputStream(targetLocation)) {
            GetObjectRequest request = GetObjectRequest.builder().bucket(bucket).key(key).build();

            try (InputStream inStream = s3Client.getObject(request)) {
                final byte[] buffer = new byte[32 * 1024];
                int numRead;
                while ((numRead = inStream.read(buffer)) != -1) {
                    outStream.write(buffer, 0, numRead);
                    numBytes += numRead;
                }
            }
        } catch (SdkException e) {
            throw new IOException("Failed to get object for key: " + key, e);
        }

        // some sanity checks
        if (numBytes != targetLocation.length()) {
            throw new IOException(
                    String.format(
                            "Error recovering writer: "
                                    + "Downloading the last data chunk file gives incorrect length. "
                                    + "File=%d bytes, Stream=%d bytes",
                            targetLocation.length(), numBytes));
        }

        return numBytes;
    }

    @Override
    public FlinkObjectMetadata getObjectMetadata(String key) throws IOException {
        try {
            HeadObjectRequest request = HeadObjectRequest.builder().bucket(bucket).key(key).build();
            HeadObjectResponse response = s3Client.headObject(request);
            return AwsSdkV2Adapters.toFlinkObjectMetadata(response);
        } catch (SdkException e) {
            throw S3AUtils.translateException("getObjectMetadata", key, e);
        }
    }
}
