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

import org.apache.flink.fs.s3.common.writer.S3AccessHelper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3ADataBlocks;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.S3AUtils;
import org.apache.hadoop.fs.s3a.WriteOperationHelper;
import org.apache.hadoop.fs.s3a.impl.PutObjectOptions;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An implementation of the {@link S3AccessHelper} for the Hadoop S3A filesystem.
 *
 * <p>This implementation uses Hadoop's {@link WriteOperationHelper} to perform S3 operations,
 * similar to the SDK v1 implementation. This provides retry logic, error handling, S3A statistics
 * integration, and auditing support.
 */
public class HadoopS3AccessHelper implements S3AccessHelper {

    private final S3AFileSystem s3a;

    private final WriteOperationHelper writeHelper;

    private final PutObjectOptions putOptions;

    public HadoopS3AccessHelper(S3AFileSystem s3a, Configuration conf) {
        this.s3a = checkNotNull(s3a);
        // Get WriteOperationHelper from S3AFileSystem which properly initializes
        // it with callbacks, statistics, and audit support
        this.writeHelper = s3a.getWriteOperationHelper();
        this.putOptions = PutObjectOptions.defaultOptions();
    }

    @Override
    public String startMultiPartUpload(String key) throws IOException {
        return writeHelper.initiateMultiPartUpload(key, putOptions);
    }

    @Override
    public UploadPartResponse uploadPart(
            String key, String uploadId, int partNumber, File inputFile, long length)
            throws IOException {
        UploadPartRequest request =
                writeHelper
                        .newUploadPartRequestBuilder(key, uploadId, partNumber, false, length)
                        .build();
        RequestBody body = RequestBody.fromFile(inputFile);
        return writeHelper.uploadPart(request, body, null);
    }

    @Override
    public PutObjectResponse putObject(String key, File inputFile) throws IOException {
        // Use WriteOperationHelper for retry logic, statistics, and audit tracking
        software.amazon.awssdk.services.s3.model.PutObjectRequest request =
                writeHelper.createPutObjectRequest(key, inputFile.length(), putOptions);
        try (S3ADataBlocks.BlockUploadData uploadData =
                new S3ADataBlocks.BlockUploadData(inputFile, () -> true)) {
            return writeHelper.putObject(request, putOptions, uploadData, null);
        }
    }

    @Override
    public CompleteMultipartUploadResponse commitMultiPartUpload(
            String destKey,
            String uploadId,
            List<CompletedPart> partETags,
            long length,
            AtomicInteger errorCount)
            throws IOException {
        return writeHelper.completeMPUwithRetries(
                destKey, uploadId, partETags, length, errorCount, putOptions);
    }

    @Override
    public boolean deleteObject(String key) throws IOException {
        return s3a.delete(new org.apache.hadoop.fs.Path('/' + key), false);
    }

    @Override
    public long getObject(String key, File targetLocation) throws IOException {
        long numBytes = 0L;
        try (final OutputStream outStream = new FileOutputStream(targetLocation);
                final org.apache.hadoop.fs.FSDataInputStream inStream =
                        s3a.open(new org.apache.hadoop.fs.Path('/' + key))) {
            final byte[] buffer = new byte[32 * 1024];

            int numRead;
            while ((numRead = inStream.read(buffer)) != -1) {
                outStream.write(buffer, 0, numRead);
                numBytes += numRead;
            }
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
    public HeadObjectResponse getObjectMetadata(String key) throws IOException {
        try {
            return s3a.getObjectMetadata(new Path('/' + key));
        } catch (SdkException e) {
            throw S3AUtils.translateException("getObjectMetadata", key, e);
        }
    }
}
