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

import org.apache.flink.fs.s3.common.writer.PartETag;
import org.apache.flink.fs.s3.common.writer.S3AccessHelper;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3ADataBlocks;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.S3AInstrumentation;
import org.apache.hadoop.fs.s3a.WriteOperationHelper;
import org.apache.hadoop.fs.s3a.impl.PutObjectOptions;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.hadoop.fs.s3a.Statistic.OBJECT_MULTIPART_UPLOAD_INITIATED;
import static org.apache.hadoop.fs.statistics.impl.IOStatisticsBinding.trackDurationOfSupplier;

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

    public HadoopS3AccessHelper(S3AFileSystem s3a) {
        this.s3a = checkNotNull(s3a);
        this.writeHelper = s3a.getWriteOperationHelper();
    }

    /** Follows the directory marker policy for the key, like {@link S3AFileSystem} does. */
    private PutObjectOptions putOptions(String key) {
        return s3a.getDirectoryMarkerPolicy().keepDirectoryMarkers(s3a.keyToQualifiedPath(key))
                ? PutObjectOptions.keepingDirs()
                : PutObjectOptions.deletingDirs();
    }

    /**
     * Creates the upload without S3A's checksum algorithm. S3 completes an upload created with one
     * only if every part's checksum is sent again, but the recoverable state holds only part
     * numbers and ETags, and flink-s3-fs-native completes restored uploads with just those. The
     * parts still carry the checksum that S3A adds to every UploadPart, and S3 verifies it.
     *
     * <p>This follows {@link WriteOperationHelper#initiateMultiPartUpload}: the request is built
     * inside the retry, which activates the audit span, and the call counts towards the same S3A
     * statistic.
     */
    @Override
    public String startMultiPartUpload(String key) throws IOException {
        final S3AInstrumentation instrumentation = s3a.getInstrumentation();
        return writeHelper.retry(
                "initiate MultiPartUpload",
                key,
                true,
                () -> {
                    final CreateMultipartUploadRequest request =
                            s3a.getRequestFactory()
                                    .newMultipartUploadRequestBuilder(key, putOptions(key))
                                    .checksumAlgorithm((String) null)
                                    .build();
                    return trackDurationOfSupplier(
                                    instrumentation == null
                                            ? null
                                            : instrumentation.getDurationTrackerFactory(),
                                    OBJECT_MULTIPART_UPLOAD_INITIATED.getSymbol(),
                                    () ->
                                            s3a.getS3AInternals()
                                                    .getAmazonS3Client("FileSink multipart upload")
                                                    .createMultipartUpload(request))
                            .uploadId();
                });
    }

    @Override
    public PartETag uploadPart(
            String key, String uploadId, int partNumber, File inputFile, long length)
            throws IOException {
        UploadPartRequest request =
                writeHelper
                        .newUploadPartRequestBuilder(key, uploadId, partNumber, false, length)
                        .build();
        RequestBody body = RequestBody.fromFile(inputFile);
        return new PartETag(partNumber, writeHelper.uploadPart(request, body, null).eTag());
    }

    @Override
    public void putObject(String key, File inputFile) throws IOException {
        final PutObjectOptions options = putOptions(key);
        software.amazon.awssdk.services.s3.model.PutObjectRequest request =
                writeHelper.createPutObjectRequest(key, inputFile.length(), options);
        try (S3ADataBlocks.BlockUploadData uploadData =
                new S3ADataBlocks.BlockUploadData(inputFile, () -> true)) {
            writeHelper.putObject(request, options, uploadData, null);
        }
    }

    @Override
    public void commitMultiPartUpload(
            String destKey,
            String uploadId,
            List<PartETag> partETags,
            long length,
            AtomicInteger errorCount)
            throws IOException {
        final List<CompletedPart> completedParts = new ArrayList<>(partETags.size());
        for (PartETag part : partETags) {
            completedParts.add(
                    CompletedPart.builder()
                            .partNumber(part.getPartNumber())
                            .eTag(part.getETag())
                            .build());
        }
        writeHelper.completeMPUwithRetries(
                destKey, uploadId, completedParts, length, errorCount, putOptions(destKey));
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
    public long getObjectLength(String key) throws IOException {
        return s3a.getObjectMetadata(new Path('/' + key)).contentLength();
    }
}
