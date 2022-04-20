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
import org.apache.flink.util.MathUtils;

import com.amazonaws.SdkBaseException;
import com.amazonaws.services.s3.model.CompleteMultipartUploadResult;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.S3AUtils;
import org.apache.hadoop.fs.s3a.WriteOperationHelper;
import org.apache.hadoop.fs.s3a.statistics.S3AStatisticsContext;
import org.apache.hadoop.fs.store.audit.AuditSpan;
import org.apache.hadoop.fs.store.audit.AuditSpanSource;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** An implementation of the {@link S3AccessHelper} for the Hadoop S3A filesystem. */
public class HadoopS3AccessHelper implements S3AccessHelper {

    private final S3AFileSystem s3a;

    private final InternalWriteOperationHelper s3accessHelper;

    public HadoopS3AccessHelper(S3AFileSystem s3a, Configuration conf) {
        checkNotNull(s3a);
        this.s3accessHelper =
                new InternalWriteOperationHelper(
                        s3a,
                        checkNotNull(conf),
                        s3a.createStoreContext().getInstrumentation(),
                        s3a.getAuditSpanSource(),
                        s3a.getActiveAuditSpan());
        this.s3a = s3a;
    }

    @Override
    public String startMultiPartUpload(String key) throws IOException {
        return s3accessHelper.initiateMultiPartUpload(key);
    }

    @Override
    public UploadPartResult uploadPart(
            String key, String uploadId, int partNumber, File inputFile, long length)
            throws IOException {
        final UploadPartRequest uploadRequest =
                s3accessHelper.newUploadPartRequest(
                        key,
                        uploadId,
                        partNumber,
                        MathUtils.checkedDownCast(length),
                        null,
                        inputFile,
                        0L);
        return s3accessHelper.uploadPart(uploadRequest);
    }

    @Override
    public PutObjectResult putObject(String key, File inputFile) throws IOException {
        final PutObjectRequest putRequest = s3accessHelper.createPutObjectRequest(key, inputFile);
        return s3accessHelper.putObject(putRequest);
    }

    @Override
    public CompleteMultipartUploadResult commitMultiPartUpload(
            String destKey,
            String uploadId,
            List<PartETag> partETags,
            long length,
            AtomicInteger errorCount)
            throws IOException {
        return s3accessHelper.completeMPUwithRetries(
                destKey, uploadId, partETags, length, errorCount);
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
    public ObjectMetadata getObjectMetadata(String key) throws IOException {
        try {
            return s3a.getObjectMetadata(new Path('/' + key));
        } catch (SdkBaseException e) {
            throw S3AUtils.translateException("getObjectMetadata", key, e);
        }
    }

    /**
     * Internal {@link WriteOperationHelper} that is wrapped so that it only exposes the
     * functionality we need for the {@link S3AccessHelper}.
     */
    private static final class InternalWriteOperationHelper extends WriteOperationHelper {

        InternalWriteOperationHelper(
                S3AFileSystem owner,
                Configuration conf,
                S3AStatisticsContext statisticsContext,
                AuditSpanSource auditSpanSource,
                AuditSpan auditSpan) {
            super(owner, conf, statisticsContext, auditSpanSource, auditSpan);
        }
    }
}
