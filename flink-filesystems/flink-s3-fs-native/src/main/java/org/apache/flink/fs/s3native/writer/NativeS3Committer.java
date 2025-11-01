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

import org.apache.flink.core.fs.RecoverableFsDataOutputStream;
import org.apache.flink.core.fs.RecoverableWriter;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Committer for S3 multipart uploads that finalizes the upload when commit is called.
 *
 * <p><b>Failure Handling:</b> A commit failure will propagate as an IOException, which typically
 * causes the Flink job to fail and trigger recovery. This is intentional because:
 *
 * <ul>
 *   <li>S3 requires all multipart uploads to be explicitly committed or aborted
 *   <li>A failed commit means the file is not finalized and data would be lost
 *   <li>Flink's checkpoint mechanism will handle recovery from the last successful checkpoint
 * </ul>
 *
 * <p>The "empty parts" check is a defensive measure against programming errors - in normal
 * operation, a multipart upload should always have at least one part before committing.
 */
public class NativeS3Committer implements RecoverableFsDataOutputStream.Committer {

    private final NativeS3AccessHelper s3AccessHelper;
    private final NativeS3Recoverable recoverable;
    private final AtomicInteger errorCount;

    public NativeS3Committer(NativeS3AccessHelper s3AccessHelper, NativeS3Recoverable recoverable) {
        this.s3AccessHelper = s3AccessHelper;
        this.recoverable = recoverable;
        this.errorCount = new AtomicInteger(0);
    }

    /**
     * Commits the multipart upload to finalize the S3 object.
     *
     * <p><b>Empty Parts Check:</b> Attempting to commit with no parts is considered a programming
     * error and will throw an IOException. This should not happen in normal operation as at least
     * one part must be uploaded before committing. If this exception is thrown, it indicates a bug
     * in the calling code or corruption of the recoverable state.
     *
     * @throws IOException if the commit fails or if attempting to commit with no parts
     */
    @Override
    public void commit() throws IOException {
        if (recoverable.parts().isEmpty()) {
            throw new IOException(
                    "Cannot commit empty multipart upload for object: "
                            + recoverable.getObjectName()
                            + ". This indicates a programming error - at least one part "
                            + "must be uploaded before committing.");
        }

        s3AccessHelper.commitMultiPartUpload(
                recoverable.getObjectName(),
                recoverable.uploadId(),
                recoverable.parts().stream()
                        .map(
                                part ->
                                        new NativeS3AccessHelper.UploadPartResult(
                                                part.getPartNumber(), part.getETag()))
                        .collect(Collectors.toList()),
                recoverable.numBytesInParts(),
                errorCount);
    }

    @Override
    public void commitAfterRecovery() throws IOException {
        commit();
    }

    @Override
    public RecoverableWriter.CommitRecoverable getRecoverable() {
        return recoverable;
    }
}
