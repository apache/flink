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

public class NativeS3Committer implements RecoverableFsDataOutputStream.Committer {

    private final NativeS3AccessHelper s3AccessHelper;
    private final NativeS3Recoverable recoverable;
    private final AtomicInteger errorCount;

    public NativeS3Committer(NativeS3AccessHelper s3AccessHelper, NativeS3Recoverable recoverable) {
        this.s3AccessHelper = s3AccessHelper;
        this.recoverable = recoverable;
        this.errorCount = new AtomicInteger(0);
    }

    @Override
    public void commit() throws IOException {
        if (recoverable.parts().isEmpty()) {
            throw new IOException("Cannot commit empty multipart upload");
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
