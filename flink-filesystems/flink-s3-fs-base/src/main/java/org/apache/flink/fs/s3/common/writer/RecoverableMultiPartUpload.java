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

package org.apache.flink.fs.s3.common.writer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.fs.RecoverableFsDataOutputStream;
import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.fs.s3.common.utils.RefCountedFSOutputStream;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.Optional;

/**
 * An uploader for parts of a multipart upload (MPU). The caller can add parts to the MPU and
 * snapshot its state to be recovered after a failure.
 */
@Internal
interface RecoverableMultiPartUpload {

    /**
     * Creates a snapshot of this MultiPartUpload (MPU) and returns a {@link
     * RecoverableFsDataOutputStream.Committer Committer} which can be used to finalize the MPU.
     *
     * @return The {@link RecoverableFsDataOutputStream.Committer Committer} that can be used to
     *     complete the MPU.
     */
    RecoverableFsDataOutputStream.Committer snapshotAndGetCommitter() throws IOException;

    /**
     * Creates a snapshot of this MultiPartUpload, from which the upload can be resumed.
     *
     * @param incompletePartFile The file containing the in-progress part which has not yet reached
     *     the minimum part size in order to be uploaded.
     * @return The {@link RecoverableWriter.ResumeRecoverable ResumeRecoverable} which can be used
     *     to resume the upload.
     */
    RecoverableWriter.ResumeRecoverable snapshotAndGetRecoverable(
            @Nullable final RefCountedFSOutputStream incompletePartFile) throws IOException;

    /**
     * Adds a part to the uploads without any size limitations.
     *
     * @param file The file with the part data.
     * @throws IOException If this method throws an exception, the RecoverableS3MultiPartUpload
     *     should not be used any more, but recovered instead.
     */
    void uploadPart(final RefCountedFSOutputStream file) throws IOException;

    /**
     * In case of an incomplete part which had not reached the minimum part size at the time of
     * snapshotting, this returns a file containing the in-progress part at which writing can be
     * resumed.
     */
    Optional<File> getIncompletePart();
}
