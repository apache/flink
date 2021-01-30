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

package org.apache.flink.fs.gshadoop.writer;

import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.RecoverableFsDataOutputStream;
import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.net.URI;

/** Implementation of {@link org.apache.flink.core.fs.RecoverableWriter} for Google Storage. */
public class GSRecoverableWriter implements RecoverableWriter {

    private final GSRecoverableOptions options;

    /**
     * Construct a Google Storage recoverable writer.
     *
     * @param options The {@link org.apache.flink.fs.gshadoop.writer.GSRecoverableOptions} to use
     *     for this recoverable writer.
     */
    public GSRecoverableWriter(GSRecoverableOptions options) {
        this.options = Preconditions.checkNotNull(options);
    }

    @Override
    public RecoverableFsDataOutputStream open(Path path) throws IOException {
        Preconditions.checkNotNull(path);

        URI pathUri = path.toUri();
        String finalBucketName = pathUri.getAuthority();
        String finalObjectName = pathUri.getPath().substring(1); // remove leading forward slash

        GSRecoverablePlan plan = options.createPlan(finalBucketName, finalObjectName);
        GSRecoverableWriterHelper.Writer writer =
                options.helper
                        .getStorage()
                        .createWriter(
                                plan.tempBlobId,
                                options.uploadContentType,
                                options.uploadChunkSize);
        return new GSRecoverableFsDataOutputStream(options, plan, writer, 0);
    }

    @Override
    public boolean supportsResume() {
        return true;
    }

    @Override
    public RecoverableFsDataOutputStream recover(ResumeRecoverable resumable) {
        Preconditions.checkNotNull(resumable);

        GSRecoverable recoverable = (GSRecoverable) resumable;
        GSRecoverableWriterHelper.Writer writer = recoverable.writerState.restore();
        return new GSRecoverableFsDataOutputStream(
                options, recoverable.plan, writer, recoverable.position);
    }

    @Override
    public boolean requiresCleanupOfRecoverableState() {
        return true;
    }

    @Override
    public boolean cleanupRecoverableState(ResumeRecoverable resumable) {
        Preconditions.checkNotNull(resumable);

        GSRecoverable recoverable = (GSRecoverable) resumable;
        return options.helper.getStorage().delete(recoverable.plan.tempBlobId);
    }

    @Override
    public RecoverableFsDataOutputStream.Committer recoverForCommit(CommitRecoverable resumable)
            throws IOException {
        Preconditions.checkNotNull(resumable);

        GSRecoverable recoverable = (GSRecoverable) resumable;
        RecoverableFsDataOutputStream outputStream = recover(recoverable);
        return outputStream.closeForCommit();
    }

    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    public SimpleVersionedSerializer<CommitRecoverable> getCommitRecoverableSerializer() {
        return (SimpleVersionedSerializer) GSRecoverableSerializer.INSTANCE;
    }

    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    public SimpleVersionedSerializer<ResumeRecoverable> getResumeRecoverableSerializer() {
        return (SimpleVersionedSerializer) GSRecoverableSerializer.INSTANCE;
    }
}
