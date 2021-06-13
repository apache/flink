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

package org.apache.flink.fs.gs.writer;

import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.RecoverableFsDataOutputStream;
import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.fs.gs.GSFileSystemOptions;
import org.apache.flink.fs.gs.storage.GSBlobIdentifier;
import org.apache.flink.fs.gs.storage.GSBlobStorage;
import org.apache.flink.fs.gs.utils.BlobUtils;
import org.apache.flink.util.Preconditions;

import java.io.IOException;

/** The recoverable writer implementation for Google storage. */
public class GSRecoverableWriter implements RecoverableWriter {

    /** The underlying blob storage. */
    private final GSBlobStorage storage;

    /** The GS file system options. */
    private final GSFileSystemOptions options;

    /**
     * Construct a GS recoverable writer.
     *
     * @param storage The underlying blob storage instance
     * @param options The GS file system options
     */
    public GSRecoverableWriter(GSBlobStorage storage, GSFileSystemOptions options) {
        this.storage = Preconditions.checkNotNull(storage);
        this.options = Preconditions.checkNotNull(options);
    }

    @Override
    public boolean requiresCleanupOfRecoverableState() {
        // we can't clean up any state prior to commit
        // see discussion: https://github.com/apache/flink/pull/15599#discussion_r623127365
        return false;
    }

    @Override
    public boolean supportsResume() {
        return true;
    }

    @Override
    public RecoverableFsDataOutputStream open(Path path) throws IOException {
        Preconditions.checkNotNull(path);

        GSBlobIdentifier finalBlobIdentifier = BlobUtils.parseUri(path.toUri());
        return new GSRecoverableFsDataOutputStream(storage, options, finalBlobIdentifier);
    }

    @Override
    public RecoverableFsDataOutputStream recover(ResumeRecoverable resumable) {
        Preconditions.checkNotNull(resumable);

        GSResumeRecoverable recoverable = (GSResumeRecoverable) resumable;
        return new GSRecoverableFsDataOutputStream(storage, options, recoverable);
    }

    @Override
    public boolean cleanupRecoverableState(ResumeRecoverable resumable) {
        // we can't safely clean up any state prior to commit, so do nothing here
        // see discussion: https://github.com/apache/flink/pull/15599#discussion_r623127365
        return true;
    }

    @Override
    public RecoverableFsDataOutputStream.Committer recoverForCommit(CommitRecoverable resumable) {
        Preconditions.checkNotNull(resumable);

        GSResumeRecoverable recoverable = (GSResumeRecoverable) resumable;
        return new GSRecoverableWriterCommitter(storage, options, recoverable);
    }

    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    public SimpleVersionedSerializer<CommitRecoverable> getCommitRecoverableSerializer() {
        return (SimpleVersionedSerializer) GSResumeRecoverableSerializer.INSTANCE;
    }

    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    public SimpleVersionedSerializer<ResumeRecoverable> getResumeRecoverableSerializer() {
        return (SimpleVersionedSerializer) GSResumeRecoverableSerializer.INSTANCE;
    }
}
