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
import java.util.HashSet;
import java.util.List;

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
        return true;
    }

    @Override
    public boolean supportsResume() {
        return true;
    }

    @Override
    public RecoverableFsDataOutputStream open(Path path) throws IOException {
        Preconditions.checkNotNull(path);

        GSBlobIdentifier finalBlobId = BlobUtils.parseUri(path.toUri());
        GSRecoverableWriterState state = new GSRecoverableWriterState(finalBlobId);
        return new GSRecoverableFsDataOutputStream(storage, options, this, state);
    }

    @Override
    public RecoverableFsDataOutputStream recover(ResumeRecoverable resumable) throws IOException {
        Preconditions.checkNotNull(resumable);

        GSRecoverableWriterState state = (GSRecoverableWriterState) resumable;
        return new GSRecoverableFsDataOutputStream(storage, options, this, state);
    }

    @Override
    public boolean cleanupRecoverableState(ResumeRecoverable resumable) throws IOException {
        Preconditions.checkNotNull(resumable);

        // determine the partial name for the temporary objects to be deleted
        GSRecoverableWriterState state = (GSRecoverableWriterState) resumable;
        String temporaryBucketName = state.getTemporaryBucketName(options);
        String temporaryObjectPartialName = state.getTemporaryObjectPartialName(options);

        // this will hold the set of blob ids that were actually deleted
        HashSet<GSBlobIdentifier> deletedBlobIdentifiers = new HashSet<>();

        // find all the temp blobs by looking for anything that starts with the temporary
        // object partial name. doing it this way finds any orphaned temp blobs as well
        List<GSBlobIdentifier> foundTempBlobIdentifiers =
                storage.list(temporaryBucketName, temporaryObjectPartialName);
        if (!foundTempBlobIdentifiers.isEmpty()) {

            // delete all the temp blobs, and populate the set with ones that were actually deleted
            // normalize in case the blob came back with a generation populated
            List<Boolean> deleteResults = storage.delete(foundTempBlobIdentifiers);
            for (int i = 0; i < deleteResults.size(); i++) {
                if (deleteResults.get(i)) {
                    deletedBlobIdentifiers.add(foundTempBlobIdentifiers.get(i));
                }
            }
        }

        // determine if we deleted everything we expected to, by comparing the stored
        // temp blob ids so the ones that were found and deleted
        for (GSBlobIdentifier componentBlobIdentifier : state.getComponentBlobIds(options)) {
            if (!deletedBlobIdentifiers.contains(componentBlobIdentifier)) {
                // we expected to delete this blob but did not, so return false
                return false;
            }
        }

        // everything was deleted that was expected to be
        return true;
    }

    @Override
    public RecoverableFsDataOutputStream.Committer recoverForCommit(CommitRecoverable resumable)
            throws IOException {
        Preconditions.checkNotNull(resumable);

        GSRecoverableWriterState state = (GSRecoverableWriterState) resumable;
        return new GSRecoverableWriterCommitter(storage, options, this, state);
    }

    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    public SimpleVersionedSerializer<CommitRecoverable> getCommitRecoverableSerializer() {
        return (SimpleVersionedSerializer) GSRecoverableWriterStateSerializer.INSTANCE;
    }

    @Override
    @SuppressWarnings({"rawtypes", "unchecked"})
    public SimpleVersionedSerializer<ResumeRecoverable> getResumeRecoverableSerializer() {
        return (SimpleVersionedSerializer) GSRecoverableWriterStateSerializer.INSTANCE;
    }
}
