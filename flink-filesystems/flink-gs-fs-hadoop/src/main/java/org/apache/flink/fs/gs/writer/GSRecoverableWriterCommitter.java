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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.fs.RecoverableFsDataOutputStream;
import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.fs.gs.GSFileSystemOptions;
import org.apache.flink.fs.gs.storage.GSBlobIdentifier;
import org.apache.flink.fs.gs.storage.GSBlobStorage;
import org.apache.flink.fs.gs.utils.BlobUtils;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/** The committer for the GS recoverable writer. */
class GSRecoverableWriterCommitter implements RecoverableFsDataOutputStream.Committer {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(GSRecoverableWriterCommitter.class);

    /** The underlying blob storage. */
    private final GSBlobStorage storage;

    /** The GS file system options. */
    @VisibleForTesting final GSFileSystemOptions options;

    /** The recoverable for the commit operation. */
    @VisibleForTesting final GSCommitRecoverable recoverable;

    /** The max number of blobs to compose in a single operation. */
    private final int composeMaxBlobs;

    GSRecoverableWriterCommitter(
            GSBlobStorage storage,
            GSFileSystemOptions options,
            GSCommitRecoverable recoverable,
            int composeMaxBlobs) {
        LOGGER.trace(
                "Creating GSRecoverableWriterCommitter with options {} for recoverable: {}",
                options,
                recoverable);
        this.storage = Preconditions.checkNotNull(storage);
        this.options = Preconditions.checkNotNull(options);
        this.recoverable = Preconditions.checkNotNull(recoverable);
        Preconditions.checkArgument(composeMaxBlobs > 0);
        this.composeMaxBlobs = composeMaxBlobs;
    }

    GSRecoverableWriterCommitter(
            GSBlobStorage storage, GSFileSystemOptions options, GSCommitRecoverable recoverable) {
        this(storage, options, recoverable, BlobUtils.COMPOSE_MAX_BLOBS);
    }

    @Override
    public void commit() throws IOException {
        LOGGER.trace("Committing recoverable with options {}: {}", options, recoverable);

        // see discussion: https://github.com/apache/flink/pull/15599#discussion_r623127365
        // first, make sure the final blob doesn't already exist
        Optional<GSBlobStorage.BlobMetadata> blobMetadata =
                storage.getMetadata(recoverable.finalBlobIdentifier);
        if (blobMetadata.isPresent()) {
            throw new IOException(
                    String.format(
                            "Blob %s already exists during attempted commit",
                            recoverable.finalBlobIdentifier));
        }

        // write the final blob
        writeFinalBlob();

        // clean up after successful commit
        cleanupTemporaryBlobs();
    }

    @Override
    public void commitAfterRecovery() throws IOException {
        LOGGER.trace(
                "Committing recoverable after recovery with options {}: {}", options, recoverable);

        // see discussion: https://github.com/apache/flink/pull/15599#discussion_r623127365
        // only write the final blob if it doesn't already exist
        Optional<GSBlobStorage.BlobMetadata> blobMetadata =
                storage.getMetadata(recoverable.finalBlobIdentifier);
        if (!blobMetadata.isPresent()) {
            writeFinalBlob();
        }

        // clean up after successful commit
        cleanupTemporaryBlobs();
    }

    @Override
    public RecoverableWriter.CommitRecoverable getRecoverable() {
        return recoverable;
    }

    /**
     * Helper to compose an arbitrary number of blobs into a final blob, staying under the
     * composeMaxBlobs limit for any individual compose operation.
     *
     * @param sourceBlobIdentifiers The source blob ids to compose
     * @param targetBlobIdentifier The target blob id for the composed result
     */
    private void composeBlobs(
            List<GSBlobIdentifier> sourceBlobIdentifiers, GSBlobIdentifier targetBlobIdentifier) {
        LOGGER.trace(
                "Composing blobs {} to {} for commit with options {}",
                sourceBlobIdentifiers,
                targetBlobIdentifier,
                options);
        Preconditions.checkNotNull(sourceBlobIdentifiers);
        Preconditions.checkArgument(sourceBlobIdentifiers.size() > 0);
        Preconditions.checkNotNull(targetBlobIdentifier);

        // split the source list into two parts; first, the ones we can compose in this operation
        // (up to composeMaxBlobs), and, second, whichever blobs are left over
        final int composeToIndex = Math.min(composeMaxBlobs, sourceBlobIdentifiers.size());
        List<GSBlobIdentifier> composeBlobIds = sourceBlobIdentifiers.subList(0, composeToIndex);
        List<GSBlobIdentifier> remainingBlobIds =
                sourceBlobIdentifiers.subList(composeToIndex, sourceBlobIdentifiers.size());

        // determine the resulting blob id for this compose operation. if this is the last compose,
        // i.e. if there are no remaining blob ids, then the composed blob id is the originally
        // specified target blob id. otherwise, we must create an intermediate blob id to hold the
        // result of this compose operation
        UUID temporaryObjectId = UUID.randomUUID();
        GSBlobIdentifier composedBlobId =
                remainingBlobIds.isEmpty()
                        ? targetBlobIdentifier
                        : BlobUtils.getTemporaryBlobIdentifier(
                                recoverable.finalBlobIdentifier, temporaryObjectId, options);

        // compose the blobs
        storage.compose(composeBlobIds, composedBlobId);

        // if we have remaining blobs, add the composed blob id to the beginning of the list
        // of remaining blob ids, and recurse
        if (!remainingBlobIds.isEmpty()) {
            remainingBlobIds.add(0, composedBlobId);
            composeBlobs(remainingBlobIds, targetBlobIdentifier);
        }
    }

    /** Writes the final blob by composing the temporary blobs and copying, if necessary. */
    private void writeFinalBlob() {

        // do we have any component blobs?
        List<GSBlobIdentifier> blobIdentifiers = recoverable.getComponentBlobIds(options);
        if (blobIdentifiers.isEmpty()) {

            // we have no blob identifiers, so just create an empty target blob
            storage.createBlob(recoverable.finalBlobIdentifier);

        } else {

            // yes, we have component blobs. compose them into the final blob id. if the component
            // blob ids are in the same bucket as the final blob id, this can be done directly.
            // otherwise, we must compose to a new temporary blob id in the same bucket as the
            // component blob ids and then copy that blob to the final blob location
            String temporaryBucketName =
                    BlobUtils.getTemporaryBucketName(recoverable.finalBlobIdentifier, options);
            if (recoverable.finalBlobIdentifier.bucketName.equals(temporaryBucketName)) {

                // compose directly to final blob
                composeBlobs(
                        recoverable.getComponentBlobIds(options), recoverable.finalBlobIdentifier);

            } else {

                // compose to the intermediate blob, then copy
                UUID temporaryObjectId = UUID.randomUUID();
                GSBlobIdentifier intermediateBlobIdentifier =
                        BlobUtils.getTemporaryBlobIdentifier(
                                recoverable.finalBlobIdentifier, temporaryObjectId, options);
                composeBlobs(recoverable.getComponentBlobIds(options), intermediateBlobIdentifier);
                storage.copy(intermediateBlobIdentifier, recoverable.finalBlobIdentifier);
            }
        }
    }

    /**
     * Clean up after a successful commit operation, by deleting any temporary blobs associated with
     * the final blob.
     */
    private void cleanupTemporaryBlobs() {
        LOGGER.trace(
                "Cleaning up temporary blobs for recoverable with options {}: {}",
                options,
                recoverable);

        // determine the partial name for the temporary objects to be deleted
        String temporaryBucketName =
                BlobUtils.getTemporaryBucketName(recoverable.finalBlobIdentifier, options);
        String temporaryObjectPartialName =
                BlobUtils.getTemporaryObjectPartialName(recoverable.finalBlobIdentifier);

        // find all the temp blobs by looking for anything that starts with the temporary
        // object partial name. doing it this way finds any orphaned temp blobs as well
        List<GSBlobIdentifier> foundTempBlobIdentifiers =
                storage.list(temporaryBucketName, temporaryObjectPartialName);
        if (!foundTempBlobIdentifiers.isEmpty()) {

            // delete all the temp blobs, and populate the set with ones that were actually deleted
            // normalize in case the blob came back with a generation populated
            storage.delete(foundTempBlobIdentifiers);
        }
    }
}
