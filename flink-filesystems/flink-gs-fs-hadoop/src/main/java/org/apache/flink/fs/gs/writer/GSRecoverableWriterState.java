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

import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.fs.gs.GSFileSystemOptions;
import org.apache.flink.util.Preconditions;

import com.google.cloud.storage.BlobId;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

/** The state of a recoverable write. */
class GSRecoverableWriterState implements RecoverableWriter.ResumeRecoverable, Cloneable {

    /** The blob id to which the recoverable write operation is writing. */
    public final BlobId finalBlobId;

    /** The number of bytes that have been written so far. */
    public long bytesWritten;

    /** Indicates if the write has been closed. */
    public boolean closed;

    /** The object ids for the temporary objects that should be composed to form the final blob. */
    public final List<UUID> componentObjectIds;

    GSRecoverableWriterState(
            BlobId finalBlobId, long bytesWritten, boolean closed, List<UUID> componentObjectIds) {
        this.finalBlobId = Preconditions.checkNotNull(finalBlobId);
        Preconditions.checkArgument(bytesWritten >= 0);
        this.bytesWritten = bytesWritten;
        this.closed = closed;

        // shallow copy the component object ids to ensure this state object exclusively
        // manages the list of component object ids
        this.componentObjectIds = new ArrayList<>(Preconditions.checkNotNull(componentObjectIds));
    }

    GSRecoverableWriterState(GSRecoverableWriterState state) {
        this(state.finalBlobId, state.bytesWritten, state.closed, state.componentObjectIds);
    }

    GSRecoverableWriterState(BlobId finalBlobId) {
        this(finalBlobId, 0, false, new ArrayList<>());
    }

    /**
     * Returns the temporary bucket name. If options specifies a temporary bucket name, we use that
     * one; otherwise, we use the bucket name of the final blob.
     *
     * @param options The GS file system options
     * @return The temporary bucket name
     */
    String getTemporaryBucketName(GSFileSystemOptions options) {
        return options.writerTemporaryBucketName.isEmpty()
                ? finalBlobId.getBucket()
                : options.writerTemporaryBucketName;
    }

    /**
     * Returns a temporary object partial name, i.e. .inprogress/foo/bar/ for the final blob with
     * object name "foo/bar". The included trailing slash is deliberate, so that we can be sure that
     * object names that start with this partial name are, in fact, temporary files associated with
     * the upload of the associated final blob.
     *
     * @param options The GS file system options
     * @return The temporary object partial name
     */
    String getTemporaryObjectPartialName(GSFileSystemOptions options) {
        String finalObjectName = finalBlobId.getName();
        return String.format("%s%s/", options.writerTemporaryObjectPrefix, finalObjectName);
    }

    /**
     * Returns a temporary object name, formed by appending the compact string version of the
     * temporary object id to the temporary object partial name, i.e.
     * .inprogress/foo/bar/EjgelvANQ525hLUW2S6DBA for the final blob with object name "foo/bar".
     *
     * @param temporaryObjectId The temporary object id
     * @param options The GS file system options
     * @return The temporary object name
     */
    String getTemporaryObjectName(UUID temporaryObjectId, GSFileSystemOptions options) {
        return getTemporaryObjectPartialName(options) + temporaryObjectId.toString();
    }

    /**
     * Creates a temporary blob id for a provided temporary object id.
     *
     * @param temporaryObjectId The temporary object id
     * @param options The GS file system options
     * @return
     */
    private BlobId createTemporaryBlobId(UUID temporaryObjectId, GSFileSystemOptions options) {
        String temporaryBucketName = getTemporaryBucketName(options);
        String temporaryObjectName = getTemporaryObjectName(temporaryObjectId, options);
        return BlobId.of(temporaryBucketName, temporaryObjectName);
    }

    /**
     * Creates a new temporary blob id.
     *
     * @param options The GS file system options
     * @return The new temporary blob id.
     */
    BlobId createTemporaryBlobId(GSFileSystemOptions options) {
        UUID temporaryObjectId = UUID.randomUUID();
        return createTemporaryBlobId(temporaryObjectId, options);
    }

    /**
     * Create a new temporary blob id and add to the list of components.
     *
     * @param options The GS file system options
     * @return The new component blob id.
     */
    BlobId createComponentBlobId(GSFileSystemOptions options) {
        UUID temporaryObjectId = UUID.randomUUID();
        componentObjectIds.add(temporaryObjectId);
        return createTemporaryBlobId(temporaryObjectId, options);
    }

    /**
     * Returns the list of component blob ids, which have to be resolved from the temporary bucket
     * name, prefix, and component ids. Resolving them this way vs. storing the blob ids directly
     * allows us to move in-progress blobs by changing options to point to new locations.
     *
     * @param options The GS file system options
     * @return The list of component blob ids
     */
    List<BlobId> getComponentBlobIds(GSFileSystemOptions options) {
        String temporaryBucketName = getTemporaryBucketName(options);
        return componentObjectIds.stream()
                .map(temporaryObjectId -> getTemporaryObjectName(temporaryObjectId, options))
                .map(temporaryObjectName -> BlobId.of(temporaryBucketName, temporaryObjectName))
                .collect(Collectors.toList());
    }
}
