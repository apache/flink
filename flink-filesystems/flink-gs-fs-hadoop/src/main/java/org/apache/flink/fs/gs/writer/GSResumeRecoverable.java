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
import org.apache.flink.fs.gs.storage.GSBlobIdentifier;
import org.apache.flink.fs.gs.utils.BlobUtils;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

/** A resumable state for a recoverable output stream. */
class GSResumeRecoverable implements RecoverableWriter.ResumeRecoverable {

    /** The blob id to which the recoverable write operation is writing. */
    public final GSBlobIdentifier finalBlobIdentifier;

    /** The write position, i.e. number of bytes that have been written so far. */
    public final long position;

    /** Indicates if the write has been closed. */
    public final boolean closed;

    /**
     * The object ids for the temporary objects that should be composed to form the final blob. This
     * is an unmodifiable list.
     */
    public final List<UUID> componentObjectIds;

    GSResumeRecoverable(
            GSBlobIdentifier finalBlobIdentifier,
            long position,
            boolean closed,
            List<UUID> componentObjectIds) {
        this.finalBlobIdentifier = Preconditions.checkNotNull(finalBlobIdentifier);
        Preconditions.checkArgument(position >= 0);
        this.position = position;
        this.closed = closed;
        this.componentObjectIds =
                Collections.unmodifiableList(
                        new ArrayList<>(Preconditions.checkNotNull(componentObjectIds)));
    }

    /**
     * Returns the list of component blob ids, which have to be resolved from the temporary bucket
     * name, prefix, and component ids. Resolving them this way vs. storing the blob ids directly
     * allows us to move in-progress blobs by changing options to point to new in-progress
     * locations.
     *
     * @param options The GS file system options
     * @return The list of component blob ids
     */
    List<GSBlobIdentifier> getComponentBlobIds(GSFileSystemOptions options) {
        String temporaryBucketName = BlobUtils.getTemporaryBucketName(finalBlobIdentifier, options);
        return componentObjectIds.stream()
                .map(
                        temporaryObjectId ->
                                BlobUtils.getTemporaryObjectName(
                                        finalBlobIdentifier, temporaryObjectId))
                .map(
                        temporaryObjectName ->
                                new GSBlobIdentifier(temporaryBucketName, temporaryObjectName))
                .collect(Collectors.toList());
    }
}
