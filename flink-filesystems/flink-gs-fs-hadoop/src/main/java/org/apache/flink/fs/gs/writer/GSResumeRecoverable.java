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
import org.apache.flink.fs.gs.storage.GSBlobIdentifier;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.UUID;

/** A resumable state for a recoverable output stream. */
class GSResumeRecoverable extends GSCommitRecoverable
        implements RecoverableWriter.ResumeRecoverable {

    private static final Logger LOGGER = LoggerFactory.getLogger(GSResumeRecoverable.class);

    /** The write position, i.e. number of bytes that have been written so far. */
    public final long position;

    /** Indicates if the write has been closed. */
    public final boolean closed;

    GSResumeRecoverable(
            GSBlobIdentifier finalBlobIdentifier,
            List<UUID> componentObjectIds,
            long position,
            boolean closed) {
        super(finalBlobIdentifier, componentObjectIds);
        LOGGER.trace(
                "Creating GSResumeRecoverable for blob {} with position={}, closed={}, and componentObjectIds={}",
                finalBlobIdentifier,
                position,
                closed,
                componentObjectIds);
        Preconditions.checkArgument(position >= 0);
        this.position = position;
        this.closed = closed;
    }

    @Override
    public String toString() {
        return "GSResumeRecoverable{"
                + "finalBlobIdentifier="
                + finalBlobIdentifier
                + ", componentObjectIds="
                + componentObjectIds
                + ", position="
                + position
                + ", closed="
                + closed
                + '}';
    }
}
