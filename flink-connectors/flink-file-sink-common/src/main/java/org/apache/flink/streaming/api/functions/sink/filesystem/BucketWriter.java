/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.functions.sink.filesystem;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.fs.Path;

import java.io.IOException;

/** An interface for factories that create the different {@link InProgressFileWriter writers}. */
@Internal
public interface BucketWriter<IN, BucketID> {

    /**
     * Used to create a new {@link InProgressFileWriter}.
     *
     * @param bucketID the id of the bucket this writer is writing to.
     * @param path the path this writer will write to.
     * @param creationTime the creation time of the file.
     * @return the new {@link InProgressFileWriter}
     * @throws IOException Thrown if creating a writer fails.
     */
    InProgressFileWriter<IN, BucketID> openNewInProgressFile(
            final BucketID bucketID, final Path path, final long creationTime) throws IOException;

    /**
     * Used to resume a {@link InProgressFileWriter} from a {@link
     * InProgressFileWriter.InProgressFileRecoverable}.
     *
     * @param bucketID the id of the bucket this writer is writing to.
     * @param inProgressFileSnapshot the state of the part file.
     * @param creationTime the creation time of the file.
     * @return the resumed {@link InProgressFileWriter}
     * @throws IOException Thrown if resuming a writer fails.
     */
    InProgressFileWriter<IN, BucketID> resumeInProgressFileFrom(
            final BucketID bucketID,
            final InProgressFileWriter.InProgressFileRecoverable inProgressFileSnapshot,
            final long creationTime)
            throws IOException;

    /** @return the property of the {@link BucketWriter} */
    WriterProperties getProperties();

    /**
     * Recovers a pending file for finalizing and committing.
     *
     * @param pendingFileRecoverable The handle with the recovery information.
     * @return A pending file
     * @throws IOException Thrown if recovering a pending file fails.
     */
    PendingFile recoverPendingFile(
            final InProgressFileWriter.PendingFileRecoverable pendingFileRecoverable)
            throws IOException;

    /**
     * Frees up any resources that were previously occupied in order to be able to recover from a
     * (potential) failure.
     *
     * <p><b>NOTE:</b> This operation should not throw an exception, but return false if the cleanup
     * did not happen for any reason.
     *
     * @param inProgressFileRecoverable the {@link InProgressFileWriter.InProgressFileRecoverable}
     *     whose state we want to clean-up.
     * @return {@code true} if the resources were successfully freed, {@code false} otherwise (e.g.
     *     the file to be deleted was not there for any reason - already deleted or never created).
     * @throws IOException if an I/O error occurs
     */
    boolean cleanupInProgressFileRecoverable(
            final InProgressFileWriter.InProgressFileRecoverable inProgressFileRecoverable)
            throws IOException;

    /** This represents the file that can not write any data to. */
    interface PendingFile {
        /**
         * Commits the pending file, making it visible. The file will contain the exact data as when
         * the pending file was created.
         *
         * @throws IOException Thrown if committing fails.
         */
        void commit() throws IOException;

        /**
         * Commits the pending file, making it visible. The file will contain the exact data as when
         * the pending file was created.
         *
         * <p>This method tolerates situations where the file was already committed and will not
         * raise an exception in that case. This is important for idempotent commit retries as they
         * need to happen after recovery.
         *
         * @throws IOException Thrown if committing fails.
         */
        void commitAfterRecovery() throws IOException;
    }
}
