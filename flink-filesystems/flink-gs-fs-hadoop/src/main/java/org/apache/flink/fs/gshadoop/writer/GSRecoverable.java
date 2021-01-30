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

import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.util.Preconditions;

/** Data structure that holds the recoverable state of a resumable upload. */
class GSRecoverable implements RecoverableWriter.ResumeRecoverable {

    /** The upload plan associated with this upload. */
    public final GSRecoverablePlan plan;

    /** The writer state associated with this upload. */
    public final GSRecoverableWriterHelper.WriterState writerState;

    /** The write position associated with this upload, i.e. number of bytes written. */
    public final long position;

    /**
     * Constructs a GSRecoverable.
     *
     * @param plan The plan
     * @param writerState The writer state
     * @param position The write position
     */
    public GSRecoverable(
            GSRecoverablePlan plan,
            GSRecoverableWriterHelper.WriterState writerState,
            long position) {
        this.plan = Preconditions.checkNotNull(plan);
        this.writerState = Preconditions.checkNotNull(writerState);
        this.position = position;
        Preconditions.checkArgument(position >= 0, "position must be non-negative: %s", position);
    }
}
