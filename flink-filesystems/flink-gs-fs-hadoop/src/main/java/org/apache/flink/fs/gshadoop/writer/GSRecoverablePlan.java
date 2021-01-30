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

import org.apache.flink.util.Preconditions;

import com.google.cloud.storage.BlobId;

/**
 * Data structure holding the upload plan for a single blob, including the temporary blob id used
 * for the resumable upload, and the final blob id that will be copied to upon commit.
 */
class GSRecoverablePlan {

    /** The temporary blob id. */
    public final BlobId tempBlobId;

    /** The final blob id. */
    public final BlobId finalBlobId;

    /**
     * Constructs a plan object.
     *
     * @param tempBlobId The temporary blob id
     * @param finalBlobId The final blob id
     */
    GSRecoverablePlan(BlobId tempBlobId, BlobId finalBlobId) {
        this.tempBlobId = Preconditions.checkNotNull(tempBlobId);
        this.finalBlobId = Preconditions.checkNotNull(finalBlobId);
    }
}
