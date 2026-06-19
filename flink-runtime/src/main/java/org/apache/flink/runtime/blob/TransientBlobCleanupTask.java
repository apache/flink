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

package org.apache.flink.runtime.blob;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.tuple.Tuple2;

import org.slf4j.Logger;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiConsumer;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Cleanup task for transient BLOBs. */
class TransientBlobCleanupTask extends TimerTask {

    /** The log object used for debugging. */
    private final Logger log;

    private final BiConsumer<JobID, TransientBlobKey> cleanupCallback;

    /** Map to store the TTL of each element stored in the local storage. */
    private ConcurrentMap<Tuple2<JobID, TransientBlobKey>, Long> blobExpiryTimes;

    /**
     * Creates a new cleanup timer task working with the given parameters from {@link BlobServer}
     * and {@link TransientBlobCache}.
     *
     * @param blobExpiryTimes map to store the TTL of each element stored in the local storage
     * @param log logger instance for debugging
     */
    TransientBlobCleanupTask(
            ConcurrentMap<Tuple2<JobID, TransientBlobKey>, Long> blobExpiryTimes,
            BiConsumer<JobID, TransientBlobKey> cleanupCallback,
            Logger log) {
        this.blobExpiryTimes = checkNotNull(blobExpiryTimes);
        this.cleanupCallback = checkNotNull(cleanupCallback);
        this.log = checkNotNull(log);
    }

    /** Cleans up transient BLOBs whose TTL is up, tolerating that files do not exist (anymore). */
    @Override
    public void run() {
        // let's cache the current time - we do not operate on a millisecond precision anyway
        final long currentTimeMillis = System.currentTimeMillis();
        // iterate through all entries and remove those where the current time is past their expiry
        Set<Map.Entry<Tuple2<JobID, TransientBlobKey>, Long>> entries =
                new HashSet<>(blobExpiryTimes.entrySet());
        for (Map.Entry<Tuple2<JobID, TransientBlobKey>, Long> entry : entries) {
            if (currentTimeMillis >= entry.getValue()) {
                JobID jobId = entry.getKey().f0;
                TransientBlobKey blobKey = entry.getKey().f1;

                cleanupCallback.accept(jobId, blobKey);
            }
        }
    }
}
