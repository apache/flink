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
 * limitations under the License
 */

package org.apache.flink.runtime.blob;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.blob.BlobKey.BlobType;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link BlobCacheSizeTracker}. */
class BlobCacheSizeTrackerTest {

    private BlobCacheSizeTracker tracker;
    private JobID jobId;
    private BlobKey blobKey;

    @BeforeEach
    void setup() {
        tracker = new BlobCacheSizeTracker(5L);
        jobId = new JobID();
        blobKey = BlobKey.createKey(BlobType.PERMANENT_BLOB);

        tracker.track(jobId, blobKey, 3L);
    }

    @Test
    void testCheckLimit() {
        List<Tuple2<JobID, BlobKey>> keys = tracker.checkLimit(3L);

        assertThat(keys).hasSize(1);
        assertThat(keys.get(0).f0).isEqualTo(jobId);
        assertThat(keys.get(0).f1).isEqualTo(blobKey);
    }

    /** If an empty BLOB is intended to be stored, no BLOBs should be removed. */
    @Test
    void testCheckLimitForEmptyBlob() {
        List<Tuple2<JobID, BlobKey>> keys = tracker.checkLimit(0L);

        assertThat(keys).isEmpty();
    }

    @Test
    void testCheckLimitForBlobWithNegativeSize() {
        assertThatThrownBy(() -> tracker.checkLimit(-1L))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testTrack() {
        assertThat(tracker.getSize(jobId, blobKey)).isEqualTo(3L);
        assertThat(tracker.getBlobKeysByJobId(jobId)).contains(blobKey);
    }

    /**
     * When trying to track a duplicated BLOB, the new one will be ignored, just like {@link
     * BlobUtils#moveTempFileToStore} does.
     */
    @Test
    void testTrackDuplicatedBlob() {
        tracker.track(jobId, blobKey, 1L);
        assertThat(tracker.getSize(jobId, blobKey)).isEqualTo(3L);
        assertThat(tracker.getBlobKeysByJobId(jobId)).hasSize(1);
    }

    @Test
    void testUntrack() {
        assertThat(tracker.checkLimit(3L)).hasSize(1);
        tracker.untrack(Tuple2.of(jobId, blobKey));

        assertThat(tracker.getSize(jobId, blobKey)).isNull();
        assertThat(tracker.getBlobKeysByJobId(jobId)).isEmpty();
        assertThat(tracker.checkLimit(3L)).isEmpty();
    }

    /** Untracking a non-existing BLOB shouldn't change anything or throw any exceptions. */
    @Test
    void testUntrackNonExistingBlob() {
        tracker.untrack(Tuple2.of(jobId, BlobKey.createKey(BlobType.PERMANENT_BLOB)));
        assertThat(tracker.getBlobKeysByJobId(jobId)).hasSize(1);
    }

    /**
     * Since the BlobCacheSizeLimitTracker only works in {@link PermanentBlobCache}, the JobID
     * shouldn't be null.
     */
    @Test
    void testUntrackBlobWithNullJobId() {
        assertThatThrownBy(
                        () ->
                                tracker.untrack(
                                        Tuple2.of(
                                                null, BlobKey.createKey(BlobType.PERMANENT_BLOB))))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void testUpdate() {
        BlobCacheSizeTracker tracker = new BlobCacheSizeTracker(5L);
        List<JobID> jobIds = new ArrayList<>();
        List<BlobKey> blobKeys = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            jobIds.add(new JobID());
            blobKeys.add(BlobKey.createKey(BlobType.PERMANENT_BLOB));
        }
        for (int i = 0; i < 5; i++) {
            tracker.track(jobIds.get(i), blobKeys.get(i), 1);
        }
        tracker.update(jobIds.get(1), blobKeys.get(1));
        tracker.update(jobIds.get(2), blobKeys.get(2));

        List<Tuple2<JobID, BlobKey>> blobsToDelete = tracker.checkLimit(2);

        assertThat(blobsToDelete)
                .contains(
                        Tuple2.of(jobIds.get(0), blobKeys.get(0)),
                        Tuple2.of(jobIds.get(3), blobKeys.get(3)));
    }

    /**
     * Updating the status for a non-existing BLOB shouldn't change anything or throw any
     * exceptions.
     */
    @Test
    void testUpdateNonExistingBlob() {
        tracker.track(new JobID(), BlobKey.createKey(BlobType.PERMANENT_BLOB), 2L);
        assertThat(tracker.checkLimit(3L)).hasSize(1);

        tracker.update(new JobID(), BlobKey.createKey(BlobType.PERMANENT_BLOB));
        assertThat(tracker.checkLimit(3L)).hasSize(1);
    }

    @Test
    void testUntrackAll() {
        tracker.track(jobId, BlobKey.createKey(BlobType.PERMANENT_BLOB), 1L);

        JobID anotherJobId = new JobID();
        tracker.track(anotherJobId, BlobKey.createKey(BlobType.PERMANENT_BLOB), 1L);

        assertThat(tracker.getBlobKeysByJobId(jobId)).hasSize(2);
        tracker.untrackAll(jobId);

        assertThat(tracker.getBlobKeysByJobId(jobId)).isEmpty();
        assertThat(tracker.getBlobKeysByJobId(anotherJobId)).hasSize(1);
    }

    /**
     * Untracking all BLOBs for a non-existing job shouldn't change anything or throw any
     * exceptions.
     */
    @Test
    void testUntrackAllWithNonExistingJob() {
        tracker.track(jobId, BlobKey.createKey(BlobType.PERMANENT_BLOB), 1L);

        assertThat(tracker.getBlobKeysByJobId(jobId)).hasSize(2);
        tracker.untrackAll(new JobID());

        assertThat(tracker.getBlobKeysByJobId(jobId)).hasSize(2);
    }
}
