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
import org.apache.flink.util.TestLogger;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/** Tests for {@link BlobCacheSizeTracker}. */
public class BlobCacheSizeTrackerTest extends TestLogger {

    private BlobCacheSizeTracker tracker;
    private JobID jobId;
    private BlobKey blobKey;

    @Before
    public void setup() {
        tracker = new BlobCacheSizeTracker(5L);
        jobId = new JobID();
        blobKey = BlobKey.createKey(BlobType.PERMANENT_BLOB);

        tracker.track(jobId, blobKey, 3L);
    }

    @Test
    public void testCheckLimit() {
        List<Tuple2<JobID, BlobKey>> keys = tracker.checkLimit(3L);

        assertEquals(1, keys.size());
        assertEquals(jobId, keys.get(0).f0);
        assertEquals(blobKey, keys.get(0).f1);
    }

    /** If an empty BLOB is intended to be stored, no BLOBs should be removed. */
    @Test
    public void testCheckLimitForEmptyBlob() {
        List<Tuple2<JobID, BlobKey>> keys = tracker.checkLimit(0L);

        assertEquals(0, keys.size());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testCheckLimitForBlobWithNegativeSize() {
        tracker.checkLimit(-1L);
    }

    @Test
    public void testTrack() {
        assertEquals(3L, (long) tracker.getSize(jobId, blobKey));
        assertTrue(tracker.getBlobKeysByJobId(jobId).contains(blobKey));
    }

    /**
     * When trying to track a duplicated BLOB, the new one will be ignored, just like {@link
     * BlobUtils#moveTempFileToStore} does.
     */
    @Test
    public void testTrackDuplicatedBlob() {
        tracker.track(jobId, blobKey, 1L);
        assertEquals(3L, (long) tracker.getSize(jobId, blobKey));
        assertEquals(1, tracker.getBlobKeysByJobId(jobId).size());
    }

    @Test
    public void testUntrack() {
        assertEquals(1, tracker.checkLimit(3L).size());
        tracker.untrack(Tuple2.of(jobId, blobKey));

        assertNull(tracker.getSize(jobId, blobKey));
        assertEquals(0, tracker.getBlobKeysByJobId(jobId).size());
        assertEquals(0, tracker.checkLimit(3L).size());
    }

    /** Untracking a non-existing BLOB shouldn't change anything or throw any exceptions. */
    @Test
    public void testUntrackNonExistingBlob() {
        tracker.untrack(Tuple2.of(jobId, BlobKey.createKey(BlobType.PERMANENT_BLOB)));
        assertEquals(1, tracker.getBlobKeysByJobId(jobId).size());
    }

    /**
     * Since the BlobCacheSizeLimitTracker only works in {@link PermanentBlobCache}, the JobID
     * shouldn't be null.
     */
    @Test(expected = NullPointerException.class)
    public void testUntrackBlobWithNullJobId() {
        tracker.untrack(Tuple2.of(null, BlobKey.createKey(BlobType.PERMANENT_BLOB)));
    }

    @Test
    public void testUpdate() {
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

        assertThat(
                blobsToDelete,
                containsInAnyOrder(
                        Tuple2.of(jobIds.get(0), blobKeys.get(0)),
                        Tuple2.of(jobIds.get(3), blobKeys.get(3))));
    }

    /**
     * Updating the status for a non-existing BLOB shouldn't change anything or throw any
     * exceptions.
     */
    @Test
    public void testUpdateNonExistingBlob() {
        tracker.track(new JobID(), BlobKey.createKey(BlobType.PERMANENT_BLOB), 2L);
        assertEquals(1, tracker.checkLimit(3L).size());

        tracker.update(new JobID(), BlobKey.createKey(BlobType.PERMANENT_BLOB));
        assertEquals(1, tracker.checkLimit(3L).size());
    }

    @Test
    public void testUntrackAll() {
        tracker.track(jobId, BlobKey.createKey(BlobType.PERMANENT_BLOB), 1L);

        JobID anotherJobId = new JobID();
        tracker.track(anotherJobId, BlobKey.createKey(BlobType.PERMANENT_BLOB), 1L);

        assertEquals(2, tracker.getBlobKeysByJobId(jobId).size());
        tracker.untrackAll(jobId);

        assertEquals(0, tracker.getBlobKeysByJobId(jobId).size());
        assertEquals(1, tracker.getBlobKeysByJobId(anotherJobId).size());
    }

    /**
     * Untracking all BLOBs for a non-existing job shouldn't change anything or throw any
     * exceptions.
     */
    @Test
    public void testUntrackAllWithNonExistingJob() {
        tracker.track(jobId, BlobKey.createKey(BlobType.PERMANENT_BLOB), 1L);

        assertEquals(2, tracker.getBlobKeysByJobId(jobId).size());
        tracker.untrackAll(new JobID());

        assertEquals(2, tracker.getBlobKeysByJobId(jobId).size());
    }
}
