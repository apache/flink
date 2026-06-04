/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package org.apache.flink.runtime.source.coordinator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * A class that is responsible for tracking the past split assignments made by {@link
 * SplitEnumerator}.
 *
 * <p>This tracker now supports tombstone-based retention of removed splits to handle metadata
 * service instability in dynamic sources. When splits/partitions are removed due to temporary
 * metadata inconsistencies, they can be retained in checkpoint state for a configurable duration.
 * If the split reappears within the retention window, its progress is recovered instead of
 * starting from scratch.
 *
 * <p><b>Usage Example for Kafka Connector:</b>
 *
 * <pre>{@code
 * // In KafkaSourceEnumerator:
 *
 * // When discovering topology changes:
 * public void discoverNewPartitions() {
 *     Set<KafkaPartitionSplit> currentSplits = fetchCurrentTopology();
 *     Set<String> currentSplitIds = currentSplits.stream()
 *         .map(KafkaPartitionSplit::splitId)
 *         .collect(Collectors.toSet());
 *
 *     // Check for removed splits
 *     for (String existingSplitId : assignedSplits.keySet()) {
 *         if (!currentSplitIds.contains(existingSplitId)) {
 *             KafkaPartitionSplit removedSplit = assignedSplits.get(existingSplitId);
 *             int subtaskId = getAssignedSubtaskId(existingSplitId);
 *
 *             // Mark as removed (will be stored as tombstone)
 *             context.getAssignmentTracker()
 *                 .markSplitRemoved(existingSplitId, removedSplit, subtaskId);
 *
 *             assignedSplits.remove(existingSplitId);
 *         }
 *     }
 *
 *     // Check for new/resurrected splits
 *     for (KafkaPartitionSplit split : currentSplits) {
 *         if (!assignedSplits.containsKey(split.splitId())) {
 *             // Try to resurrect from tombstone
 *             RemovedSplitInfo<KafkaPartitionSplit> resurrected =
 *                 context.getAssignmentTracker().tryResurrectSplit(split.splitId());
 *
 *             if (resurrected != null) {
 *                 // Split was recently removed - restore its progress
 *                 KafkaPartitionSplit oldSplit = resurrected.getSplit();
 *                 split = new KafkaPartitionSplit(
 *                     split.getTopicPartition(),
 *                     oldSplit.getStartingOffset(), // Restore old offset!
 *                     split.getStoppingOffset()
 *                 );
 *                 LOG.info("Resurrected split {} from tombstone", split.splitId());
 *             } else {
 *                 // Truly new split - use configured starting offset
 *                 LOG.info("Discovered new split {}", split.splitId());
 *             }
 *
 *             assignSplitToReader(split);
 *         }
 *     }
 * }
 * }</pre>
 */
@Internal
public class SplitAssignmentTracker<SplitT extends SourceSplit> {
    // All the split assignments since the last successful checkpoint.
    // Maintaining this allow the subtasks to fail over independently.
    // The mapping is [CheckpointId -> [SubtaskId -> LinkedHashSet[SourceSplits]]].
    private final SortedMap<Long, Map<Integer, LinkedHashSet<SplitT>>> assignmentsByCheckpointId;
    // The split assignments since the last checkpoint attempt.
    // The mapping is [SubtaskId -> LinkedHashSet[SourceSplits]].
    private Map<Integer, LinkedHashSet<SplitT>> uncheckpointedAssignments;

    // Tombstone entries for removed splits to handle metadata service instability.
    // The mapping is [SplitId -> RemovedSplitInfo].
    // These entries are retained for a configurable duration to allow resurrection
    // if the split reappears due to metadata service flakiness.
    private final Map<String, RemovedSplitInfo<SplitT>> removedSplitsTombstones;

    // Duration in milliseconds to retain removed splits before permanently removing them.
    // A value of 0 means immediate removal (current behavior).
    private final long removedSplitsRetentionMs;

    public SplitAssignmentTracker() {
        this(0L);
    }

    public SplitAssignmentTracker(long removedSplitsRetentionMs) {
        this.assignmentsByCheckpointId = new TreeMap<>();
        this.uncheckpointedAssignments = new HashMap<>();
        this.removedSplitsTombstones = new HashMap<>();
        this.removedSplitsRetentionMs = removedSplitsRetentionMs;
    }

    /**
     * Represents a removed split with its removal timestamp and last known state.
     * Used to handle temporary metadata service inconsistencies.
     */
    @Internal
    static class RemovedSplitInfo<SplitT extends SourceSplit> {
        private final SplitT split;
        private final long removalTimestamp;
        private final int lastAssignedSubtaskId;

        RemovedSplitInfo(SplitT split, long removalTimestamp, int lastAssignedSubtaskId) {
            this.split = split;
            this.removalTimestamp = removalTimestamp;
            this.lastAssignedSubtaskId = lastAssignedSubtaskId;
        }

        public SplitT getSplit() {
            return split;
        }

        public long getRemovalTimestamp() {
            return removalTimestamp;
        }

        public int getLastAssignedSubtaskId() {
            return lastAssignedSubtaskId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            RemovedSplitInfo<?> that = (RemovedSplitInfo<?>) o;
            return removalTimestamp == that.removalTimestamp
                    && lastAssignedSubtaskId == that.lastAssignedSubtaskId
                    && Objects.equals(split.splitId(), that.split.splitId());
        }

        @Override
        public int hashCode() {
            return Objects.hash(split.splitId(), removalTimestamp, lastAssignedSubtaskId);
        }
    }

    /**
     * Behavior of SplitAssignmentTracker on checkpoint. Tracker will mark uncheckpointed assignment
     * as checkpointed with current checkpoint ID.
     *
     * @param checkpointId the id of the ongoing checkpoint
     */
    public void onCheckpoint(long checkpointId) throws Exception {
        // Include the uncheckpointed assignments to the snapshot.
        assignmentsByCheckpointId.put(checkpointId, uncheckpointedAssignments);
        uncheckpointedAssignments = new HashMap<>();
    }

    /**
     * Take a snapshot of the split assignments and tombstones.
     * The snapshot includes both uncheckpointed assignments and tombstone entries.
     */
    public byte[] snapshotState(SimpleVersionedSerializer<SplitT> splitSerializer)
            throws Exception {
        // Serialize assignments
        byte[] assignmentsData =
                SourceCoordinatorSerdeUtils.serializeAssignments(
                        uncheckpointedAssignments, splitSerializer);

        // Serialize tombstones
        byte[] tombstonesData =
                SourceCoordinatorSerdeUtils.serializeTombstones(
                        removedSplitsTombstones, splitSerializer);

        // Combine both into a single snapshot
        try (java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
                java.io.DataOutputStream out = new java.io.DataOutputStream(baos)) {
            // Write assignments length and data
            out.writeInt(assignmentsData.length);
            out.write(assignmentsData);

            // Write tombstones length and data
            out.writeInt(tombstonesData.length);
            out.write(tombstonesData);

            out.flush();
            return baos.toByteArray();
        }
    }

    /**
     * Restore the state of the SplitAssignmentTracker including tombstones.
     * Supports backward compatibility with older versions that don't have tombstones.
     *
     * @param splitSerializer The serializer of the splits.
     * @param assignmentData The state of the SplitAssignmentTracker.
     * @throws Exception when the state deserialization fails.
     */
    public void restoreState(
            SimpleVersionedSerializer<SplitT> splitSerializer, byte[] assignmentData)
            throws Exception {
        try (java.io.ByteArrayInputStream bais = new java.io.ByteArrayInputStream(assignmentData);
                java.io.DataInputStream in = new java.io.DataInputStream(bais)) {

            // Read assignments
            int assignmentsLength = in.readInt();
            byte[] assignmentsBytes = new byte[assignmentsLength];
            in.readFully(assignmentsBytes);
            uncheckpointedAssignments =
                    SourceCoordinatorSerdeUtils.deserializeAssignments(
                            assignmentsBytes, splitSerializer);

            // Try to read tombstones (may not exist in older checkpoints)
            if (in.available() > 0) {
                int tombstonesLength = in.readInt();
                if (tombstonesLength > 0) {
                    byte[] tombstonesBytes = new byte[tombstonesLength];
                    in.readFully(tombstonesBytes);
                    Map<String, RemovedSplitInfo<SplitT>> restoredTombstones =
                            SourceCoordinatorSerdeUtils.deserializeTombstones(
                                    tombstonesBytes, splitSerializer);
                    removedSplitsTombstones.putAll(restoredTombstones);
                }
            }
            // If tombstones don't exist in checkpoint, that's fine - backward compatibility
        } catch (java.io.EOFException e) {
            // Backward compatibility: old checkpoints don't have tombstones
            // This is expected and should not cause an error
        }
    }

    /**
     * when a checkpoint has been successfully made, this method is invoked to clean up the
     * assignment history before this successful checkpoint.
     *
     * @param checkpointId the id of the successful checkpoint.
     */
    public void onCheckpointComplete(long checkpointId) {
        assignmentsByCheckpointId.entrySet().removeIf(entry -> entry.getKey() <= checkpointId);
        cleanupExpiredTombstones();
    }

    /**
     * Remove expired tombstone entries based on the retention duration.
     * This is called after successful checkpoints to avoid accumulating stale entries.
     */
    private void cleanupExpiredTombstones() {
        if (removedSplitsRetentionMs == 0) {
            // Immediate removal (current behavior), no tombstones to clean
            return;
        }

        long currentTime = System.currentTimeMillis();
        Iterator<Map.Entry<String, RemovedSplitInfo<SplitT>>> iterator =
                removedSplitsTombstones.entrySet().iterator();

        while (iterator.hasNext()) {
            Map.Entry<String, RemovedSplitInfo<SplitT>> entry = iterator.next();
            long removalTimestamp = entry.getValue().getRemovalTimestamp();
            if (currentTime - removalTimestamp > removedSplitsRetentionMs) {
                iterator.remove();
            }
        }
    }

    /**
     * Mark a split as removed and store it as a tombstone if retention is enabled.
     * If the split reappears within the retention window, its progress can be recovered.
     *
     * @param splitId the ID of the split being removed
     * @param split the split object
     * @param lastAssignedSubtaskId the subtask ID where this split was last assigned
     */
    public void markSplitRemoved(String splitId, SplitT split, int lastAssignedSubtaskId) {
        if (removedSplitsRetentionMs > 0) {
            long currentTime = System.currentTimeMillis();
            removedSplitsTombstones.put(
                    splitId, new RemovedSplitInfo<>(split, currentTime, lastAssignedSubtaskId));
        }
        // If retention is 0, we don't store tombstones (current immediate removal behavior)
    }

    /**
     * Check if a split that is reappearing should be resurrected from tombstone state.
     * If the split was recently removed (within retention window), return its previous state.
     *
     * @param splitId the ID of the split to check
     * @return the RemovedSplitInfo if the split should be resurrected, null otherwise
     */
    public RemovedSplitInfo<SplitT> tryResurrectSplit(String splitId) {
        if (removedSplitsRetentionMs == 0) {
            // No resurrection when retention is disabled
            return null;
        }

        RemovedSplitInfo<SplitT> tombstone = removedSplitsTombstones.get(splitId);
        if (tombstone == null) {
            // No tombstone found
            return null;
        }

        long currentTime = System.currentTimeMillis();
        long removalTimestamp = tombstone.getRemovalTimestamp();

        if (currentTime - removalTimestamp <= removedSplitsRetentionMs) {
            // Within retention window - resurrect the split
            removedSplitsTombstones.remove(splitId);
            return tombstone;
        } else {
            // Expired - remove tombstone and treat as new split
            removedSplitsTombstones.remove(splitId);
            return null;
        }
    }

    /**
     * Get all current tombstone entries (for testing and serialization).
     *
     * @return map of split IDs to their removal information
     */
    @VisibleForTesting
    Map<String, RemovedSplitInfo<SplitT>> getRemovedSplitsTombstones() {
        return Collections.unmodifiableMap(removedSplitsTombstones);
    }

    /**
     * Record a new split assignment.
     *
     * @param splitsAssignment the new split assignment.
     */
    public void recordSplitAssignment(SplitsAssignment<SplitT> splitsAssignment) {
        addSplitAssignment(splitsAssignment, uncheckpointedAssignments);
    }

    /**
     * This method is invoked when a source reader fails over. In this case, the source reader will
     * restore its split assignment to the last successful checkpoint. Any split assignment to that
     * source reader after the last successful checkpoint will be lost on the source reader side as
     * if those splits were never assigned. To handle this case, the coordinator needs to find those
     * splits and return them back to the SplitEnumerator for re-assignment.
     *
     * @param subtaskId the subtask id of the reader that failed over.
     * @param restoredCheckpointId the ID of the checkpoint that the reader was restored to.
     * @return A list of splits that needs to be added back to the {@link SplitEnumerator}.
     */
    public List<SplitT> getAndRemoveUncheckpointedAssignment(
            int subtaskId, long restoredCheckpointId) {
        final ArrayList<SplitT> splits = new ArrayList<>();

        for (final Map.Entry<Long, Map<Integer, LinkedHashSet<SplitT>>> entry :
                assignmentsByCheckpointId.entrySet()) {
            if (entry.getKey() > restoredCheckpointId) {
                removeFromAssignment(subtaskId, entry.getValue(), splits);
            }
        }

        removeFromAssignment(subtaskId, uncheckpointedAssignments, splits);
        return splits;
    }

    // ------------- Methods visible for testing ----------------

    @VisibleForTesting
    SortedMap<Long, Map<Integer, LinkedHashSet<SplitT>>> assignmentsByCheckpointId() {
        return assignmentsByCheckpointId;
    }

    @VisibleForTesting
    Map<Integer, LinkedHashSet<SplitT>> assignmentsByCheckpointId(long checkpointId) {
        return assignmentsByCheckpointId.get(checkpointId);
    }

    Map<Integer, LinkedHashSet<SplitT>> uncheckpointedAssignments() {
        return Collections.unmodifiableMap(uncheckpointedAssignments);
    }

    // -------------- private helpers ---------------

    private void removeFromAssignment(
            int subtaskId,
            Map<Integer, LinkedHashSet<SplitT>> assignments,
            List<SplitT> toPutBack) {
        Set<SplitT> splitForSubtask = assignments.remove(subtaskId);
        if (splitForSubtask != null) {
            toPutBack.addAll(splitForSubtask);
        }
    }

    private void addSplitAssignment(
            SplitsAssignment<SplitT> additionalAssignment,
            Map<Integer, LinkedHashSet<SplitT>> assignments) {
        additionalAssignment
                .assignment()
                .forEach(
                        (id, splits) ->
                                assignments
                                        .computeIfAbsent(id, ignored -> new LinkedHashSet<>())
                                        .addAll(splits));
    }
}
