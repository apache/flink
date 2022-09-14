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

package org.apache.flink.connector.file.sink.compactor.operator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.sink.FileSinkCommittable;
import org.apache.flink.connector.file.sink.compactor.FileCompactStrategy;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;
import org.apache.flink.streaming.api.functions.sink.filesystem.InProgressFileWriter.PendingFileRecoverable;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.util.SimpleVersionedListState;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Coordinator that coordinates file compaction for the {@link FileSink}.
 *
 * <p>All committable emitted from the writers are collected and packed into {@link
 * CompactorRequest}s. The {@link FileCompactStrategy} defines when the requests can be fired. When
 * a firing condition is met, the requests will be sent to the {@link CompactorOperator}.
 *
 * <p>The {@link CompactCoordinator} stores the non-fired committable as its state, and may emit a
 * request at any time. A {@link CompactorOperator} must ensure that the ownership of the
 * committable in a compact request is successfully handed from the coordinator, before it can
 * actually perform the compaction.
 */
@Internal
public class CompactCoordinator extends AbstractStreamOperator<CompactorRequest>
        implements OneInputStreamOperator<
                        CommittableMessage<FileSinkCommittable>, CompactorRequest>,
                BoundedOneInput {

    static final ListStateDescriptor<byte[]> REMAINING_COMMITTABLE_RAW_STATES_DESC =
            new ListStateDescriptor<>(
                    "remaining_compact_commit_raw_state", BytePrimitiveArraySerializer.INSTANCE);

    private final FileCompactStrategy strategy;
    private final SimpleVersionedSerializer<FileSinkCommittable> committableSerializer;

    private final Map<String, CompactorRequest> packingRequests = new HashMap<>();
    private final Map<String, CompactTrigger> triggers = new HashMap<>();

    private ListState<FileSinkCommittable> remainingCommittableState;

    public CompactCoordinator(
            FileCompactStrategy strategy,
            SimpleVersionedSerializer<FileSinkCommittable> committableSerializer) {
        this.strategy = strategy;
        this.committableSerializer = checkNotNull(committableSerializer);
    }

    @Override
    public void processElement(StreamRecord<CommittableMessage<FileSinkCommittable>> element)
            throws Exception {
        CommittableMessage<FileSinkCommittable> message = element.getValue();
        if (message instanceof CommittableWithLineage) {
            FileSinkCommittable committable =
                    ((CommittableWithLineage<FileSinkCommittable>) element.getValue())
                            .getCommittable();
            if (packAndTrigger(committable)) {
                fireAndPurge(committable.getBucketId());
            }
        }
        // or message instanceof CommittableSummary
        // info in CommittableSummary is not necessary for compacting at present, ignore it
    }

    private boolean packAndTrigger(FileSinkCommittable committable) {
        String bucketId = committable.getBucketId();
        CompactorRequest bucketRequest =
                packingRequests.computeIfAbsent(bucketId, CompactorRequest::new);
        if (committable.hasInProgressFileToCleanup() || committable.hasCompactedFileToCleanup()) {
            checkState(!committable.hasPendingFile());
            // cleanup request, pass through directly
            bucketRequest.addToPassthrough(committable);
            return false;
        }

        if (!committable.hasPendingFile()) {
            throw new RuntimeException("Committable to compact has no content.");
        }

        CompactTrigger trigger =
                triggers.computeIfAbsent(bucketId, id -> new CompactTrigger(strategy));
        CompactTriggerResult triggerResult = trigger.onElement(committable);
        switch (triggerResult) {
            case PASS_THROUGH:
                bucketRequest.addToPassthrough(committable);
                return false;
            case CONTINUE:
                bucketRequest.addToCompact(committable);
                return false;
            case FIRE_AND_PURGE:
                bucketRequest.addToCompact(committable);
                return true;
            default:
                throw new RuntimeException("Unexpected trigger result:" + triggerResult);
        }
    }

    private void fireAndPurge(String bucketId) {
        triggers.remove(bucketId);
        CompactorRequest request = packingRequests.remove(bucketId);
        if (request != null) {
            output.collect(new StreamRecord<>(request));
        }
    }

    @Override
    public void endInput() throws Exception {
        // emit all requests remained
        for (CompactorRequest request : packingRequests.values()) {
            output.collect(new StreamRecord<>(request));
        }
        packingRequests.clear();
        triggers.clear();
    }

    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
        super.prepareSnapshotPreBarrier(checkpointId);

        // trigger on checkpoint
        List<String> bucketsToFire = new ArrayList<>(triggers.size());
        for (Map.Entry<String, CompactTrigger> e : triggers.entrySet()) {
            String bucketId = e.getKey();
            CompactTrigger trigger = e.getValue();
            if (trigger.onCheckpoint(checkpointId) == CompactTriggerResult.FIRE_AND_PURGE) {
                bucketsToFire.add(bucketId);
            }
        }
        bucketsToFire.forEach(this::fireAndPurge);
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);

        List<FileSinkCommittable> remainingCommittable =
                packingRequests.values().stream()
                        .flatMap(r -> r.getCommittableToCompact().stream())
                        .collect(Collectors.toList());
        packingRequests.values().stream()
                .flatMap(r -> r.getCommittableToPassthrough().stream())
                .forEach(remainingCommittable::add);
        remainingCommittableState.update(remainingCommittable);

        // triggers will be recomputed when restoring so it's not necessary to store
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);

        remainingCommittableState =
                new SimpleVersionedListState<>(
                        context.getOperatorStateStore()
                                .getListState(REMAINING_COMMITTABLE_RAW_STATES_DESC),
                        committableSerializer);

        Iterable<FileSinkCommittable> stateRemaining = remainingCommittableState.get();
        if (stateRemaining != null) {
            for (FileSinkCommittable committable : stateRemaining) {
                // restore and redistribute
                if (packAndTrigger(committable)) {
                    fireAndPurge(committable.getBucketId());
                }
            }
        }
    }

    enum CompactTriggerResult {
        CONTINUE,
        FIRE_AND_PURGE,
        PASS_THROUGH
    }

    static class CompactTrigger {
        private final long threshold;
        private final int numCheckpointsBeforeCompaction;

        private long size;
        private long triggeredCpId = -1;

        CompactTrigger(FileCompactStrategy strategy) {
            this.threshold = strategy.getSizeThreshold();
            this.numCheckpointsBeforeCompaction = strategy.getNumCheckpointsBeforeCompaction();
        }

        public CompactTriggerResult onElement(FileSinkCommittable committable) {
            PendingFileRecoverable file = committable.getPendingFile();
            if (file == null) {
                return CompactTriggerResult.PASS_THROUGH;
            }

            if (file.getPath() == null || !file.getPath().getName().startsWith(".")) {
                // the file may be written with writer of elder version, or
                // the file will be visible once committed, so it can not be compacted.
                // pass through, add to results, do not add to compacting files
                return CompactTriggerResult.PASS_THROUGH;
            }

            long curSize = file.getSize();
            if (curSize < 0) {
                // unrecognized committable, can not compact, pass through directly
                return CompactTriggerResult.PASS_THROUGH;
            }

            if (threshold < 0) {
                return CompactTriggerResult.CONTINUE;
            }

            size += curSize;
            return size >= threshold
                    ? CompactTriggerResult.FIRE_AND_PURGE
                    : CompactTriggerResult.CONTINUE;
        }

        public CompactTriggerResult onCheckpoint(long checkpointId) {
            if (numCheckpointsBeforeCompaction < 0) {
                return CompactTriggerResult.CONTINUE;
            }
            if (triggeredCpId < 0) {
                triggeredCpId = checkpointId - 1;
            }
            return checkpointId - triggeredCpId >= numCheckpointsBeforeCompaction
                    ? CompactTriggerResult.FIRE_AND_PURGE
                    : CompactTriggerResult.CONTINUE;
        }
    }
}
