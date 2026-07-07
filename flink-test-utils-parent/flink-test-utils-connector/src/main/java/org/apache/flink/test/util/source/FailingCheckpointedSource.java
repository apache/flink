/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.test.util.source;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.util.FlinkRuntimeException;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A test source with a checkpointed emission position that fails subtask 0 exactly once after a
 * completed checkpoint and resumes from the restored position, for verifying exactly-once behavior
 * across recovery.
 *
 * <p>Emission holds just past the {@link FailurePolicy} arming threshold until a snapshot arms the
 * failure, continues to the failure position, and throws once the armed checkpoint completes.
 * Recovery therefore always replays the records between the arming and failure positions,
 * independent of emission speed. A failing policy requires checkpointing to be enabled. All records
 * of one {@link EventGenerator#emit} call are emitted in a single {@code pollNext} and cannot be
 * split by a checkpoint. Rescaling is not supported; use a fixed source parallelism.
 *
 * <p>{@link #withIdleAfterEmission()} keeps the source idle instead of finishing (for
 * processing-time tests); {@link #withSimulatedStateLossOnRecovery()} deliberately breaks
 * exactly-once, only for validating that a test still detects duplicates.
 */
@Experimental
public class FailingCheckpointedSource<T>
        implements Source<T, SequenceSplit, Void>, ResultTypeQueryable<T> {

    private static final long serialVersionUID = 1L;

    /**
     * Generates and emits the records (and optionally timestamps and watermarks) for one sequence
     * number of one subtask.
     */
    @FunctionalInterface
    public interface EventGenerator<T> extends Serializable {
        void emit(int subtaskIndex, long sequenceNo, GeneratorOutput<T> output);
    }

    /** Output for the {@link EventGenerator}, bridging to the reader output. */
    public interface GeneratorOutput<T> {
        void collect(T record);

        void collect(T record, long timestamp);

        void emitWatermark(long timestamp);
    }

    /** Determines whether and when the source fails. */
    public static final class FailurePolicy implements Serializable {

        private static final long serialVersionUID = 1L;
        private static final long NEVER = -1;

        private final long failAfterEmitCalls;
        private final long armAfterEmitCalls;

        private FailurePolicy(long failAfterEmitCalls, long armAfterEmitCalls) {
            this.failAfterEmitCalls = failAfterEmitCalls;
            this.armAfterEmitCalls = armAfterEmitCalls;
        }

        /**
         * Fails subtask 0 once, after it has emitted {@code emitCalls} generator invocations and a
         * checkpoint taken past half of that has completed.
         */
        public static FailurePolicy failAfterEmitCalls(long emitCalls) {
            checkArgument(emitCalls >= 1, "emitCalls must be >= 1");
            return new FailurePolicy(emitCalls, emitCalls / 2);
        }

        /** Never fails; the source just emits and finishes. */
        public static FailurePolicy neverFail() {
            return new FailurePolicy(NEVER, NEVER);
        }

        boolean fails() {
            return failAfterEmitCalls != NEVER;
        }
    }

    private final EventGenerator<T> generator;
    private final long emitCallsPerSubtask;
    private final FailurePolicy failurePolicy;
    private final TypeInformation<T> producedType;
    private final boolean idleAfterEmission;
    private final boolean simulateStateLossOnRecovery;

    private FailingCheckpointedSource(
            EventGenerator<T> generator,
            long emitCallsPerSubtask,
            FailurePolicy failurePolicy,
            TypeInformation<T> producedType,
            boolean idleAfterEmission,
            boolean simulateStateLossOnRecovery) {
        this.generator = checkNotNull(generator);
        this.emitCallsPerSubtask = emitCallsPerSubtask;
        this.failurePolicy = checkNotNull(failurePolicy);
        this.producedType = checkNotNull(producedType);
        this.idleAfterEmission = idleAfterEmission;
        this.simulateStateLossOnRecovery = simulateStateLossOnRecovery;
        checkArgument(emitCallsPerSubtask >= 1, "emitCallsPerSubtask must be >= 1");
        checkArgument(
                !failurePolicy.fails() || failurePolicy.failAfterEmitCalls <= emitCallsPerSubtask,
                "the failure position must not lie beyond the emitted sequence");
    }

    public static <T> FailingCheckpointedSource<T> of(
            EventGenerator<T> generator,
            long emitCallsPerSubtask,
            FailurePolicy failurePolicy,
            TypeInformation<T> producedType) {
        return new FailingCheckpointedSource<>(
                generator, emitCallsPerSubtask, failurePolicy, producedType, false, false);
    }

    /**
     * Returns a copy of this source that stays idle after emitting all sequence numbers instead of
     * finishing, for processing-time tests whose job is terminated externally.
     */
    public FailingCheckpointedSource<T> withIdleAfterEmission() {
        return new FailingCheckpointedSource<>(
                generator,
                emitCallsPerSubtask,
                failurePolicy,
                producedType,
                true,
                simulateStateLossOnRecovery);
    }

    /**
     * Returns a copy of this source whose readers discard the restored position after recovery.
     * Only for validating that a test detects exactly-once violations; never use in a test's normal
     * path.
     */
    public FailingCheckpointedSource<T> withSimulatedStateLossOnRecovery() {
        return new FailingCheckpointedSource<>(
                generator,
                emitCallsPerSubtask,
                failurePolicy,
                producedType,
                idleAfterEmission,
                true);
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SourceReader<T, SequenceSplit> createReader(SourceReaderContext readerContext) {
        return new FailingCheckpointedSourceReader<>(
                generator,
                emitCallsPerSubtask,
                failurePolicy,
                idleAfterEmission,
                simulateStateLossOnRecovery);
    }

    @Override
    public SplitEnumerator<SequenceSplit, Void> createEnumerator(
            SplitEnumeratorContext<SequenceSplit> enumContext) {
        return new SequenceSplitEnumerator(enumContext, true);
    }

    @Override
    public SplitEnumerator<SequenceSplit, Void> restoreEnumerator(
            SplitEnumeratorContext<SequenceSplit> enumContext, Void checkpoint) {
        // After restore the readers recover their splits from their own state, so the enumerator
        // must not assign fresh splits to re-registering readers.
        return new SequenceSplitEnumerator(enumContext, false);
    }

    @Override
    public SimpleVersionedSerializer<SequenceSplit> getSplitSerializer() {
        return SequenceSplit.SERIALIZER;
    }

    @Override
    public SimpleVersionedSerializer<Void> getEnumeratorCheckpointSerializer() {
        return VoidSerializer.INSTANCE;
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return producedType;
    }

    /** Assigns one position split per subtask on fresh starts. */
    private static final class SequenceSplitEnumerator
            implements SplitEnumerator<SequenceSplit, Void> {

        private final SplitEnumeratorContext<SequenceSplit> context;
        private final boolean assignSplitsOnRegistration;
        private final Map<Integer, SequenceSplit> splitsToReassign = new HashMap<>();
        private final Set<Integer> assignedSubtasks = new HashSet<>();

        private SequenceSplitEnumerator(
                SplitEnumeratorContext<SequenceSplit> context, boolean assignSplitsOnRegistration) {
            this.context = context;
            this.assignSplitsOnRegistration = assignSplitsOnRegistration;
        }

        @Override
        public void start() {}

        @Override
        public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {}

        @Override
        public void addSplitsBack(List<SequenceSplit> splits, int subtaskId) {
            for (SequenceSplit split : splits) {
                splitsToReassign.put(split.getSubtaskIndex(), split);
            }
        }

        @Override
        public void addReader(int subtaskId) {
            final SequenceSplit returnedSplit = splitsToReassign.remove(subtaskId);
            if (returnedSplit != null) {
                context.assignSplit(returnedSplit, subtaskId);
            } else if (assignSplitsOnRegistration && assignedSubtasks.add(subtaskId)) {
                // only assign on the very first registration; on partial failover this
                // enumerator survives and the reader recovers its split from its own state
                context.assignSplit(new SequenceSplit(subtaskId, 0), subtaskId);
            }
        }

        @Override
        public Void snapshotState(long checkpointId) {
            return null;
        }

        @Override
        public void close() {}
    }

    /**
     * The reader. All methods are invoked in the task's mailbox thread, so no synchronization is
     * required between emission, snapshots, and checkpoint notifications.
     */
    private static final class FailingCheckpointedSourceReader<T>
            implements SourceReader<T, SequenceSplit> {

        private final EventGenerator<T> generator;
        private final long emitCallsPerSubtask;
        private final FailurePolicy failurePolicy;
        private final boolean idleAfterEmission;
        private final boolean simulateStateLossOnRecovery;
        private final BridgingGeneratorOutput generatorOutput = new BridgingGeneratorOutput();

        private boolean hasSplit;
        private int subtaskIndex;
        private long position;
        private boolean eligibleToFail;
        @Nullable private Long armedCheckpointId;
        private boolean failureRequested;
        private CompletableFuture<Void> availability = CompletableFuture.completedFuture(null);

        private FailingCheckpointedSourceReader(
                EventGenerator<T> generator,
                long emitCallsPerSubtask,
                FailurePolicy failurePolicy,
                boolean idleAfterEmission,
                boolean simulateStateLossOnRecovery) {
            this.generator = generator;
            this.emitCallsPerSubtask = emitCallsPerSubtask;
            this.failurePolicy = failurePolicy;
            this.idleAfterEmission = idleAfterEmission;
            this.simulateStateLossOnRecovery = simulateStateLossOnRecovery;
        }

        @Override
        public void start() {}

        @Override
        public InputStatus pollNext(ReaderOutput<T> output) {
            if (failureRequested && position >= failurePolicy.failAfterEmitCalls) {
                throw new FlinkRuntimeException("Artificial Failure");
            }
            if (!hasSplit) {
                return pause();
            }
            if (eligibleToFail) {
                if (armedCheckpointId == null && position > failurePolicy.armAfterEmitCalls) {
                    // hold until a snapshot arms, so the armed checkpoint provably captures a
                    // pre-failure position and recovery replays the records in between
                    return pause();
                }
                if (position >= failurePolicy.failAfterEmitCalls) {
                    // hold at the failure position until the armed checkpoint completes
                    return pause();
                }
            }
            if (position >= emitCallsPerSubtask) {
                return idleAfterEmission ? pause() : InputStatus.END_OF_INPUT;
            }
            generatorOutput.delegate = output;
            generator.emit(subtaskIndex, position++, generatorOutput);
            return InputStatus.MORE_AVAILABLE;
        }

        private InputStatus pause() {
            if (availability.isDone()) {
                availability = new CompletableFuture<>();
            }
            return InputStatus.NOTHING_AVAILABLE;
        }

        @Override
        public List<SequenceSplit> snapshotState(long checkpointId) {
            if (!hasSplit) {
                return Collections.emptyList();
            }
            // only arm past the threshold, so recovery demonstrably restores progress
            if (eligibleToFail
                    && armedCheckpointId == null
                    && position > failurePolicy.armAfterEmitCalls) {
                armedCheckpointId = checkpointId;
                // resume emission toward the failure position
                availability.complete(null);
            }
            return Collections.singletonList(new SequenceSplit(subtaskIndex, position));
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) {
            if (eligibleToFail && armedCheckpointId != null && armedCheckpointId == checkpointId) {
                failureRequested = true;
                availability.complete(null);
            }
        }

        @Override
        public void notifyCheckpointAborted(long checkpointId) {
            // let a later snapshot re-arm after an abort
            if (armedCheckpointId != null && armedCheckpointId == checkpointId) {
                armedCheckpointId = null;
            }
        }

        @Override
        public void addSplits(List<SequenceSplit> splits) {
            checkState(
                    !hasSplit && splits.size() == 1,
                    "expecting exactly one split per subtask, got %s (split already present: %s)",
                    splits,
                    hasSplit);
            final SequenceSplit split = splits.get(0);
            hasSplit = true;
            subtaskIndex = split.getSubtaskIndex();
            position = simulateStateLossOnRecovery ? 0 : split.getPosition();
            // restored position > 0 identifies the recovery attempt (no attempt number in
            // SourceReaderContext)
            eligibleToFail =
                    failurePolicy.fails()
                            && split.getSubtaskIndex() == 0
                            && split.getPosition() == 0;
            availability.complete(null);
        }

        @Override
        public CompletableFuture<Void> isAvailable() {
            return availability;
        }

        @Override
        public void notifyNoMoreSplits() {}

        @Override
        public void close() {
            availability.complete(null);
        }

        private final class BridgingGeneratorOutput implements GeneratorOutput<T> {

            @Nullable private ReaderOutput<T> delegate;

            @Override
            public void collect(T record) {
                delegate.collect(record);
            }

            @Override
            public void collect(T record, long timestamp) {
                delegate.collect(record, timestamp);
            }

            @Override
            public void emitWatermark(long timestamp) {
                delegate.emitWatermark(new Watermark(timestamp));
            }
        }
    }
}
