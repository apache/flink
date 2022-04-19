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

import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.connector.file.sink.FileSinkCommittable;
import org.apache.flink.connector.file.sink.compactor.FileCompactStrategy;
import org.apache.flink.connector.file.sink.compactor.operator.CompactCoordinator.CompactTrigger;
import org.apache.flink.connector.file.sink.compactor.operator.CompactCoordinator.CompactTriggerResult;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.util.SimpleVersionedListState;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.types.Either;

import static org.apache.flink.connector.file.sink.compactor.operator.CompactCoordinator.REMAINING_COMMITTABLE_RAW_STATES_DESC;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Handler that processes the state of {@link CompactCoordinator} when compaction is disabled. */
public class CompactCoordinatorStateHandler
        extends AbstractStreamOperator<
                Either<CommittableMessage<FileSinkCommittable>, CompactorRequest>>
        implements OneInputStreamOperator<
                        CommittableMessage<FileSinkCommittable>,
                        Either<CommittableMessage<FileSinkCommittable>, CompactorRequest>>,
                BoundedOneInput,
                CheckpointListener {

    private final SimpleVersionedSerializer<FileSinkCommittable> committableSerializer;

    public CompactCoordinatorStateHandler(
            SimpleVersionedSerializer<FileSinkCommittable> committableSerializer) {
        this.committableSerializer = checkNotNull(committableSerializer);
    }

    @Override
    public void processElement(StreamRecord<CommittableMessage<FileSinkCommittable>> element)
            throws Exception {
        output.collect(new StreamRecord<>(Either.Left(element.getValue())));
    }

    @Override
    public void endInput() throws Exception {}

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);

        ListState<FileSinkCommittable> remainingCommittableState =
                new SimpleVersionedListState<>(
                        context.getOperatorStateStore()
                                .getListState(REMAINING_COMMITTABLE_RAW_STATES_DESC),
                        committableSerializer);

        // A default trigger to judge whether a pending file should be compacted or passed through
        CompactTrigger trigger =
                new CompactTrigger(
                        FileCompactStrategy.Builder.newBuilder().setSizeThreshold(0).build());

        Iterable<FileSinkCommittable> stateRemaining = remainingCommittableState.get();
        if (stateRemaining != null) {
            for (FileSinkCommittable committable : stateRemaining) {
                // all committable should be wrapped with a single request, since multiple files
                // compacting is not available now
                String bucketId = committable.getBucketId();
                CompactorRequest request = new CompactorRequest(bucketId);
                if (committable.hasPendingFile()
                        && trigger.onElement(committable) != CompactTriggerResult.PASS_THROUGH) {
                    request.addToCompact(committable);
                } else {
                    request.addToPassthrough(committable);
                }
                output.collect(new StreamRecord<>(Either.Right(request)));
            }
        }

        // Remaining committable should be all emitted, and the state can be cleared. From now on
        // the operator is stateless, snapshotState is not necessary.
        remainingCommittableState.clear();
    }
}
