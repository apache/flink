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
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.sink.FileSinkCommittable;
import org.apache.flink.connector.file.sink.compactor.FileCompactor;
import org.apache.flink.connector.file.sink.compactor.IdenticalFileCompactor;
import org.apache.flink.connector.file.sink.compactor.operator.CompactorOperator.RemainingRequestsSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableSummary;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.InProgressFileWriter.PendingFileRecoverable;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.util.SimpleVersionedListState;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.types.Either;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.apache.flink.connector.file.sink.compactor.operator.CompactorOperator.REMAINING_REQUESTS_RAW_STATES_DESC;
import static org.apache.flink.util.Preconditions.checkState;

/** Handler that processes the state of {@link CompactorOperator} when compaction is disabled. */
@Internal
public class CompactorOperatorStateHandler
        extends AbstractStreamOperator<CommittableMessage<FileSinkCommittable>>
        implements OneInputStreamOperator<
                        Either<CommittableMessage<FileSinkCommittable>, CompactorRequest>,
                        CommittableMessage<FileSinkCommittable>>,
                BoundedOneInput,
                CheckpointListener {

    private final SimpleVersionedSerializer<FileSinkCommittable> committableSerializer;
    private final BucketWriter<?, String> bucketWriter;

    private final FileCompactor fileCompactor;

    private transient CompactService compactService;

    // Flag indicating the in-progress file of the previous run from the writer has been received
    // and processed.
    private boolean writerStateDrained = false;

    // Flag indicating all compaction related states are drained, the operator can now pass through
    // everything.
    private boolean stateDrained = false;

    // There may be a in-progress file of the previous run that we have to process as a compaction
    // request first, or the file is invisible after committing.
    // We have to hold the summary and committables (of this run), and send them along with the
    // result of this compaction request, as well as the results of the remaining requests of this
    // operator, if there are.
    private CommittableSummary<FileSinkCommittable> holdingSummary;
    private List<CommittableMessage<FileSinkCommittable>> holdingMessages;
    private final List<CommittableMessage<FileSinkCommittable>> compactingMessages =
            new ArrayList<>();

    private final List<Tuple2<CompactorRequest, CompletableFuture<Iterable<FileSinkCommittable>>>>
            compactingRequests = new ArrayList<>();
    private SimpleVersionedListState<Map<Long, List<CompactorRequest>>> remainingRequestsState;
    private Iterable<Map<Long, List<CompactorRequest>>> stateRemaining;

    public CompactorOperatorStateHandler(
            SimpleVersionedSerializer<FileSinkCommittable> committableSerializer,
            BucketWriter<?, String> bucketWriter) {
        this.committableSerializer = committableSerializer;
        this.bucketWriter = bucketWriter;

        this.fileCompactor = new IdenticalFileCompactor();
    }

    @Override
    public void open() throws Exception {
        super.open();
        this.compactService = new CompactService(1, fileCompactor, bucketWriter);
        compactService.open();

        if (stateRemaining != null) {
            for (Map<Long, List<CompactorRequest>> requests : stateRemaining) {
                for (Map.Entry<Long, List<CompactorRequest>> e : requests.entrySet()) {
                    for (CompactorRequest request : e.getValue()) {
                        // each toCompact must be wrapped with a single compact request, since
                        // multiple files compacting is not available now
                        List<FileSinkCommittable> toCompactList = request.getCommittableToCompact();
                        List<FileSinkCommittable> toPassThrough =
                                request.getCommittableToPassthrough();

                        String bucketId =
                                !toCompactList.isEmpty()
                                        ? toCompactList.get(0).getBucketId()
                                        : toPassThrough.get(0).getBucketId();

                        for (FileSinkCommittable toCompact : toCompactList) {
                            CompactorRequest compactRequest = new CompactorRequest(bucketId);
                            compactRequest.addToCompact(toCompact);
                            submit(compactRequest);
                        }

                        CompactorRequest passThroughRequest = new CompactorRequest(bucketId);
                        toPassThrough.forEach(passThroughRequest::addToPassthrough);
                        submit(passThroughRequest);
                    }
                }
            }
        }
        stateRemaining = null;
    }

    @Override
    public void processElement(
            StreamRecord<Either<CommittableMessage<FileSinkCommittable>, CompactorRequest>> element)
            throws Exception {
        Either<CommittableMessage<FileSinkCommittable>, CompactorRequest> record =
                element.getValue();
        if (stateDrained) {
            // all input should be committable messages to pass through
            output.collect(new StreamRecord<>(record.left()));
            return;
        }

        if (record.isRight()) {
            submit(element.getValue().right());
            return;
        }

        CommittableMessage<FileSinkCommittable> message = record.left();
        if (message instanceof CommittableSummary) {
            checkState(holdingSummary == null, "Duplicate summary before the first checkpoint.");
            holdingSummary = (CommittableSummary<FileSinkCommittable>) message;
            holdingMessages = new ArrayList<>(holdingSummary.getNumberOfCommittables());
        } else {
            boolean compacting = false;
            CommittableWithLineage<FileSinkCommittable> committableWithLineage =
                    (CommittableWithLineage<FileSinkCommittable>) message;
            if (committableWithLineage.getCommittable().hasPendingFile()) {
                FileSinkCommittable committable = committableWithLineage.getCommittable();
                PendingFileRecoverable pendingFile = committable.getPendingFile();
                if (pendingFile.getPath() != null
                        && pendingFile.getPath().getName().startsWith(".")) {
                    // The pending file is the in-progress file of the previous run, which
                    // should be committed and compacted before sending to the committer.
                    CompactorRequest request = new CompactorRequest(committable.getBucketId());
                    request.addToCompact(committable);
                    submit(request);

                    compacting = true;
                    compactingMessages.add(message);
                } else {
                    // A normal file is received, indicating the writer state is drained.
                    writerStateDrained = true;
                    if (compactingMessages.isEmpty() && compactingRequests.isEmpty()) {
                        // No state needs to be handled, the holding summary and all committable
                        // messages can be sent eagerly
                        checkState(holdingSummary != null);
                        output.collect(new StreamRecord<>(holdingSummary));
                        holdingSummary = null;

                        this.stateDrained = true;
                        output.collect(new StreamRecord<>(committableWithLineage));
                    }
                }
            }
            if (!compacting && !stateDrained) {
                // Compacting messages should not be added
                // If the state is drained, no further messages need to be added
                holdingMessages.add(message);
            }
        }
    }

    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
        super.prepareSnapshotPreBarrier(checkpointId);
        if (stateDrained) {
            return;
        }
        drain();
        // The operator is stateless once drain is called. snapshotState is not necessary.
    }

    @Override
    public void endInput() throws Exception {
        if (!stateDrained) {
            drain();
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (compactService != null) {
            compactService.close();
        }
    }

    private void submit(CompactorRequest request) {
        CompletableFuture<Iterable<FileSinkCommittable>> resultFuture = new CompletableFuture<>();
        compactService.submit(request, resultFuture);
        compactingRequests.add(new Tuple2<>(request, resultFuture));
    }

    private void drain() throws ExecutionException, InterruptedException {
        checkState(holdingSummary != null);
        checkState(
                holdingSummary.getNumberOfPendingCommittables()
                                == holdingSummary.getNumberOfCommittables()
                        && holdingSummary.getNumberOfCommittables()
                                == holdingMessages.size() + compactingMessages.size());

        Long checkpointId =
                holdingSummary.getCheckpointId().isPresent()
                        ? holdingSummary.getCheckpointId().getAsLong()
                        : null;
        int subtaskId = holdingSummary.getSubtaskId();

        if (!compactingRequests.isEmpty()) {
            CompletableFuture.allOf(
                            compactingRequests.stream()
                                    .map(r -> r.f1)
                                    .toArray(CompletableFuture[]::new))
                    .join();

            for (Tuple2<CompactorRequest, CompletableFuture<Iterable<FileSinkCommittable>>>
                    compacting : compactingRequests) {
                CompletableFuture<Iterable<FileSinkCommittable>> future = compacting.f1;
                checkState(future.isDone());
                // Exception is thrown if it's completed exceptionally
                for (FileSinkCommittable c : future.get()) {
                    holdingMessages.add(new CommittableWithLineage<>(c, checkpointId, subtaskId));
                }
            }
        }

        // Appending the compacted committable to the holding summary
        CommittableSummary<FileSinkCommittable> summary =
                new CommittableSummary<>(
                        holdingSummary.getSubtaskId(),
                        holdingSummary.getNumberOfSubtasks(),
                        holdingSummary.getCheckpointId().isPresent()
                                ? holdingSummary.getCheckpointId().getAsLong()
                                : null,
                        holdingMessages.size(),
                        holdingMessages.size(),
                        holdingSummary.getNumberOfFailedCommittables());
        output.collect(new StreamRecord<>(summary));
        for (CommittableMessage<FileSinkCommittable> committable : holdingMessages) {
            output.collect(new StreamRecord<>(committable));
        }

        // Remaining requests should be all done and their results are all emitted.
        // From now on the operator is stateless.
        remainingRequestsState.clear();

        compactingRequests.clear();
        compactingMessages.clear();
        holdingSummary = null;
        holdingMessages = null;

        if (writerStateDrained) {
            // We can pass through everything if the writer state is also drained.
            stateDrained = true;
            compactService.close();
            compactService = null;
        }
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);

        remainingRequestsState =
                new SimpleVersionedListState<>(
                        context.getOperatorStateStore()
                                .getListState(REMAINING_REQUESTS_RAW_STATES_DESC),
                        new RemainingRequestsSerializer(
                                new CompactorRequestSerializer(committableSerializer)));

        stateRemaining = remainingRequestsState.get();

        // stateDrained can not be determined here, since even if the stateRemaining is empty,
        // there may still be some requests from the coordinator and a in-progress file in the file
        // writer
    }

    @VisibleForTesting
    public boolean isWriterStateDrained() {
        return writerStateDrained;
    }

    @VisibleForTesting
    public boolean isStateDrained() {
        return stateDrained;
    }
}
