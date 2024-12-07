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
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.sink.FileSinkCommittable;
import org.apache.flink.connector.file.sink.compactor.IdenticalFileCompactor;
import org.apache.flink.connector.file.sink.compactor.operator.CompactorOperator.RemainingRequestsSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableSummary;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketWriter;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.operators.util.SimpleVersionedListState;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.types.Either;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
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

    private transient CompactService compactService;
    private final List<Tuple2<CompactorRequest, CompletableFuture<Iterable<FileSinkCommittable>>>>
            compactingRequests = new LinkedList<>();

    private SimpleVersionedListState<Map<Long, List<CompactorRequest>>> remainingRequestsState;
    private Iterable<Map<Long, List<CompactorRequest>>> stateRemaining;

    public CompactorOperatorStateHandler(
            StreamOperatorParameters<CommittableMessage<FileSinkCommittable>> parameters,
            SimpleVersionedSerializer<FileSinkCommittable> committableSerializer,
            BucketWriter<?, String> bucketWriter) {
        super(parameters);
        this.committableSerializer = committableSerializer;
        this.bucketWriter = bucketWriter;
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
        // CompactService is not initialized yet, we can not submit requests here.
        remainingRequestsState =
                new SimpleVersionedListState<>(
                        context.getOperatorStateStore()
                                .getListState(REMAINING_REQUESTS_RAW_STATES_DESC),
                        new RemainingRequestsSerializer(
                                new CompactorRequestSerializer(committableSerializer)));
        stateRemaining = remainingRequestsState.get();
    }

    @Override
    public void open() throws Exception {
        super.open();
        this.compactService = new CompactService(1, new IdenticalFileCompactor(), bucketWriter);
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

                        String bucketId = request.getBucketId();

                        for (FileSinkCommittable toCompact : toCompactList) {
                            CompactorRequest compactRequest = new CompactorRequest(bucketId);
                            compactRequest.addToCompact(toCompact);
                            compactingRequests.add(
                                    new Tuple2<>(compactRequest, submit(compactRequest)));
                        }

                        CompactorRequest passThroughRequest = new CompactorRequest(bucketId);
                        toPassThrough.forEach(passThroughRequest::addToPassthrough);
                        compactingRequests.add(
                                new Tuple2<>(passThroughRequest, submit(passThroughRequest)));
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

        if (record.isLeft()) {
            CommittableMessage<FileSinkCommittable> message = record.left();
            if (message instanceof CommittableWithLineage) {
                if (isHiddenCommittable((CommittableWithLineage<FileSinkCommittable>) message)) {
                    handleHiddenCommittable((CommittableWithLineage<FileSinkCommittable>) message);
                } else {
                    output.collect(new StreamRecord<>(message));
                }
            } else {
                if (compactingRequests.isEmpty()) {
                    output.collect(new StreamRecord<>(message));
                } else {
                    appendCompactingResultsToSummary(
                            (CommittableSummary<FileSinkCommittable>) message);
                }
            }
        } else {
            CompactorRequest request = element.getValue().right();
            compactingRequests.add(new Tuple2<>(request, submit(request)));
        }
    }

    private void appendCompactingResultsToSummary(CommittableSummary<FileSinkCommittable> summary)
            throws ExecutionException, InterruptedException {
        // To guarantee the order, we have to wait for all results here.
        List<FileSinkCommittable> results = new ArrayList<>();
        for (Tuple2<CompactorRequest, CompletableFuture<Iterable<FileSinkCommittable>>> t :
                compactingRequests) {
            t.f1.get().forEach(results::add);
        }
        compactingRequests.clear();

        // Append the results to the summary and send them following it
        output.collect(
                new StreamRecord<>(
                        new CommittableSummary<>(
                                summary.getSubtaskId(),
                                summary.getNumberOfSubtasks(),
                                summary.getCheckpointIdOrEOI(),
                                summary.getNumberOfCommittables() + results.size(),
                                summary.getNumberOfFailedCommittables())));
        for (FileSinkCommittable committable : results) {
            output.collect(
                    new StreamRecord<>(
                            new CommittableWithLineage<>(
                                    committable,
                                    summary.getCheckpointIdOrEOI(),
                                    summary.getSubtaskId())));
        }
    }

    private boolean isHiddenCommittable(CommittableWithLineage<FileSinkCommittable> message) {
        return message.getCommittable().hasPendingFile()
                && message.getCommittable().getPendingFile().getPath() != null
                && message.getCommittable().getPendingFile().getPath().getName().startsWith(".");
    }

    private void handleHiddenCommittable(CommittableWithLineage<FileSinkCommittable> message)
            throws ExecutionException, InterruptedException {
        FileSinkCommittable committable = message.getCommittable();

        // The pending file is the in-progress file of the previous run, which
        // should be committed and compacted before sending to the committer.
        CompactorRequest request = new CompactorRequest(committable.getBucketId());
        request.addToCompact(committable);

        // Wait for the result synchronously, and pass though the result, but append
        // cleanup request to the next summary, since the count of pending committable
        // for this checkpoint is immutable now
        Iterable<FileSinkCommittable> result = submit(request).get();
        Long checkpointId = message.getCheckpointIdOrEOI();
        boolean pendingFileSent = false;
        for (FileSinkCommittable c : result) {
            if (c.hasPendingFile()) {
                checkState(
                        !pendingFileSent,
                        "A in-progress file should not be converted to multiple pending files");
                pendingFileSent = true;
                output.collect(
                        new StreamRecord<>(
                                new CommittableWithLineage<>(
                                        c, checkpointId, message.getSubtaskId())));
            } else {
                // Wrap cleanup request as pass through request and reserved in the
                // compacting requests.
                // These requests will be appended to the next summary, if there is.
                CompactorRequest passThroughRequest = new CompactorRequest(c.getBucketId());
                passThroughRequest.addToPassthrough(c);
                compactingRequests.add(
                        new Tuple2<>(passThroughRequest, submit(passThroughRequest)));
            }
        }
    }

    @Override
    public void endInput() throws Exception {
        // Although there may be remaining cleanup requests in compactingRequests, there is no way
        // to let Committer accepts them since the eoi summary has been sent.
        // For now we can do nothing but leave them there.
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (compactService != null) {
            compactService.close();
        }
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);
        if (compactingRequests.isEmpty()) {
            return;
        }

        // Results of some requests are not drained by a summary. They should be reserved in the
        // state and wait for the next summary.

        List<CompactorRequest> remainingRequests = new ArrayList<>();
        for (Tuple2<CompactorRequest, CompletableFuture<Iterable<FileSinkCommittable>>> t :
                compactingRequests) {
            if (t.f1.isDone()) {
                // We can add the results as a pass-through request if the compaction is done
                Iterable<FileSinkCommittable> result = t.f1.get();
                if (result.iterator().hasNext()) {
                    String bucketId = result.iterator().next().getBucketId();
                    CompactorRequest passThroughRequest = new CompactorRequest(bucketId);
                    result.forEach(passThroughRequest::addToPassthrough);
                    remainingRequests.add(passThroughRequest);
                }
            } else {
                // Or we add the original request in the state
                remainingRequests.add(t.f0);
            }
        }
        Map<Long, List<CompactorRequest>> requestsMap = new HashMap<>();
        requestsMap.put(-1L, remainingRequests);
        remainingRequestsState.update(Collections.singletonList(requestsMap));
    }

    private CompletableFuture<Iterable<FileSinkCommittable>> submit(CompactorRequest request) {
        CompletableFuture<Iterable<FileSinkCommittable>> resultFuture = new CompletableFuture<>();
        compactService.submit(request, resultFuture);
        return resultFuture;
    }
}
