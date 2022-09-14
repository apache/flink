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
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.sink.FileSinkCommittable;
import org.apache.flink.connector.file.sink.compactor.FileCompactStrategy;
import org.apache.flink.connector.file.sink.compactor.FileCompactor;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableSummary;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketWriter;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.util.SimpleVersionedListState;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * An operator that perform compaction for the {@link FileSink}.
 *
 * <p>Requests received from the {@link CompactCoordinator} will firstly be held in memory, and
 * snapshot into the state of a checkpoint. When the checkpoint is successfully completed, all
 * requests received before can be submitted. The results can be emitted at the next {@link
 * #prepareSnapshotPreBarrier} invoking after the compaction is finished, to ensure that committers
 * can receive only one CommittableSummary and the corresponding number of Committable for a single
 * checkpoint.
 */
@Internal
public class CompactorOperator
        extends AbstractStreamOperator<CommittableMessage<FileSinkCommittable>>
        implements OneInputStreamOperator<
                        CompactorRequest, CommittableMessage<FileSinkCommittable>>,
                BoundedOneInput,
                CheckpointListener {

    private static final long SUBMITTED_ID = -1L;

    static final ListStateDescriptor<byte[]> REMAINING_REQUESTS_RAW_STATES_DESC =
            new ListStateDescriptor<>(
                    "remaining_requests_raw_state", BytePrimitiveArraySerializer.INSTANCE);

    private final FileCompactStrategy strategy;
    private final SimpleVersionedSerializer<FileSinkCommittable> committableSerializer;

    private final FileCompactor fileCompactor;
    private final BucketWriter<?, String> bucketWriter;

    private transient CompactService compactService;

    // collecting requests during the current checkpoint
    private List<CompactorRequest> collectingRequests = new ArrayList<>();
    // checkpoint id to its collecting requests, the requests is added when checkpointing, and are
    // moved to compacting requests once submitted.
    private final TreeMap<Long, List<CompactorRequest>> checkpointRequests = new TreeMap<>();
    // the currently compacting requests and their completable futures, completed requests are
    // removed and the results are emitted at #prepareSnapshotPreBarrier
    private final List<Tuple2<CompactorRequest, CompletableFuture<Iterable<FileSinkCommittable>>>>
            compactingRequests = new LinkedList<>();

    // state combining checkpointRequests and compactingRequests, compactingRequests will be
    // submitted again while restoring
    private ListState<Map<Long, List<CompactorRequest>>> remainingRequestsState;

    public CompactorOperator(
            FileCompactStrategy strategy,
            SimpleVersionedSerializer<FileSinkCommittable> committableSerializer,
            FileCompactor fileCompactor,
            BucketWriter<?, String> bucketWriter) {
        this.strategy = strategy;
        this.committableSerializer = committableSerializer;
        this.fileCompactor = fileCompactor;
        this.bucketWriter = bucketWriter;
    }

    @Override
    public void open() throws Exception {
        super.open();
        this.compactService =
                new CompactService(strategy.getNumCompactThreads(), fileCompactor, bucketWriter);
        compactService.open();

        // submit all requests that is already submitted before restored checkpoint
        submitUntil(SUBMITTED_ID);
    }

    @Override
    public void processElement(StreamRecord<CompactorRequest> element) throws Exception {
        collectingRequests.add(element.getValue());
    }

    @Override
    public void endInput() throws Exception {
        // add collecting requests into the final snapshot
        checkpointRequests.put(Long.MAX_VALUE, collectingRequests);
        collectingRequests = new ArrayList<>();

        // submit all requests and wait until they are done
        submitUntil(Long.MAX_VALUE);
        assert checkpointRequests.isEmpty();

        getAllTasksFuture().join();
        emitCompacted(null);
        assert compactingRequests.isEmpty();
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        super.notifyCheckpointComplete(checkpointId);
        submitUntil(checkpointId);
    }

    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
        emitCompacted(checkpointId);
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);

        // add collecting requests during the checkpoint into the snapshot
        checkpointRequests.put(context.getCheckpointId(), collectingRequests);
        collectingRequests = new ArrayList<>();

        // snapshot all compacting requests as well, including the requests that are not finished
        // when invoking prepareSnapshotPreBarrier but finished now, since they are not emitted yet
        Map<Long, List<CompactorRequest>> requests = new HashMap<>(checkpointRequests);
        requests.computeIfAbsent(SUBMITTED_ID, id -> new ArrayList<>())
                .addAll(compactingRequests.stream().map(r -> r.f0).collect(Collectors.toList()));
        remainingRequestsState.update(Collections.singletonList(requests));
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

        Iterable<Map<Long, List<CompactorRequest>>> stateRemaining = remainingRequestsState.get();
        if (stateRemaining != null) {
            for (Map<Long, List<CompactorRequest>> requests : stateRemaining) {
                // elements can be more than one when redistributed after parallelism changing
                for (Map.Entry<Long, List<CompactorRequest>> e : requests.entrySet()) {
                    List<CompactorRequest> list =
                            checkpointRequests.computeIfAbsent(e.getKey(), id -> new ArrayList<>());
                    list.addAll(e.getValue());
                }
            }
        }
        // open is called afterwards, so do not submit here since compact service is not opened yet
    }

    @Override
    public void close() throws Exception {
        if (compactService != null) {
            compactService.close();
        }
    }

    private void submitUntil(long checkpointId) {
        NavigableMap<Long, List<CompactorRequest>> canSubmit =
                checkpointRequests.subMap(Long.MIN_VALUE, true, checkpointId, true);
        for (Entry<Long, List<CompactorRequest>> requestEntry : canSubmit.entrySet()) {
            for (CompactorRequest request : requestEntry.getValue()) {
                CompletableFuture<Iterable<FileSinkCommittable>> resultFuture =
                        new CompletableFuture<>();
                compactingRequests.add(new Tuple2<>(request, resultFuture));
                compactService.submit(request, resultFuture);
            }
        }
        canSubmit.clear();
    }

    private void emitCompacted(@Nullable Long checkpointId) throws Exception {
        List<FileSinkCommittable> compacted = new ArrayList<>();
        Iterator<Tuple2<CompactorRequest, CompletableFuture<Iterable<FileSinkCommittable>>>> iter =
                compactingRequests.iterator();
        while (iter.hasNext()) {
            Tuple2<CompactorRequest, CompletableFuture<Iterable<FileSinkCommittable>>> compacting =
                    iter.next();
            CompletableFuture<Iterable<FileSinkCommittable>> future = compacting.f1;
            if (future.isDone()) {
                iter.remove();
                // Exception is thrown if it's completed exceptionally
                for (FileSinkCommittable c : future.get()) {
                    compacted.add(c);
                }
            }
        }

        if (compacted.isEmpty()) {
            return;
        }

        // A summary must be sent before all results during this checkpoint
        CommittableSummary<FileSinkCommittable> summary =
                new CommittableSummary<>(
                        getRuntimeContext().getIndexOfThisSubtask(),
                        getRuntimeContext().getNumberOfParallelSubtasks(),
                        checkpointId,
                        compacted.size(),
                        compacted.size(),
                        0);
        output.collect(new StreamRecord<>(summary));
        for (FileSinkCommittable c : compacted) {
            CommittableWithLineage<FileSinkCommittable> comm =
                    new CommittableWithLineage<>(
                            c, checkpointId, getRuntimeContext().getIndexOfThisSubtask());
            output.collect(new StreamRecord<>(comm));
        }
    }

    @VisibleForTesting
    public CompletableFuture<?> getAllTasksFuture() {
        return CompletableFuture.allOf(
                compactingRequests.stream().map(r -> r.f1).toArray(CompletableFuture[]::new));
    }

    static class RemainingRequestsSerializer
            implements SimpleVersionedSerializer<Map<Long, List<CompactorRequest>>> {

        private static final int MAGIC_NUMBER = 0xa946be83;

        private final CompactorRequestSerializer requestSerializer;

        RemainingRequestsSerializer(CompactorRequestSerializer requestSerializer) {
            this.requestSerializer = requestSerializer;
        }

        @Override
        public int getVersion() {
            return 1;
        }

        @Override
        public byte[] serialize(Map<Long, List<CompactorRequest>> remainingRequests)
                throws IOException {
            DataOutputSerializer out = new DataOutputSerializer(256);
            out.writeInt(MAGIC_NUMBER);
            serializeV1(remainingRequests, out);
            return out.getCopyOfBuffer();
        }

        @Override
        public Map<Long, List<CompactorRequest>> deserialize(int version, byte[] serialized)
                throws IOException {
            DataInputDeserializer in = new DataInputDeserializer(serialized);

            switch (version) {
                case 1:
                    validateMagicNumber(in);
                    return deserializeV1(in);
                default:
                    throw new IOException("Unrecognized version or corrupt state: " + version);
            }
        }

        private void serializeV1(
                Map<Long, List<CompactorRequest>> request, DataOutputSerializer out)
                throws IOException {
            out.writeInt(request.size());
            for (Map.Entry<Long, List<CompactorRequest>> e : request.entrySet()) {
                out.writeLong(e.getKey());
                SimpleVersionedSerialization.writeVersionAndSerializeList(
                        requestSerializer, e.getValue(), out);
            }
        }

        private Map<Long, List<CompactorRequest>> deserializeV1(DataInputDeserializer in)
                throws IOException {
            int size = in.readInt();
            Map<Long, List<CompactorRequest>> requestMap = new HashMap<>(size);
            for (int i = 0; i < size; i++) {
                long cpId = in.readLong();
                List<CompactorRequest> requests =
                        SimpleVersionedSerialization.readVersionAndDeserializeList(
                                requestSerializer, in);
                requestMap.put(cpId, requests);
            }
            return requestMap;
        }

        private static void validateMagicNumber(DataInputView in) throws IOException {
            int magicNumber = in.readInt();
            if (magicNumber != MAGIC_NUMBER) {
                throw new IOException(
                        String.format("Corrupt data: Unexpected magic number %08X", magicNumber));
            }
        }
    }
}
