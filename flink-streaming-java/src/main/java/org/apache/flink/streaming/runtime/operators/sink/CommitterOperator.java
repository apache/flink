/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.operators.sink;

import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;

import java.util.Collections;

import static org.apache.flink.util.IOUtils.closeAll;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An operator that processes committables of a {@link org.apache.flink.api.connector.sink.Sink}.
 *
 * <p>The operator may be part of a sink pipeline but usually is the last operator. There are
 * currently two ways this operator is used:
 *
 * <ul>
 *   <li>In streaming mode, there is a {@link SinkOperator} with parallelism p containing {@link
 *       org.apache.flink.api.connector.sink.SinkWriter} and {@link
 *       org.apache.flink.api.connector.sink.Committer} and this operator containing the {@link
 *       org.apache.flink.api.connector.sink.GlobalCommitter} with parallelism 1.
 *   <li>In batch mode, there is a {@link SinkOperator} with parallelism p containing {@link
 *       org.apache.flink.api.connector.sink.SinkWriter} and this operator containing the {@link
 *       org.apache.flink.api.connector.sink.Committer} and {@link
 *       org.apache.flink.api.connector.sink.GlobalCommitter} with parallelism 1.
 * </ul>
 *
 * @param <InputT> the type of the committable
 * @param <OutputT> the type of the committable to send to downstream operators
 */
class CommitterOperator<InputT, OutputT> extends AbstractStreamOperator<byte[]>
        implements OneInputStreamOperator<byte[], byte[]>, BoundedOneInput {

    private final SimpleVersionedSerializer<InputT> inputSerializer;
    private final CommitterHandler<InputT, OutputT> committerHandler;
    private final CommitRetrier commitRetrier;

    public CommitterOperator(
            ProcessingTimeService processingTimeService,
            SimpleVersionedSerializer<InputT> inputSerializer,
            CommitterHandler<InputT, OutputT> committerHandler) {
        this.inputSerializer = checkNotNull(inputSerializer);
        this.committerHandler = checkNotNull(committerHandler);
        this.processingTimeService = processingTimeService;
        this.commitRetrier = new CommitRetrier(processingTimeService, committerHandler);
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
        committerHandler.initializeState(context);
        // try to re-commit recovered transactions as quickly as possible
        commitRetrier.retryWithDelay();
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);
        committerHandler.snapshotState(context);
    }

    @Override
    public void endInput() throws Exception {
        committerHandler.endOfInput();
        commitRetrier.retryIndefinitely();
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        super.notifyCheckpointComplete(checkpointId);
        committerHandler.notifyCheckpointCompleted(checkpointId);
        commitRetrier.retryWithDelay();
    }

    @Override
    public void processElement(StreamRecord<byte[]> element) throws Exception {
        committerHandler.processCommittables(
                Collections.singletonList(
                        SimpleVersionedSerialization.readVersionAndDeSerialize(
                                inputSerializer, element.getValue())));
    }

    @Override
    public void close() throws Exception {
        closeAll(committerHandler, super::close);
    }
}
