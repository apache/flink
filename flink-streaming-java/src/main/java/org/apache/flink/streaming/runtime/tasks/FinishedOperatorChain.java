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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.io.network.api.writer.RecordWriterDelegate;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.TaskStateManager;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFutures;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamTaskStateInitializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.SerializedValue;

import java.util.Map;
import java.util.function.Supplier;

/**
 * The {@link OperatorChain} that is used for restoring tasks that are {@link
 * TaskStateManager#isTaskDeployedAsFinished()}.
 */
@Internal
public class FinishedOperatorChain<OUT, OP extends StreamOperator<OUT>>
        extends OperatorChain<OUT, OP> {

    public FinishedOperatorChain(
            StreamTask<OUT, OP> containingTask,
            RecordWriterDelegate<SerializationDelegate<StreamRecord<OUT>>> recordWriterDelegate) {
        super(containingTask, recordWriterDelegate);
    }

    @Override
    public boolean isTaskDeployedAsFinished() {
        return true;
    }

    @Override
    public WatermarkGaugeExposingOutput<StreamRecord<OUT>> getMainOperatorOutput() {
        return new FinishedOnRestoreMainOperatorOutput<>(streamOutputs);
    }

    @Override
    public void dispatchOperatorEvent(OperatorID operator, SerializedValue<OperatorEvent> event) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) {}

    @Override
    public void endInput(int inputId) throws Exception {}

    @Override
    public void initializeStateAndOpenOperators(
            StreamTaskStateInitializer streamTaskStateInitializer) {}

    @Override
    public void finishOperators(StreamTaskActionExecutor actionExecutor) throws Exception {}

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {}

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {}

    @Override
    public boolean isClosed() {
        return isClosed;
    }

    @Override
    public void snapshotState(
            Map<OperatorID, OperatorSnapshotFutures> operatorSnapshotsInProgress,
            CheckpointMetaData checkpointMetaData,
            CheckpointOptions checkpointOptions,
            Supplier<Boolean> isRunning,
            ChannelStateWriter.ChannelStateWriteResult channelStateWriteResult,
            CheckpointStreamFactory storage)
            throws Exception {}
}
