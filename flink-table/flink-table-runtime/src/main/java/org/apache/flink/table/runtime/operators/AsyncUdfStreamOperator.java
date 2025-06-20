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

package org.apache.flink.table.runtime.operators;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.DefaultOpenContext;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.asyncprocessing.AsyncExecutionController;
import org.apache.flink.runtime.asyncprocessing.SimpleAsyncExecutionController;
import org.apache.flink.runtime.asyncprocessing.operators.AbstractAsyncRunnableStreamOperator;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.functions.sink.legacy.SinkFunction;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.OutputTypeConfigurable;
import org.apache.flink.streaming.api.operators.UserFunctionProvider;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.util.functions.StreamingFunctionUtils;

import java.util.concurrent.ExecutorService;

import static java.util.Objects.requireNonNull;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Operator contains udf and also has the ability of async processing with key ordered. */
public class AsyncUdfStreamOperator<OUT, F extends Function>
        extends AbstractAsyncRunnableStreamOperator<OUT>
        implements OutputTypeConfigurable<OUT>, UserFunctionProvider<F> {

    private static final long serialVersionUID = 1L;

    /** The user function. */
    protected final F userFunction;

    public AsyncUdfStreamOperator(
            F userFunction,
            KeySelector<?, ?> keySelector1,
            KeySelector<?, ?> keySelector2,
            ExecutorService asyncThreadPool,
            int asyncBufferSize,
            long asyncBufferTimeout,
            int inFlightRecordsLimit) {
        super(
                keySelector1,
                keySelector2,
                asyncThreadPool,
                asyncBufferSize,
                asyncBufferTimeout,
                inFlightRecordsLimit);
        this.userFunction = requireNonNull(userFunction);
        checkUdfCheckpointingPreconditions();
    }

    @Override
    protected AsyncExecutionController createAsyncExecutionController() {
        if (isAsyncKeyOrderedProcessingEnabled()) {
            final StreamTask<?, ?> containingTask = checkNotNull(getContainingTask());
            final MailboxExecutor mailboxExecutor =
                    containingTask
                            .getMailboxExecutorFactory()
                            .createExecutor(getOperatorConfig().getChainIndex());
            final int maxParallelism = environment.getTaskInfo().getMaxNumberOfParallelSubtasks();

            return new SimpleAsyncExecutionController(
                    mailboxExecutor,
                    this::handleAsyncException,
                    asyncThreadPool,
                    getDeclarationManager(),
                    getEpochParallelMode(),
                    maxParallelism,
                    asyncBufferSize,
                    asyncBufferTimeout,
                    inFlightRecordsLimit,
                    null,
                    getMetricGroup().addGroup("tableAsyncProcessing"));
        }
        return null;
    }

    /**
     * Gets the user function executed in this operator.
     *
     * @return The user function of this operator.
     */
    public F getUserFunction() {
        return userFunction;
    }

    // ------------------------------------------------------------------------
    //  operator life cycle
    // ------------------------------------------------------------------------

    @Override
    public void setup(
            StreamTask<?, ?> containingTask,
            StreamConfig config,
            Output<StreamRecord<OUT>> output) {
        super.setup(containingTask, config, output);
        FunctionUtils.setFunctionRuntimeContext(userFunction, getRuntimeContext());
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);
        StreamingFunctionUtils.snapshotFunctionState(
                context, getOperatorStateBackend(), userFunction);
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
        StreamingFunctionUtils.restoreFunctionState(context, userFunction);
    }

    @Override
    public void open() throws Exception {
        super.open();
        FunctionUtils.openFunction(userFunction, DefaultOpenContext.INSTANCE);
    }

    @Override
    public void finish() throws Exception {
        super.finish();
        if (userFunction instanceof SinkFunction) {
            ((SinkFunction<?>) userFunction).finish();
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        FunctionUtils.closeFunction(userFunction);
    }

    // ------------------------------------------------------------------------
    //  checkpointing and recovery
    // ------------------------------------------------------------------------

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        super.notifyCheckpointComplete(checkpointId);

        if (userFunction instanceof CheckpointListener) {
            ((CheckpointListener) userFunction).notifyCheckpointComplete(checkpointId);
        }
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        super.notifyCheckpointAborted(checkpointId);

        if (userFunction instanceof CheckpointListener) {
            ((CheckpointListener) userFunction).notifyCheckpointAborted(checkpointId);
        }
    }

    // ------------------------------------------------------------------------
    //  Output type configuration
    // ------------------------------------------------------------------------

    @Override
    public void setOutputType(TypeInformation<OUT> outTypeInfo, ExecutionConfig executionConfig) {
        StreamingFunctionUtils.setOutputType(userFunction, outTypeInfo, executionConfig);
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    private void checkUdfCheckpointingPreconditions() {

        if (userFunction instanceof CheckpointedFunction
                && userFunction instanceof ListCheckpointed) {

            throw new IllegalStateException(
                    "User functions are not allowed to implement "
                            + "CheckpointedFunction AND ListCheckpointed.");
        }
    }
}
