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
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.functions.sink.legacy.SinkFunction;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.OutputTypeConfigurable;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.util.functions.StreamingFunctionUtils;

import static java.util.Objects.requireNonNull;

/**
 * This is used as the base class for operators that have two user-defined function for left and
 * right side. This class handles the opening and closing of the user-defined functions, as part of
 * the operator life cycle.
 *
 * <p>This {@link TableAbstractCoUdfStreamOperator} is similar with {@link
 * AbstractUdfStreamOperator}, and the only differences is {@link TableAbstractCoUdfStreamOperator}
 * can handle two inputs.
 *
 * @param <OUT> The output type of the operator
 * @param <LEFT_FUNC> The type of the triggered user function for left input
 * @param <RIGHT_FUNC> The type of the triggered user function for right input
 */
public abstract class TableAbstractCoUdfStreamOperator<
                OUT, LEFT_FUNC extends Function, RIGHT_FUNC extends Function>
        extends AbstractStreamOperator<OUT> implements OutputTypeConfigurable<OUT> {
    private static final long serialVersionUID = 1L;

    /** The triggered user function for left side. */
    protected final LEFT_FUNC leftTriggeredUserFunction;

    /** The triggered user function for right side. */
    protected final RIGHT_FUNC rightTriggeredUserFunction;

    public TableAbstractCoUdfStreamOperator(
            LEFT_FUNC leftTriggeredUserFunction, RIGHT_FUNC rightTriggeredUserFunction) {
        this.leftTriggeredUserFunction = requireNonNull(leftTriggeredUserFunction);
        this.rightTriggeredUserFunction = requireNonNull(rightTriggeredUserFunction);
        checkUdfCheckpointingPreconditions();
    }

    /**
     * Gets the triggered user function executed in this operator.
     *
     * @return The triggered user function of this operator.
     */
    public LEFT_FUNC getLeftTriggeredUserFunction() {
        return leftTriggeredUserFunction;
    }

    public RIGHT_FUNC getRightTriggeredUserFunction() {
        return rightTriggeredUserFunction;
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
        FunctionUtils.setFunctionRuntimeContext(leftTriggeredUserFunction, getRuntimeContext());
        FunctionUtils.setFunctionRuntimeContext(rightTriggeredUserFunction, getRuntimeContext());
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);
        StreamingFunctionUtils.snapshotFunctionState(
                context, getOperatorStateBackend(), leftTriggeredUserFunction);
        StreamingFunctionUtils.snapshotFunctionState(
                context, getOperatorStateBackend(), rightTriggeredUserFunction);
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
        StreamingFunctionUtils.restoreFunctionState(context, leftTriggeredUserFunction);
        StreamingFunctionUtils.restoreFunctionState(context, rightTriggeredUserFunction);
    }

    @Override
    public void open() throws Exception {
        super.open();
        super.open();
        FunctionUtils.openFunction(leftTriggeredUserFunction, DefaultOpenContext.INSTANCE);
        FunctionUtils.openFunction(rightTriggeredUserFunction, DefaultOpenContext.INSTANCE);
    }

    @Override
    public void finish() throws Exception {
        super.finish();
        if (leftTriggeredUserFunction instanceof SinkFunction) {
            ((SinkFunction<?>) leftTriggeredUserFunction).finish();
        }
        if (rightTriggeredUserFunction instanceof SinkFunction) {
            ((SinkFunction<?>) rightTriggeredUserFunction).finish();
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        FunctionUtils.closeFunction(leftTriggeredUserFunction);
        FunctionUtils.closeFunction(rightTriggeredUserFunction);
    }

    // ------------------------------------------------------------------------
    //  checkpointing and recovery
    // ------------------------------------------------------------------------

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        super.notifyCheckpointComplete(checkpointId);

        if (leftTriggeredUserFunction instanceof CheckpointListener) {
            ((CheckpointListener) leftTriggeredUserFunction).notifyCheckpointComplete(checkpointId);
        }
        if (rightTriggeredUserFunction instanceof CheckpointListener) {
            ((CheckpointListener) rightTriggeredUserFunction)
                    .notifyCheckpointComplete(checkpointId);
        }
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        super.notifyCheckpointAborted(checkpointId);

        if (leftTriggeredUserFunction instanceof CheckpointListener) {
            ((CheckpointListener) leftTriggeredUserFunction).notifyCheckpointAborted(checkpointId);
        }
        if (rightTriggeredUserFunction instanceof CheckpointListener) {
            ((CheckpointListener) rightTriggeredUserFunction).notifyCheckpointAborted(checkpointId);
        }
    }

    // ------------------------------------------------------------------------
    //  Output type configuration
    // ------------------------------------------------------------------------

    @Override
    public void setOutputType(TypeInformation<OUT> outTypeInfo, ExecutionConfig executionConfig) {
        StreamingFunctionUtils.setOutputType(
                leftTriggeredUserFunction, outTypeInfo, executionConfig);
        StreamingFunctionUtils.setOutputType(
                rightTriggeredUserFunction, outTypeInfo, executionConfig);
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    private void checkUdfCheckpointingPreconditions() {
        if (leftTriggeredUserFunction instanceof CheckpointedFunction
                && leftTriggeredUserFunction instanceof ListCheckpointed) {

            throw new IllegalStateException(
                    "Triggered user functions are not allowed to implement "
                            + "CheckpointedFunction AND ListCheckpointed.");
        }

        if (rightTriggeredUserFunction instanceof CheckpointedFunction
                && rightTriggeredUserFunction instanceof ListCheckpointed) {

            throw new IllegalStateException(
                    "Triggered user functions are not allowed to implement "
                            + "CheckpointedFunction AND ListCheckpointed.");
        }
    }
}
