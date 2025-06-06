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

package org.apache.flink.table.runtime.generated;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.table.runtime.operators.process.AbstractProcessTableOperator;
import org.apache.flink.table.runtime.operators.process.AbstractProcessTableOperator.RunnerContext;
import org.apache.flink.table.runtime.operators.process.AbstractProcessTableOperator.RunnerOnTimerContext;
import org.apache.flink.table.runtime.operators.process.PassAllCollector;
import org.apache.flink.table.runtime.operators.process.PassThroughCollectorBase;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.function.RunnableWithException;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Arrays;

/**
 * Abstraction of code-generated calls to {@link ProcessTableFunction} to be used within {@link
 * AbstractProcessTableOperator}.
 */
@Internal
public abstract class ProcessTableRunner extends AbstractRichFunction {

    // Constant references after initialization
    protected State[] stateHandles;
    private HashFunction[] stateHashCode;
    private RecordEqualiser[] stateEquals;
    private boolean emitRowtime;

    // Contexts
    protected RunnerContext runnerContext;
    protected RunnerOnTimerContext runnerOnTimerContext;

    // Collectors
    protected PassThroughCollectorBase evalCollector;
    protected PassAllCollector onTimerCollector;

    // Current input table
    protected int inputIndex = -1;
    protected RowData inputRow;

    // Current time
    private long currentWatermark = Long.MIN_VALUE;
    private @Nullable Long rowtime;
    private @Nullable StringData timerName;

    /** State entries to be converted into external data structure; null if state is empty. */
    protected RowData[] valueStateToFunction;

    /**
     * Reference to whether the state has been cleared within the function; if yes, a conversion
     * from external to internal data structure is not necessary anymore.
     */
    protected boolean[] stateCleared;

    /** State ready for persistence; null if {@link #stateCleared} was true during conversion. */
    protected RowData[] valueStateFromFunction;

    public void initialize(
            State[] stateHandles,
            HashFunction[] stateHashCode,
            RecordEqualiser[] stateEquals,
            boolean emitRowtime,
            RunnerContext runnerContext,
            RunnerOnTimerContext runnerOnTimerContext,
            PassThroughCollectorBase evalCollector,
            PassAllCollector onTimerCollector) {
        this.stateHandles = stateHandles;
        this.stateHashCode = stateHashCode;
        this.stateEquals = stateEquals;
        this.emitRowtime = emitRowtime;

        // Accessed by generated code
        this.runnerContext = runnerContext;
        this.runnerOnTimerContext = runnerOnTimerContext;
        this.evalCollector = evalCollector;
        this.onTimerCollector = onTimerCollector;
        this.valueStateToFunction = new RowData[stateHandles.length];
        this.stateCleared = new boolean[stateHandles.length];
        this.valueStateFromFunction = new RowData[stateHandles.length];
    }

    public void ingestTableEvent(int pos, RowData row, int timeColumn) {
        evalCollector.setPrefix(pos, row);
        if (timeColumn == -1) {
            rowtime = null;
        } else {
            final long inputTime = row.getTimestamp(timeColumn, 3).getMillisecond();
            if (emitRowtime) {
                evalCollector.setRowtime(inputTime);
            }
            rowtime = inputTime;
        }
        inputIndex = pos;
        inputRow = row;
    }

    public void ingestTimerEvent(RowData key, @Nullable StringData name, long timerTime) {
        onTimerCollector.setPrefix(-1, key);
        if (emitRowtime) {
            onTimerCollector.setRowtime(timerTime);
        }
        rowtime = timerTime;
        timerName = name;
    }

    public void ingestWatermarkEvent(long watermarkTime) {
        currentWatermark = watermarkTime;
    }

    public void clearAllState() {
        Arrays.fill(stateCleared, true);
    }

    public void clearState(int statePos) {
        stateCleared[statePos] = true;
    }

    public long getCurrentWatermark() {
        return currentWatermark;
    }

    public @Nullable Long getTime() {
        return rowtime;
    }

    public @Nullable StringData getTimerName() {
        return timerName;
    }

    public void processEval() throws Exception {
        // Drop late events
        if (rowtime != null && rowtime <= currentWatermark) {
            return;
        }
        processMethod(this::callEval);
    }

    public void processOnTimer() throws Exception {
        processMethod(this::callOnTimer);
    }

    public abstract void callEval() throws Exception;

    public abstract void callOnTimer() throws Exception;

    private void processMethod(RunnableWithException method) throws Exception {
        if (stateHandles.length > 0) {
            // For each function call:
            // - the state is read from Flink
            // - converted into external data structure
            // - evaluated
            // - converted into internal data structure (if not cleared)
            // - the state is written into Flink
            moveStateToFunction();
            method.run();
            moveStateFromFunction();
        } else {
            method.run();
        }
    }

    @SuppressWarnings("unchecked")
    private void moveStateToFunction() throws IOException {
        Arrays.fill(stateCleared, false);
        for (int i = 0; i < stateHandles.length; i++) {
            final State stateHandle = stateHandles[i];
            if (!(stateHandle instanceof ValueState)) {
                continue;
            }
            final ValueState<RowData> valueState = (ValueState<RowData>) stateHandle;
            final RowData value = valueState.value();
            valueStateToFunction[i] = value;
        }
    }

    @SuppressWarnings("unchecked")
    private void moveStateFromFunction() throws IOException {
        for (int i = 0; i < stateHandles.length; i++) {
            final State stateHandle = stateHandles[i];
            if (stateHandle instanceof ValueState) {
                moveValueStateFromFunction((ValueState<RowData>) stateHandle, i);
            } else {
                if (stateCleared[i]) {
                    stateHandle.clear();
                }
            }
        }
    }

    private void moveValueStateFromFunction(ValueState<RowData> valueState, int pos)
            throws IOException {
        final RowData fromFunction = valueStateFromFunction[pos];
        if (fromFunction == null || isEmpty(fromFunction)) {
            valueState.clear();
        } else {
            final HashFunction hashCode = stateHashCode[pos];
            final RecordEqualiser equals = stateEquals[pos];
            final RowData toFunction = valueStateToFunction[pos];
            // Reduce state updates by checking if something has changed
            if (toFunction == null
                    || hashCode.hashCode(toFunction) != hashCode.hashCode(fromFunction)
                    || !equals.equals(toFunction, fromFunction)) {
                valueState.update(fromFunction);
            }
        }
    }

    private static boolean isEmpty(RowData row) {
        for (int i = 0; i < row.getArity(); i++) {
            if (!row.isNullAt(i)) {
                return false;
            }
        }
        return row.getRowKind() == RowKind.INSERT;
    }
}
