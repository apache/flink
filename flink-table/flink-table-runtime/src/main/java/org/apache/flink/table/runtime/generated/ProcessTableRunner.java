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
import org.apache.flink.table.runtime.generated.ProcessTableRunner.StateHandle.Kind;
import org.apache.flink.table.runtime.operators.process.AbstractProcessTableOperator;
import org.apache.flink.table.runtime.operators.process.AbstractProcessTableOperator.RunnerContext;
import org.apache.flink.table.runtime.operators.process.AbstractProcessTableOperator.RunnerOnTimerContext;
import org.apache.flink.table.runtime.operators.process.PassAllCollector;
import org.apache.flink.table.runtime.operators.process.PassThroughCollectorBase;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.function.RunnableWithException;

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * Abstraction of code-generated calls to {@link ProcessTableFunction} to be used within {@link
 * AbstractProcessTableOperator}.
 */
@Internal
public abstract class ProcessTableRunner extends AbstractRichFunction {

    // Constant references after initialization.
    // Accessed by generated code.
    protected StateHandle[] stateHandles;
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
    private long tableWatermark = Long.MIN_VALUE;
    private @Nullable Long rowtime;
    private @Nullable StringData timerName;

    public void initialize(
            StateHandle[] stateHandles,
            boolean emitRowtime,
            RunnerContext runnerContext,
            RunnerOnTimerContext runnerOnTimerContext,
            PassThroughCollectorBase evalCollector,
            PassAllCollector onTimerCollector) {
        this.stateHandles = stateHandles;
        this.emitRowtime = emitRowtime;

        // Accessed by generated code
        this.runnerContext = runnerContext;
        this.runnerOnTimerContext = runnerOnTimerContext;
        this.evalCollector = evalCollector;
        this.onTimerCollector = onTimerCollector;
    }

    public void ingestTableEvent(int pos, RowData row, int timeColumn, long watermark) {
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
        tableWatermark = watermark;
    }

    public void ingestTimerEvent(RowData key, @Nullable StringData name, long timerTime) {
        onTimerCollector.setPrefix(-1, key);
        if (emitRowtime) {
            onTimerCollector.setRowtime(timerTime);
        }
        rowtime = timerTime;
        timerName = name;
        tableWatermark = Long.MIN_VALUE;
    }

    public void clearAllState() {
        for (StateHandle stateHandle : stateHandles) {
            stateHandle.cleared = true;
        }
    }

    public void clearState(int statePos) {
        stateHandles[statePos].cleared = true;
    }

    public long getTableWatermark() {
        return tableWatermark;
    }

    public @Nullable Long getTime() {
        return rowtime;
    }

    public @Nullable StringData getTimerName() {
        return timerName;
    }

    public void processEval() throws Exception {
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
            // - eager value state is read from Flink and converted into external data structure
            // - views are cheap to set up (lazy access to Flink state)
            // - evaluated
            // - eager value state is converted into internal data structure (if not cleared)
            //   and written into Flink
            // - views may only need to be cleared if requested
            moveStateToFunction();
            method.run();
            moveStateFromFunction();
        } else {
            method.run();
        }
    }

    @SuppressWarnings("unchecked")
    private void moveStateToFunction() throws IOException {
        for (StateHandle stateHandle : stateHandles) {
            stateHandle.cleared = false;
            if (stateHandle.kind != Kind.EAGER_VALUE) {
                // Views access Flink state lazily; nothing to move eagerly.
                continue;
            }
            final ValueState<RowData> valueState = (ValueState<RowData>) stateHandle.state;
            stateHandle.toFunction = valueState.value();
        }
    }

    @SuppressWarnings("unchecked")
    private void moveStateFromFunction() throws IOException {
        for (StateHandle stateHandle : stateHandles) {
            if (stateHandle.kind == Kind.EAGER_VALUE) {
                moveValueStateFromFunction(stateHandle);
            } else if (stateHandle.cleared) {
                stateHandle.state.clear();
            }
        }
    }

    private void moveValueStateFromFunction(StateHandle stateHandle) throws IOException {
        @SuppressWarnings("unchecked")
        final ValueState<RowData> valueState = (ValueState<RowData>) stateHandle.state;
        final RowData fromFunction = stateHandle.fromFunction;
        if (fromFunction == null || isEmpty(fromFunction)) {
            valueState.clear();
        } else {
            final HashFunction hashCode = stateHandle.hashFunction;
            final RecordEqualiser equals = stateHandle.equaliser;
            final RowData toFunction = stateHandle.toFunction;
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

    // --------------------------------------------------------------------------------------------

    /**
     * A bundle of everything the runner needs to handle a single state entry.
     *
     * <p>The {@link Kind} determines how the entry is moved to/from the function. For all view
     * kinds ({@link Kind#MAP_VIEW}, {@link Kind#LIST_VIEW}, {@link Kind#VALUE_VIEW}) the view
     * accesses Flink state lazily, so {@link #hashFunction} and {@link #equaliser} are {@code null}
     * and the scratch fields are unused. Only {@link Kind#EAGER_VALUE} follows a Read-Modify-Write
     * cycle and uses the hash function, equaliser, and per-invocation scratch fields.
     */
    @Internal
    public static final class StateHandle {

        /** Kind of a state entry. */
        public enum Kind {
            MAP_VIEW,
            LIST_VIEW,
            VALUE_VIEW,
            EAGER_VALUE
        }

        private final Kind kind;
        private final State state;
        private final @Nullable HashFunction hashFunction;
        private final @Nullable RecordEqualiser equaliser;

        // Per-invocation scratch, only used for EAGER_VALUE.

        /** State entry to be converted into external data structure; null if state is empty. */
        private @Nullable RowData toFunction;

        /** State ready for persistence; null if {@link #cleared} was true during conversion. */
        private @Nullable RowData fromFunction;

        /**
         * Whether the state has been cleared within the function; if yes, a conversion from
         * external to internal data structure is not necessary anymore.
         */
        private boolean cleared;

        public StateHandle(
                Kind kind,
                State state,
                @Nullable HashFunction hashFunction,
                @Nullable RecordEqualiser equaliser) {
            this.kind = kind;
            this.state = state;
            this.hashFunction = hashFunction;
            this.equaliser = equaliser;
        }

        public Kind getKind() {
            return kind;
        }

        public State getState() {
            return state;
        }

        public @Nullable RowData getToFunction() {
            return toFunction;
        }

        public void setFromFunction(@Nullable RowData fromFunction) {
            this.fromFunction = fromFunction;
        }

        public boolean isCleared() {
            return cleared;
        }
    }
}
