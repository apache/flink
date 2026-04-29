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

package org.apache.flink.table.runtime.operators.process;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.utils.ProjectedRowData;
import org.apache.flink.table.types.inference.PassThroughMode;

import java.util.List;

/**
 * Collector that dispatches per-arg according to each table argument's {@link PassThroughMode}.
 *
 * <p>For each emission triggered from input arg {@code pos}, the prefix slot is filled according to
 * {@code modes[pos]}:
 *
 * <ul>
 *   <li>{@link PassThroughMode#KEY KEY} - the arg's projected partition keys.
 *   <li>{@link PassThroughMode#ALL ALL} - the full input row (single-arg PTFs only).
 *   <li>{@link PassThroughMode#NONE NONE} - empty; the function emits the complete output and the
 *       prefix and rowtime joins are skipped entirely.
 * </ul>
 *
 * <p>The rowtime suffix is a single output column shared across all emission paths and therefore a
 * function-wide decision: it is appended only when no arg uses {@code NONE}. This is computed once
 * at construction as {@code appendsRowtime}.
 */
@Internal
public class PassThroughCollector extends PassThroughCollectorBase {

    private static final RowData EMPTY_PREFIX = GenericRowData.of();

    private final PassThroughMode[] modes;
    private final ProjectedRowData[] keyProjections;
    private final boolean appendsRowtime;

    private PassThroughMode currentMode = PassThroughMode.KEY;

    public PassThroughCollector(
            final Output<StreamRecord<RowData>> output,
            final ChangelogMode changelogMode,
            final List<RuntimeTableSemantics> tableSemantics) {
        // PTFs with only scalar args have an empty tableSemantics list. RepeatedRowData requires
        // a positive repetition count, so we fall back to 1 for that degenerate case.
        super(output, changelogMode, Math.max(tableSemantics.size(), 1));
        this.modes = new PassThroughMode[tableSemantics.size()];
        this.keyProjections = new ProjectedRowData[tableSemantics.size()];
        boolean anyNone = false;
        for (int pos = 0; pos < tableSemantics.size(); pos++) {
            final RuntimeTableSemantics arg = tableSemantics.get(pos);
            modes[pos] = arg.passThroughMode();
            if (modes[pos] == PassThroughMode.KEY) {
                keyProjections[pos] = ProjectedRowData.from(arg.partitionByColumns());
            }
            anyNone |= modes[pos] == PassThroughMode.NONE;
        }
        this.appendsRowtime = !anyNone;
    }

    @Override
    public void setPrefix(final int pos, final RowData input) {
        if (pos < 0) {
            // Timer event: there is no input row, only the keyed-state key. Use the key as the
            // full prefix unless any arg uses NONE (function owns the schema even for timers),
            // in which case the framework contributes nothing. We reuse appendsRowtime as the
            // flag here because both decisions share the same condition (no arg has NONE).
            currentMode = appendsRowtime ? PassThroughMode.ALL : PassThroughMode.NONE;
            prefix = appendsRowtime ? input : EMPTY_PREFIX;
            return;
        }
        currentMode = modes[pos];
        switch (currentMode) {
            case KEY:
                prefix = keyProjections[pos].replaceRow(input);
                break;
            case ALL:
                prefix = input;
                break;
            case NONE:
                prefix = EMPTY_PREFIX;
                break;
        }
    }

    @Override
    public void setRowtime(final Long time) {
        // Rowtime is whole-PTF: only attached if every arg permits a system suffix.
        if (appendsRowtime) {
            super.setRowtime(time);
        }
    }

    @Override
    public void collect(final RowData functionOutput) {
        if (currentMode == PassThroughMode.NONE) {
            // Function owns its full output: skip the prefix + rowtime join.
            emit(functionOutput, functionOutput.getRowKind());
            return;
        }
        super.collect(functionOutput);
    }
}
