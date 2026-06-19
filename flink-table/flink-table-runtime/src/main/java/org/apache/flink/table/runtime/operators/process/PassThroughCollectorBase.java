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
import org.apache.flink.table.api.TableRuntimeException;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.runtime.util.StreamRecordCollector;
import org.apache.flink.types.RowKind;

/** Base class for collectors that pass input columns. */
@Internal
public abstract class PassThroughCollectorBase extends StreamRecordCollector<RowData> {

    private final RepeatedRowData repeatedPrefix;
    private final JoinedRowData withFunctionOutput;
    private final JoinedRowData withRowtime;
    private final ChangelogMode changelogMode;

    protected RowData prefix;

    private RowData rowtime;

    public PassThroughCollectorBase(
            Output<StreamRecord<RowData>> output,
            ChangelogMode changelogMode,
            int prefixRepetition) {
        super(output);
        this.changelogMode = changelogMode;
        // constructs a flattened row of [[[prefix]{1,n} | function output] | rowtime]
        repeatedPrefix = new RepeatedRowData(prefixRepetition);
        withFunctionOutput = new JoinedRowData();
        withRowtime = new JoinedRowData();
        prefix = GenericRowData.of();
        rowtime = GenericRowData.of();
    }

    public abstract void setPrefix(int pos, RowData input);

    public void setRowtime(Long time) {
        rowtime = GenericRowData.of(TimestampData.fromEpochMillis(time));
    }

    @Override
    public void collect(RowData functionOutput) {
        repeatedPrefix.replace(prefix);
        withFunctionOutput.replace(repeatedPrefix, functionOutput);
        withRowtime.replace(withFunctionOutput, rowtime);
        // Forward supported change flags.
        final RowKind kind = functionOutput.getRowKind();
        if (!changelogMode.contains(kind)) {
            throw new TableRuntimeException(
                    String.format(
                            "Invalid row kind received: %s. "
                                    + "Expected produced changelog mode: %s",
                            kind, changelogMode));
        }
        withRowtime.setRowKind(kind);
        super.collect(withRowtime);
    }
}
