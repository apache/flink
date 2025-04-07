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
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.utils.JoinedRowData;
import org.apache.flink.table.runtime.util.StreamRecordCollector;

/** Base class for collectors that pass input columns. */
@Internal
public abstract class PassThroughCollectorBase extends StreamRecordCollector<RowData> {

    private final JoinedRowData withPrefix;
    private final JoinedRowData withRowtime;

    private RowData rowtime;
    protected RowData prefix;

    public PassThroughCollectorBase(Output<StreamRecord<RowData>> output) {
        super(output);
        // constructs a flattened row of [[prefix | function output] | rowtime]
        withPrefix = new JoinedRowData();
        withRowtime = new JoinedRowData();
        prefix = GenericRowData.of();
        rowtime = GenericRowData.of();
    }

    public abstract void setPrefix(RowData input);

    public void setRowtime(Long time) {
        rowtime = GenericRowData.of(TimestampData.fromEpochMillis(time));
    }

    @Override
    public void collect(RowData functionOutput) {
        withPrefix.replace(prefix, functionOutput);
        withRowtime.replace(withPrefix, rowtime);
        super.collect(withRowtime);
    }
}
