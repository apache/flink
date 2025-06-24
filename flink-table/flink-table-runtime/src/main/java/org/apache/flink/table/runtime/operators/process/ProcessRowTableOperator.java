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

import org.apache.flink.streaming.api.operators.MultipleInputStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.table.functions.TableSemantics;
import org.apache.flink.table.runtime.generated.HashFunction;
import org.apache.flink.table.runtime.generated.ProcessTableRunner;
import org.apache.flink.table.runtime.generated.RecordEqualiser;

import javax.annotation.Nullable;

import java.util.List;

/**
 * Implementation of {@link OneInputStreamOperator} for {@link ProcessTableFunction} with at most
 * one table with row semantics.
 *
 * <p>This class is required because {@link MultipleInputStreamOperator} has issues with chaining
 * when the transformation is not keyed.
 */
public class ProcessRowTableOperator extends AbstractProcessTableOperator
        implements OneInputStreamOperator<RowData, RowData> {

    private final @Nullable TableSemantics inputSemantics;

    public ProcessRowTableOperator(
            StreamOperatorParameters<RowData> parameters,
            List<RuntimeTableSemantics> tableSemantics,
            List<RuntimeStateInfo> stateInfos,
            ProcessTableRunner processTableRunner,
            HashFunction[] stateHashCode,
            RecordEqualiser[] stateEquals,
            RuntimeChangelogMode producedChangelogMode) {
        super(
                parameters,
                tableSemantics,
                stateInfos,
                processTableRunner,
                stateHashCode,
                stateEquals,
                producedChangelogMode);
        if (tableSemantics.isEmpty()) {
            inputSemantics = null;
        } else {
            inputSemantics = tableSemantics.get(0);
        }
    }

    @Override
    public void setKeyContextElement1(StreamRecord<?> record) {
        // not applicable
    }

    @Override
    public void processElement(StreamRecord<RowData> element) throws Exception {
        if (inputSemantics != null) {
            processTableRunner.ingestTableEvent(0, element.getValue(), inputSemantics.timeColumn());
        }
        processTableRunner.processEval();
    }

    @Override
    public void processWatermarkStatus(WatermarkStatus watermarkStatus) throws Exception {
        super.processWatermarkStatus(watermarkStatus, 1);
    }

    @Override
    public void processLatencyMarker(LatencyMarker latencyMarker) throws Exception {
        super.reportOrForwardLatencyMarker(latencyMarker);
    }
}
