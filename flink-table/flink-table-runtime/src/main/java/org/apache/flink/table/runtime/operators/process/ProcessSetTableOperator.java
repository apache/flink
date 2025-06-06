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

import org.apache.flink.streaming.api.operators.AbstractInput;
import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.api.operators.MultipleInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.table.functions.TableSemantics;
import org.apache.flink.table.runtime.generated.HashFunction;
import org.apache.flink.table.runtime.generated.ProcessTableRunner;
import org.apache.flink.table.runtime.generated.RecordEqualiser;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Implementation of {@link MultipleInputStreamOperator} for {@link ProcessTableFunction} with at
 * least one table with set semantics.
 */
public class ProcessSetTableOperator extends AbstractProcessTableOperator
        implements MultipleInputStreamOperator<RowData> {

    public ProcessSetTableOperator(
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
    }

    @Override
    @SuppressWarnings("rawtypes")
    public List<Input> getInputs() {
        return IntStream.range(0, tableSemantics.size())
                .mapToObj(
                        inputIdx -> {
                            final TableSemantics inputSemantics = tableSemantics.get(inputIdx);
                            final int timeColumn = inputSemantics.timeColumn();
                            return new AbstractInput<RowData, RowData>(this, inputIdx + 1) {
                                @Override
                                public void processElement(StreamRecord<RowData> element)
                                        throws Exception {
                                    processTableRunner.ingestTableEvent(
                                            inputIdx, element.getValue(), timeColumn);
                                    processTableRunner.processEval();
                                }
                            };
                        })
                .collect(Collectors.toList());
    }
}
