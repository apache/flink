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

import org.apache.flink.api.common.functions.DefaultOpenContext;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.api.TableRuntimeException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.table.functions.TableSemantics;
import org.apache.flink.table.runtime.generated.ProcessTableRunner;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.Optional;

/** Operator for {@link ProcessTableFunction}. */
public class ProcessTableOperator extends AbstractStreamOperator<RowData>
        implements OneInputStreamOperator<RowData, RowData> {

    private final @Nullable RuntimeTableSemantics tableSemantics;
    private final ProcessTableRunner processTableRunner;

    private transient PassThroughCollectorBase collector;

    public ProcessTableOperator(
            StreamOperatorParameters<RowData> parameters,
            @Nullable RuntimeTableSemantics tableSemantics,
            ProcessTableRunner processTableRunner) {
        super(parameters);
        this.tableSemantics = tableSemantics;
        this.processTableRunner = processTableRunner;
    }

    @Override
    public void open() throws Exception {
        super.open();
        if (tableSemantics == null || tableSemantics.passColumnsThrough()) {
            collector = new PassAllCollector(output);
        } else {
            collector = new PassPartitionKeysCollector(output, tableSemantics.partitionByColumns());
        }
        processTableRunner.runnerCollector = this.collector;
        processTableRunner.runnerContext = new ProcessFunctionContext(tableSemantics);

        FunctionUtils.setFunctionRuntimeContext(processTableRunner, getRuntimeContext());
        FunctionUtils.openFunction(processTableRunner, DefaultOpenContext.INSTANCE);
    }

    @Override
    public void processElement(StreamRecord<RowData> element) throws Exception {
        collector.setInput(element.getValue());

        processTableRunner.processElement(0, element.getValue());
    }

    private static class ProcessFunctionContext implements ProcessTableFunction.Context {

        private final Map<String, RuntimeTableSemantics> tableSemanticsMap;

        ProcessFunctionContext(@Nullable RuntimeTableSemantics tableSemantics) {
            this.tableSemanticsMap =
                    Optional.ofNullable(tableSemantics)
                            .map(s -> Map.of(tableSemantics.getArgName(), tableSemantics))
                            .orElse(Map.of());
        }

        @Override
        public TableSemantics tableSemanticsFor(String argName) {
            final RuntimeTableSemantics tableSemantics = tableSemanticsMap.get(argName);
            if (tableSemantics == null) {
                throw new TableRuntimeException("Unknown table argument: " + argName);
            }
            return tableSemantics;
        }
    }
}
