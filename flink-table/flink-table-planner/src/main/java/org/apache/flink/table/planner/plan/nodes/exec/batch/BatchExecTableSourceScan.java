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

package org.apache.flink.table.planner.plan.nodes.exec.batch;

import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.InputFormatSourceFunction;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecTableSourceScan;
import org.apache.flink.table.planner.plan.nodes.exec.spec.DynamicTableSourceSpec;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Preconditions;

import java.util.Collections;
import java.util.UUID;

/**
 * Batch {@link ExecNode} to read data from an external source defined by a bounded {@link
 * ScanTableSource}.
 */
public class BatchExecTableSourceScan extends CommonExecTableSourceScan
        implements BatchExecNode<RowData> {

    // Avoids creating different ids if translated multiple times
    private final String dynamicFilteringDataListenerID = UUID.randomUUID().toString();
    private final ReadableConfig tableConfig;

    // This constructor can be used only when table source scan has
    // BatchExecDynamicFilteringDataCollector input
    public BatchExecTableSourceScan(
            ReadableConfig tableConfig,
            DynamicTableSourceSpec tableSourceSpec,
            InputProperty inputProperty,
            RowType outputType,
            String description) {
        super(
                ExecNodeContext.newNodeId(),
                ExecNodeContext.newContext(BatchExecTableSourceScan.class),
                ExecNodeContext.newPersistedConfig(BatchExecTableSourceScan.class, tableConfig),
                tableSourceSpec,
                Collections.singletonList(inputProperty),
                outputType,
                description);
        this.tableConfig = tableConfig;
    }

    public BatchExecTableSourceScan(
            ReadableConfig tableConfig,
            DynamicTableSourceSpec tableSourceSpec,
            RowType outputType,
            String description) {
        super(
                ExecNodeContext.newNodeId(),
                ExecNodeContext.newContext(BatchExecTableSourceScan.class),
                ExecNodeContext.newPersistedConfig(BatchExecTableSourceScan.class, tableConfig),
                tableSourceSpec,
                Collections.emptyList(),
                outputType,
                description);
        this.tableConfig = tableConfig;
    }

    public String getDynamicFilteringDataListenerID() {
        return dynamicFilteringDataListenerID;
    }

    @Override
    protected Transformation<RowData> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config) {
        final Transformation<RowData> transformation =
                super.translateToPlanInternal(planner, config);
        // the boundedness has been checked via the runtime provider already, so we can safely
        // declare all legacy transformations as bounded to make the stream graph generator happy
        ExecNodeUtil.makeLegacySourceTransformationsBounded(transformation);
        return transformation;
    }

    public static BatchExecDynamicFilteringDataCollector getDynamicFilteringDataCollector(
            BatchExecNode<?> node) {
        Preconditions.checkState(
                node.getInputEdges().size() == 1,
                "The fact source must have one "
                        + "input representing dynamic filtering data collector");
        BatchExecNode<?> input = (BatchExecNode<?>) node.getInputEdges().get(0).getSource();
        if (input instanceof BatchExecDynamicFilteringDataCollector) {
            return (BatchExecDynamicFilteringDataCollector) input;
        }

        Preconditions.checkState(
                input instanceof BatchExecExchange,
                "There could only be BatchExecExchange "
                        + "between fact source and dynamic filtering data collector");
        return getDynamicFilteringDataCollector(input);
    }

    @Override
    public Transformation<RowData> createInputFormatTransformation(
            StreamExecutionEnvironment env,
            InputFormat<RowData, ?> inputFormat,
            InternalTypeInfo<RowData> outputTypeInfo,
            String operatorName) {
        // env.createInput will use ContinuousFileReaderOperator, but it do not support multiple
        // paths. If read partitioned source, after partition pruning, we need let InputFormat
        // to read multiple partitions which are multiple paths.
        // We can use InputFormatSourceFunction directly to support InputFormat.
        final InputFormatSourceFunction<RowData> function =
                new InputFormatSourceFunction<>(inputFormat, outputTypeInfo);
        return env.addSource(function, operatorName, outputTypeInfo).getTransformation();
    }

    public BatchExecTableSourceScan copyAndRemoveInputs() {
        BatchExecTableSourceScan tableSourceScan =
                new BatchExecTableSourceScan(
                        tableConfig,
                        getTableSourceSpec(),
                        (RowType) getOutputType(),
                        getDescription());
        tableSourceScan.setInputEdges(Collections.emptyList());
        return tableSourceScan;
    }
}
