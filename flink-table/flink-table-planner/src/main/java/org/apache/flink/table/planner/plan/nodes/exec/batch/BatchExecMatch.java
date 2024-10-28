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

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.codegen.sort.SortCodeGenerator;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.MultipleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecMatch;
import org.apache.flink.table.planner.plan.nodes.exec.spec.MatchSpec;
import org.apache.flink.table.planner.plan.nodes.exec.spec.SortSpec;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.runtime.operators.sink.StreamRecordTimestampInserter;
import org.apache.flink.table.runtime.operators.sort.SortOperator;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.TypeCheckUtils;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.util.Collections;

import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.getPrecision;

/** Batch {@link ExecNode} which matches along with MATCH_RECOGNIZE. */
public class BatchExecMatch extends CommonExecMatch
        implements BatchExecNode<RowData>, MultipleTransformationTranslator<RowData> {

    public static final String TIMESTAMP_INSERTER_TRANSFORMATION = "timestamp-inserter";

    public BatchExecMatch(
            ReadableConfig tableConfig,
            MatchSpec matchSpec,
            InputProperty inputProperty,
            RowType outputType,
            String description) {
        super(
                ExecNodeContext.newNodeId(),
                ExecNodeContext.newContext(BatchExecMatch.class),
                ExecNodeContext.newPersistedConfig(BatchExecMatch.class, tableConfig),
                matchSpec,
                Collections.singletonList(inputProperty),
                outputType,
                description);
    }

    @Override
    public void checkOrderKeys(RowType inputRowType) {
        SortSpec orderKeys = matchSpec.getOrderKeys();
        if (orderKeys.getFieldSize() == 0) {
            throw new TableException("You must specify non-empty order by.");
        }

        SortSpec.SortFieldSpec timeOrderField = orderKeys.getFieldSpec(0);
        int timeOrderFieldIdx = timeOrderField.getFieldIndex();
        LogicalType timeOrderFieldType = inputRowType.getTypeAt(timeOrderFieldIdx);

        if (!TypeCheckUtils.isTimePoint(timeOrderFieldType)) {
            throw new TableException("You must specify time point for order by as the first one.");
        }

        // time ordering needs to be ascending
        if (!orderKeys.getAscendingOrders()[0]) {
            throw new TableException("Primary sort order of a table must be ascending on time.");
        }
    }

    @Override
    protected Transformation<RowData> translateOrder(
            PlannerBase planner,
            Transformation<RowData> inputTransform,
            RowType inputRowType,
            ExecEdge inputEdge,
            ExecNodeConfig config) {
        if (isProcTime(inputRowType)) {
            // In proctime process records in the order they come.
            return inputTransform;
        }

        SortSpec sortSpec = matchSpec.getOrderKeys();
        RowType inputType = (RowType) inputEdge.getOutputType();
        SortCodeGenerator codeGen =
                new SortCodeGenerator(
                        config, planner.getFlinkContext().getClassLoader(), inputType, sortSpec);
        SortOperator operator =
                new SortOperator(
                        codeGen.generateNormalizedKeyComputer("BatchExecSortComputer"),
                        codeGen.generateRecordComparator("BatchExecSortComparator"),
                        config.get(ExecutionConfigOptions.TABLE_EXEC_SORT_MAX_NUM_FILE_HANDLES),
                        config.get(ExecutionConfigOptions.TABLE_EXEC_SPILL_COMPRESSION_ENABLED),
                        (int)
                                config.get(
                                                ExecutionConfigOptions
                                                        .TABLE_EXEC_SPILL_COMPRESSION_BLOCK_SIZE)
                                        .getBytes(),
                        config.get(ExecutionConfigOptions.TABLE_EXEC_SORT_ASYNC_MERGE_ENABLED));
        long sortMemory =
                config.get(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_SORT_MEMORY).getBytes();
        OneInputTransformation<RowData, RowData> sortedInputTransform =
                ExecNodeUtil.createOneInputTransformation(
                        inputTransform,
                        createTransformationName(config),
                        createTransformationDescription(config),
                        SimpleOperatorFactory.of(operator),
                        InternalTypeInfo.of(inputRowType),
                        inputTransform.getParallelism(),
                        sortMemory,
                        false);
        return translateTimestamp(sortedInputTransform, inputRowType, config);
    }

    protected Transformation<RowData> translateTimestamp(
            Transformation<RowData> inputTransform, RowType inputRowType, ExecNodeConfig config) {
        // copy the timestamp field from order by clause into the StreamRecord timestamp field
        SortSpec.SortFieldSpec timeOrderField = matchSpec.getOrderKeys().getFieldSpec(0);
        int timeOrderFieldIdx = timeOrderField.getFieldIndex();
        LogicalType timeOrderFieldType = inputRowType.getTypeAt(timeOrderFieldIdx);
        int precision = getPrecision(timeOrderFieldType);
        Transformation<RowData> transform =
                ExecNodeUtil.createOneInputTransformation(
                        inputTransform,
                        createTransformationMeta(
                                TIMESTAMP_INSERTER_TRANSFORMATION,
                                String.format(
                                        "BatchRecordTimestampInserter(timestamp field: %s)",
                                        timeOrderFieldIdx),
                                "BatchRecordTimestampInserter",
                                config),
                        new StreamRecordTimestampInserter(timeOrderFieldIdx, precision),
                        inputTransform.getOutputType(),
                        inputTransform.getParallelism(),
                        false);
        if (inputsContainSingleton()) {
            transform.setParallelism(1);
            transform.setMaxParallelism(1);
        }
        return transform;
    }
}
