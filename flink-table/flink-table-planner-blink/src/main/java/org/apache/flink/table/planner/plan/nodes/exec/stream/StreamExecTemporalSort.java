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

package org.apache.flink.table.planner.plan.nodes.exec.stream;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.codegen.sort.ComparatorCodeGenerator;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.utils.SortSpec;
import org.apache.flink.table.runtime.generated.GeneratedRecordComparator;
import org.apache.flink.table.runtime.keyselector.EmptyRowDataKeySelector;
import org.apache.flink.table.runtime.operators.sort.ProcTimeSortOperator;
import org.apache.flink.table.runtime.operators.sort.RowTimeSortOperator;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampKind;
import org.apache.flink.table.types.logical.TimestampType;

import java.util.Collections;

/** {@link StreamExecNode} for time-ascending-order Sort without `limit`. */
public class StreamExecTemporalSort extends ExecNodeBase<RowData>
        implements StreamExecNode<RowData> {

    private final SortSpec sortSpec;

    public StreamExecTemporalSort(
            SortSpec sortSpec, ExecEdge inputEdge, RowType outputType, String description) {
        super(Collections.singletonList(inputEdge), outputType, description);
        this.sortSpec = sortSpec;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(PlannerBase planner) {
        // time ordering needs to be ascending
        if (sortSpec.getFieldSize() == 0 || !sortSpec.getFieldSpec(0).getIsAscendingOrder()) {
            throw new TableException(
                    "Sort: Primary sort order of a streaming table must be ascending on time.\n"
                            + "please re-check sort statement according to the description above");
        }

        ExecNode<RowData> inputNode = (ExecNode<RowData>) getInputNodes().get(0);
        Transformation<RowData> inputTransform = inputNode.translateToPlan(planner);

        RowType inputType = (RowType) inputNode.getOutputType();
        LogicalType timeType = inputType.getTypeAt(sortSpec.getFieldSpec(0).getFieldIndex());
        TableConfig config = planner.getTableConfig();
        if (timeType instanceof TimestampType) {
            TimestampType keyType = (TimestampType) timeType;
            if (keyType.getKind() == TimestampKind.ROWTIME) {
                return createSortRowTime(inputType, inputTransform, config);
            } else if (keyType.getKind() == TimestampKind.PROCTIME) {
                return createSortProcTime(inputType, inputTransform, config);
            }
        }
        throw new TableException(
                String.format(
                        "Sort: Internal Error\n"
                                + "First field in temporal sort is not a time attribute, %s is given.",
                        timeType));
    }

    /** Create Sort logic based on processing time. */
    private Transformation<RowData> createSortProcTime(
            RowType inputType, Transformation<RowData> inputTransform, TableConfig tableConfig) {
        // if the order has secondary sorting fields in addition to the proctime
        if (sortSpec.getFieldSize() > 1) {
            // skip the first field which is the proctime field and would be ordered by timer.
            SortSpec specExcludeTime = sortSpec.createSubSortSpec(1);

            GeneratedRecordComparator rowComparator =
                    ComparatorCodeGenerator.gen(
                            tableConfig,
                            "ProcTimeSortComparator",
                            specExcludeTime.getFieldIndices(),
                            specExcludeTime.getFieldTypes(inputType),
                            specExcludeTime.getAscendingOrders(),
                            specExcludeTime.getNullsIsLast());
            ProcTimeSortOperator sortOperator =
                    new ProcTimeSortOperator(InternalTypeInfo.of(inputType), rowComparator);

            OneInputTransformation<RowData, RowData> transform =
                    new OneInputTransformation<>(
                            inputTransform,
                            getDesc(),
                            sortOperator,
                            InternalTypeInfo.of(inputType),
                            inputTransform.getParallelism());

            // as input node is singleton exchange, its parallelism is 1.
            if (inputsContainSingleton()) {
                transform.setParallelism(1);
                transform.setMaxParallelism(1);
            }

            EmptyRowDataKeySelector selector = EmptyRowDataKeySelector.INSTANCE;
            transform.setStateKeySelector(selector);
            transform.setStateKeyType(selector.getProducedType());
            return transform;
        } else {
            // if the order is done only on proctime we only need to forward the elements
            return inputTransform;
        }
    }

    /** Create Sort logic based on row time. */
    private Transformation<RowData> createSortRowTime(
            RowType inputType, Transformation<RowData> inputTransform, TableConfig tableConfig) {
        GeneratedRecordComparator rowComparator = null;
        if (sortSpec.getFieldSize() > 1) {
            // skip the first field which is the rowtime field and would be ordered by timer.
            SortSpec specExcludeTime = sortSpec.createSubSortSpec(1);
            rowComparator =
                    ComparatorCodeGenerator.gen(
                            tableConfig,
                            "RowTimeSortComparator",
                            specExcludeTime.getFieldIndices(),
                            specExcludeTime.getFieldTypes(inputType),
                            specExcludeTime.getAscendingOrders(),
                            specExcludeTime.getNullsIsLast());
        }
        RowTimeSortOperator sortOperator =
                new RowTimeSortOperator(
                        InternalTypeInfo.of(inputType),
                        sortSpec.getFieldSpec(0).getFieldIndex(),
                        rowComparator);

        OneInputTransformation<RowData, RowData> transform =
                new OneInputTransformation<>(
                        inputTransform,
                        getDesc(),
                        sortOperator,
                        InternalTypeInfo.of(inputType),
                        inputTransform.getParallelism());

        if (inputsContainSingleton()) {
            transform.setParallelism(1);
            transform.setMaxParallelism(1);
        }

        EmptyRowDataKeySelector selector = EmptyRowDataKeySelector.INSTANCE;
        transform.setStateKeySelector(selector);
        transform.setStateKeyType(selector.getProducedType());
        return transform;
    }
}
