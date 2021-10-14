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

package org.apache.flink.table.planner.plan.nodes.exec.common;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.codegen.ValuesCodeGenerator;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.SingleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.serde.RexNodeJsonSerializer;
import org.apache.flink.table.runtime.operators.values.ValuesInputFormat;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;

import java.util.Collections;
import java.util.List;

/** Base {@link ExecNode} that read records from given values. */
public abstract class CommonExecValues extends ExecNodeBase<RowData>
        implements SingleTransformationTranslator<RowData> {

    public static final String FIELD_NAME_TUPLES = "tuples";

    @JsonIgnore private final List<List<RexLiteral>> tuples;

    public CommonExecValues(
            List<List<RexLiteral>> tuples, int id, RowType outputType, String description) {
        super(id, Collections.emptyList(), outputType, description);
        this.tuples = tuples;
    }

    @Override
    protected Transformation<RowData> translateToPlanInternal(PlannerBase planner) {
        final ValuesInputFormat inputFormat =
                ValuesCodeGenerator.generatorInputFormat(
                        planner.getTableConfig(),
                        (RowType) getOutputType(),
                        tuples,
                        getClass().getSimpleName());
        final Transformation<RowData> transformation =
                planner.getExecEnv()
                        .createInput(inputFormat, inputFormat.getProducedType())
                        .getTransformation();
        transformation.setName(getDescription());
        transformation.setParallelism(1);
        transformation.setMaxParallelism(1);
        return transformation;
    }

    /**
     * In order to use {@link RexNodeJsonSerializer} to serialize {@link RexLiteral}, so we force
     * cast element of tuples to {@link RexNode} which is the parent class of {@link RexLiteral}.
     */
    @JsonProperty(value = FIELD_NAME_TUPLES)
    public List<List<RexNode>> getTuples() {
        return (List<List<RexNode>>) (Object) tuples;
    }
}
