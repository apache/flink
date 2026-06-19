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

package org.apache.flink.table.planner.plan.nodes.physical.common;

import org.apache.flink.table.planner.plan.nodes.exec.spec.VectorSearchSpec;
import org.apache.flink.table.planner.plan.nodes.physical.FlinkPhysicalRel;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;
import org.apache.flink.table.planner.plan.utils.FunctionCallUtil;
import org.apache.flink.table.planner.plan.utils.JoinTypeUtil;
import org.apache.flink.table.planner.plan.utils.RelExplainUtil;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexProgram;

import javax.annotation.Nullable;

import java.util.List;
import java.util.stream.Collectors;

/** Common physical node for {@code VECTOR_SEARCH} and {@code VECTOR_SEARCH_AGG}. */
public abstract class CommonPhysicalVectorSearchFunction extends SingleRel
        implements FlinkPhysicalRel {

    protected final TableSourceTable searchTable;
    protected final @Nullable RexProgram projectionOnVectorTable;
    protected final VectorSearchSpec vectorSearchSpec;
    protected final RelDataType outputRowType;

    protected CommonPhysicalVectorSearchFunction(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelNode input,
            RelOptTable searchTable,
            @Nullable RexProgram projectionOnVectorTable,
            VectorSearchSpec vectorSearchSpec,
            RelDataType outputRowType) {
        super(cluster, traits, input);
        this.searchTable = (TableSourceTable) searchTable;
        this.projectionOnVectorTable = projectionOnVectorTable;
        this.vectorSearchSpec = vectorSearchSpec;
        this.outputRowType = outputRowType;
    }

    @Override
    protected RelDataType deriveRowType() {
        return outputRowType;
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        List<String> columnToSearch =
                vectorSearchSpec.getSearchColumns().keySet().stream()
                        .map(searchTable.getRowType().getFieldNames()::get)
                        .collect(Collectors.toList());
        List<String> columnToQuery =
                vectorSearchSpec.getSearchColumns().values().stream()
                        .map(
                                param ->
                                        FunctionCallUtil.explainFunctionParam(
                                                param, getInput().getRowType().getFieldNames()))
                        .collect(Collectors.toList());

        Integer topK =
                ((FunctionCallUtil.Constant) vectorSearchSpec.getTopK())
                        .literal.getValueAs(Integer.class);

        String leftSelect = String.join(", ", getInput().getRowType().getFieldNames());
        String rightSelect =
                projectionOnVectorTable == null
                        ? String.join(", ", searchTable.getRowType().getFieldNames())
                        : RelExplainUtil.selectionToString(
                                projectionOnVectorTable,
                                this::getExpressionString,
                                RelExplainUtil.preferExpressionFormat(pw),
                                convertToExpressionDetail(pw.getDetailLevel()));

        return super.explainTerms(pw)
                .item("table", searchTable.contextResolvedTable().getIdentifier().asSummaryString())
                .item("joinType", JoinTypeUtil.getFlinkJoinType(vectorSearchSpec.getJoinType()))
                .item("columnToSearch", String.join(", ", columnToSearch))
                .item("columnToQuery", String.join(", ", columnToQuery))
                .item("topK", topK)
                .itemIf(
                        "config",
                        vectorSearchSpec.getRuntimeConfig(),
                        vectorSearchSpec.getRuntimeConfig() != null)
                .item("select", String.join(", ", leftSelect, rightSelect, "score"));
    }
}
