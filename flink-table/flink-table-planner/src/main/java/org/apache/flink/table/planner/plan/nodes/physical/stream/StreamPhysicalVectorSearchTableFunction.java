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

package org.apache.flink.table.planner.plan.nodes.physical.stream;

import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.spec.VectorSearchSpec;
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

/** Stream physical RelNode for vector search table function. */
public class StreamPhysicalVectorSearchTableFunction extends SingleRel
        implements StreamPhysicalRel {

    private final RelOptTable searchTable;
    private final @Nullable RexProgram calcProgram;
    private final VectorSearchSpec vectorSearchSpec;
    private final RelDataType outputRowType;

    public StreamPhysicalVectorSearchTableFunction(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelNode input,
            RelOptTable searchTable,
            @Nullable RexProgram calcProgram,
            VectorSearchSpec vectorSearchSpec,
            RelDataType outputRowType) {
        super(cluster, traits, input);
        this.searchTable = searchTable;
        this.calcProgram = calcProgram;
        this.vectorSearchSpec = vectorSearchSpec;
        this.outputRowType = outputRowType;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new StreamPhysicalVectorSearchTableFunction(
                getCluster(),
                traitSet,
                inputs.get(0),
                searchTable,
                calcProgram,
                vectorSearchSpec,
                outputRowType);
    }

    @Override
    protected RelDataType deriveRowType() {
        return outputRowType;
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        List<String> columnToSearch =
                vectorSearchSpec.getSearchColumns().keySet().stream()
                        .map(
                                calcProgram == null
                                        ? searchTable.getRowType().getFieldNames()::get
                                        : calcProgram.getOutputRowType().getFieldNames()::get)
                        .collect(Collectors.toList());
        List<String> columnToQuery =
                vectorSearchSpec.getSearchColumns().values().stream()
                        .map(this::explainQueryColumnParam)
                        .collect(Collectors.toList());

        Integer topK =
                ((FunctionCallUtil.Constant) vectorSearchSpec.getTopK())
                        .literal.getValueAs(Integer.class);

        String leftSelect = String.join(", ", getInput(0).getRowType().getFieldNames());
        String rightSelect =
                calcProgram == null
                        ? String.join(", ", searchTable.getRowType().getFieldNames())
                        : RelExplainUtil.selectionToString(
                                calcProgram,
                                this::getExpressionString,
                                RelExplainUtil.preferExpressionFormat(pw),
                                convertToExpressionDetail(pw.getDetailLevel()));

        return super.explainTerms(pw)
                .item(
                        "table",
                        ((TableSourceTable) searchTable)
                                .contextResolvedTable()
                                .getIdentifier()
                                .asSummaryString())
                .item("joinType", JoinTypeUtil.getFlinkJoinType(vectorSearchSpec.getJoinType()))
                .item("columnToSearch", String.join(", ", columnToSearch))
                .item("columnToQuery", String.join(", ", columnToQuery))
                .item("topK", topK)
                .item("select", String.join(", ", leftSelect, rightSelect, "score"));
    }

    @Override
    public boolean requireWatermark() {
        return false;
    }

    @Override
    public ExecNode<?> translateToExecNode() {
        throw new UnsupportedOperationException("Vector search not supported yet.");
    }

    private String explainQueryColumnParam(FunctionCallUtil.FunctionParam param) {
        if (param instanceof FunctionCallUtil.FieldRef) {
            int index = ((FunctionCallUtil.FieldRef) param).index;
            return getInput(0).getRowType().getFieldNames().get(index);
        } else if (param instanceof FunctionCallUtil.Constant) {
            return ((FunctionCallUtil.Constant) param).literal.toString();
        }
        return null;
    }
}
