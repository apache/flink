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

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.spec.VectorSearchSpec;
import org.apache.flink.table.planner.plan.nodes.exec.spec.VectorSearchTableSourceSpec;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecVectorSearchTableFunction;
import org.apache.flink.table.planner.plan.nodes.physical.common.CommonPhysicalVectorSearchFunction;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;
import org.apache.flink.table.planner.plan.utils.ChangelogPlanUtils;
import org.apache.flink.table.planner.plan.utils.VectorSearchUtil;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import org.apache.flink.util.Preconditions;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexProgram;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

/** Stream physical RelNode for vector search table function. */
public class StreamPhysicalVectorSearchTableFunction extends CommonPhysicalVectorSearchFunction
        implements StreamPhysicalRel {

    public StreamPhysicalVectorSearchTableFunction(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelNode input,
            TableSourceTable searchTable,
            @Nullable RexProgram projectionOnVectorTable,
            VectorSearchSpec vectorSearchSpec,
            RelDataType outputRowType) {
        super(
                cluster,
                traits,
                input,
                searchTable,
                projectionOnVectorTable,
                vectorSearchSpec,
                outputRowType);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new StreamPhysicalVectorSearchTableFunction(
                getCluster(),
                traitSet,
                inputs.get(0),
                searchTable,
                projectionOnVectorTable,
                vectorSearchSpec,
                outputRowType);
    }

    @Override
    public boolean requireWatermark() {
        return false;
    }

    @Override
    public ExecNode<?> translateToExecNode() {
        TableConfig tableConfig = ShortcutUtils.unwrapTableConfig(this);
        VectorSearchTableSourceSpec sourceSpec = new VectorSearchTableSourceSpec(searchTable);
        Preconditions.checkNotNull(sourceSpec.getTableSourceSpec())
                .setTableSource(searchTable.tableSource());
        if (projectionOnVectorTable != null) {
            throw new UnsupportedOperationException(
                    "Don't support calc on VECTOR_SEARCH node now.");
        }
        return new StreamExecVectorSearchTableFunction(
                tableConfig,
                sourceSpec,
                vectorSearchSpec,
                VectorSearchUtil.isAsyncVectorSearch(
                                searchTable,
                                Optional.ofNullable(vectorSearchSpec.getRuntimeConfig())
                                        .orElse(Collections.emptyMap()),
                                vectorSearchSpec.getSearchColumns().keySet())
                        ? VectorSearchUtil.getMergedVectorSearchAsyncOptions(
                                vectorSearchSpec.getRuntimeConfig() == null
                                        ? Collections.emptyMap()
                                        : vectorSearchSpec.getRuntimeConfig(),
                                tableConfig,
                                getInputChangelogMode())
                        : null,
                InputProperty.DEFAULT,
                FlinkTypeFactory.toLogicalRowType(outputRowType),
                getRelDetailedDescription());
    }

    // ~ Utilities --------------------------------------------------------------------------

    private ChangelogMode getInputChangelogMode() {
        return getInputChangelogMode(getInput());
    }

    private ChangelogMode getInputChangelogMode(RelNode rel) {
        if (rel instanceof StreamPhysicalRel) {
            return JavaScalaConversionUtil.toJava(
                            ChangelogPlanUtils.getChangelogMode((StreamPhysicalRel) rel))
                    .orElse(ChangelogMode.insertOnly());
        } else if (rel instanceof HepRelVertex) {
            return getInputChangelogMode(((HepRelVertex) rel).getCurrentRel());
        } else {
            return ChangelogMode.insertOnly();
        }
    }
}
