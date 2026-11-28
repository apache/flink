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

import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecChangelogFilter;
import org.apache.flink.table.planner.plan.utils.ChangelogPlanUtils;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.types.inference.StaticArgumentTrait;
import org.apache.flink.types.RowKind;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.type.RelDataType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.apache.flink.table.planner.utils.ShortcutUtils.unwrapTableConfig;

/**
 * Stream physical RelNode that keeps only the rows whose {@link RowKind} is in a given set and
 * drops all others.
 *
 * <p>The planner uses it to drop UPDATE_BEFORE and DELETE rows below a function that ignores
 * retractions (see {@link StaticArgumentTrait#IGNORE_RETRACTIONS}).
 */
public class StreamPhysicalChangelogFilter extends SingleRel implements StreamPhysicalRel {

    private final EnumSet<RowKind> keptRowKinds;

    public StreamPhysicalChangelogFilter(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode input,
            EnumSet<RowKind> keptRowKinds) {
        super(cluster, traitSet, input);
        this.keptRowKinds = EnumSet.copyOf(keptRowKinds);
    }

    public Set<RowKind> getKeptRowKinds() {
        return Collections.unmodifiableSet(keptRowKinds);
    }

    @Override
    public boolean requireWatermark() {
        return false;
    }

    @Override
    protected RelDataType deriveRowType() {
        return getInput().getRowType();
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new StreamPhysicalChangelogFilter(
                getCluster(), traitSet, inputs.get(0), keptRowKinds);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        final ChangelogMode.Builder keptMode = ChangelogMode.newBuilder();
        keptRowKinds.forEach(keptMode::addContainedKind);
        return super.explainTerms(pw)
                .item(
                        "keep",
                        ChangelogPlanUtils.stringifyChangelogMode(
                                JavaScalaConversionUtil.toScala(Optional.of(keptMode.build()))));
    }

    @Override
    public ExecNode<?> translateToExecNode() {
        final Optional<ChangelogMode> outputMode =
                JavaScalaConversionUtil.toJava(ChangelogPlanUtils.getChangelogMode(this));
        if (outputMode.isPresent()
                && !keptRowKinds.containsAll(outputMode.get().getContainedKinds())) {
            throw new IllegalStateException(
                    getClass().getSimpleName()
                            + " is required to emit row kinds that it drops. "
                            + "This should never happen.");
        }

        return new StreamExecChangelogFilter(
                unwrapTableConfig(this),
                new ArrayList<>(keptRowKinds),
                InputProperty.DEFAULT,
                FlinkTypeFactory.toLogicalRowType(getRowType()),
                getRelDetailedDescription());
    }
}
