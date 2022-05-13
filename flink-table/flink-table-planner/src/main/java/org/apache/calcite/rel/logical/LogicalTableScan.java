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

package org.apache.calcite.rel.logical;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.schema.Table;

import java.util.List;

/**
 * A <code>LogicalTableScan</code> reads all the rows from a {@link RelOptTable}.
 *
 * <p>This class is copied from Calcite because the {@link #explainTerms} should consider hints.
 *
 * <p>If the table is a <code>net.sf.saffron.ext.JdbcTable</code>, then this is literally possible.
 * But for other kinds of tables, there may be many ways to read the data from the table. For some
 * kinds of table, it may not even be possible to read all of the rows unless some narrowing
 * constraint is applied.
 *
 * <p>In the example of the <code>net.sf.saffron.ext.ReflectSchema</code> schema,
 *
 * <blockquote>
 *
 * <pre>select from fields</pre>
 *
 * </blockquote>
 *
 * <p>cannot be implemented, but
 *
 * <blockquote>
 *
 * <pre>select from fields as f
 * where f.getClass().getName().equals("java.lang.String")</pre>
 *
 * </blockquote>
 *
 * <p>can. It is the optimizer's responsibility to find these ways, by applying transformation
 * rules.
 */
public final class LogicalTableScan extends TableScan {
    // ~ Constructors -----------------------------------------------------------

    /**
     * Creates a LogicalTableScan.
     *
     * <p>Use {@link #create} unless you know what you're doing.
     */
    public LogicalTableScan(
            RelOptCluster cluster, RelTraitSet traitSet, List<RelHint> hints, RelOptTable table) {
        super(cluster, traitSet, hints, table);
    }

    @Deprecated // to be removed before 2.0
    public LogicalTableScan(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table) {
        this(cluster, traitSet, ImmutableList.of(), table);
    }

    @Deprecated // to be removed before 2.0
    public LogicalTableScan(RelOptCluster cluster, RelOptTable table) {
        this(cluster, cluster.traitSetOf(Convention.NONE), ImmutableList.of(), table);
    }

    /** Creates a LogicalTableScan by parsing serialized output. */
    public LogicalTableScan(RelInput input) {
        super(input);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        assert traitSet.containsIfApplicable(Convention.NONE);
        assert inputs.isEmpty();
        return this;
    }

    // BEGIN FLINK MODIFICATION
    // {@link #explainTerms} method should consider hints due to CALCITE-4581.
    // This file should be remove once CALCITE-4581 is fixed.
    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).itemIf("hints", getHints(), !getHints().isEmpty());
    }
    // END FLINK MODIFICATION

    /**
     * Creates a LogicalTableScan.
     *
     * @param cluster Cluster
     * @param relOptTable Table
     * @param hints The hints
     */
    public static LogicalTableScan create(
            RelOptCluster cluster, final RelOptTable relOptTable, List<RelHint> hints) {
        final Table table = relOptTable.unwrap(Table.class);
        final RelTraitSet traitSet =
                cluster.traitSetOf(Convention.NONE)
                        .replaceIfs(
                                RelCollationTraitDef.INSTANCE,
                                () -> {
                                    if (table != null) {
                                        return table.getStatistic().getCollations();
                                    }
                                    return ImmutableList.of();
                                });
        return new LogicalTableScan(cluster, traitSet, hints, relOptTable);
    }

    @Override
    public RelNode withHints(List<RelHint> hintList) {
        return new LogicalTableScan(getCluster(), traitSet, hintList, table);
    }
}
