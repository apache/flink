/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.calcite.rel.logical;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.metadata.RelMdCollation;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.Util;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Set;

/**
 * Sub-class of {@link org.apache.calcite.rel.core.Project} not targeted at any particular engine or
 * calling convention.
 */
public final class LogicalProject extends Project {
    // ~ Constructors -----------------------------------------------------------

    /**
     * Creates a LogicalProject.
     *
     * <p>Use {@link #create} unless you know what you're doing.
     *
     * @param cluster Cluster this relational expression belongs to
     * @param traitSet Traits of this relational expression
     * @param hints Hints of this relational expression
     * @param input Input relational expression
     * @param projects List of expressions for the input columns
     * @param rowType Output row type
     * @param variablesSet Correlation variables set by this relational expression to be used by
     *     nested expressions
     */
    public LogicalProject(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            List<RelHint> hints,
            RelNode input,
            List<? extends RexNode> projects,
            RelDataType rowType,
            Set<CorrelationId> variablesSet) {
        super(cluster, traitSet, hints, input, projects, rowType, variablesSet);
        assert traitSet.containsIfApplicable(Convention.NONE);
    }

    @Deprecated // to be removed before 2.0
    public LogicalProject(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            List<RelHint> hints,
            RelNode input,
            List<? extends RexNode> projects,
            RelDataType rowType) {
        this(cluster, traitSet, hints, input, projects, rowType, ImmutableSet.of());
    }

    @Deprecated // to be removed before 2.0
    public LogicalProject(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode input,
            List<? extends RexNode> projects,
            RelDataType rowType) {
        this(cluster, traitSet, ImmutableList.of(), input, projects, rowType, ImmutableSet.of());
    }

    @Deprecated // to be removed before 2.0
    public LogicalProject(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode input,
            List<? extends RexNode> projects,
            RelDataType rowType,
            int flags) {
        this(cluster, traitSet, ImmutableList.of(), input, projects, rowType, ImmutableSet.of());
        Util.discard(flags);
    }

    @Deprecated // to be removed before 2.0
    public LogicalProject(
            RelOptCluster cluster,
            RelNode input,
            List<RexNode> projects,
            @Nullable List<? extends @Nullable String> fieldNames,
            int flags) {
        this(
                cluster,
                cluster.traitSetOf(RelCollations.EMPTY),
                ImmutableList.of(),
                input,
                projects,
                RexUtil.createStructType(cluster.getTypeFactory(), projects, fieldNames, null),
                ImmutableSet.of());
        Util.discard(flags);
    }

    /** Creates a LogicalProject by parsing serialized output. */
    public LogicalProject(RelInput input) {
        super(input);
    }

    // ~ Methods ----------------------------------------------------------------

    /**
     * Creates a LogicalProject.
     *
     * @deprecated Use {@link #create(RelNode, List, List, List, Set)} instead
     */
    @Deprecated // to be removed before 2.0
    public static LogicalProject create(
            final RelNode input,
            List<RelHint> hints,
            final List<? extends RexNode> projects,
            @Nullable List<? extends @Nullable String> fieldNames) {
        return create(input, hints, projects, fieldNames, ImmutableSet.of());
    }

    /** Creates a LogicalProject. */
    public static LogicalProject create(
            final RelNode input,
            List<RelHint> hints,
            final List<? extends RexNode> projects,
            @Nullable List<? extends @Nullable String> fieldNames,
            final Set<CorrelationId> variablesSet) {
        final RelOptCluster cluster = input.getCluster();
        final RelDataType rowType =
                RexUtil.createStructType(
                        cluster.getTypeFactory(),
                        projects,
                        fieldNames,
                        SqlValidatorUtil.F_SUGGESTER);
        return create(input, hints, projects, rowType, variablesSet);
    }

    /**
     * Creates a LogicalProject, specifying row type rather than field names.
     *
     * @deprecated Use {@link #create(RelNode, List, List, RelDataType, Set)} instead
     */
    @Deprecated // to be removed before 2.0
    public static LogicalProject create(
            final RelNode input,
            List<RelHint> hints,
            final List<? extends RexNode> projects,
            RelDataType rowType) {
        return create(input, hints, projects, rowType, ImmutableSet.of());
    }

    /** Creates a LogicalProject, specifying row type rather than field names. */
    public static LogicalProject create(
            final RelNode input,
            List<RelHint> hints,
            final List<? extends RexNode> projects,
            RelDataType rowType,
            final Set<CorrelationId> variablesSet) {
        final RelOptCluster cluster = input.getCluster();
        final RelMetadataQuery mq = cluster.getMetadataQuery();
        final RelTraitSet traitSet =
                cluster.traitSet()
                        .replace(Convention.NONE)
                        .replaceIfs(
                                RelCollationTraitDef.INSTANCE,
                                () -> RelMdCollation.project(mq, input, projects));
        return new LogicalProject(cluster, traitSet, hints, input, projects, rowType, variablesSet);
    }

    @Override
    public LogicalProject copy(
            RelTraitSet traitSet, RelNode input, List<RexNode> projects, RelDataType rowType) {
        LogicalProject project =
                new LogicalProject(getCluster(), traitSet, hints, input, projects, rowType);
        project.setIgnoreForMultiJoin(this.canBeIgnoredForMultiJoin);
        return project;
    }

    @Override
    public RelNode accept(RelShuttle shuttle) {
        return shuttle.visit(this);
    }

    @Override
    public RelNode withHints(List<RelHint> hintList) {
        return new LogicalProject(
                getCluster(), traitSet, hintList, input, getProjects(), getRowType(), variablesSet);
    }

    @Override
    public boolean deepEquals(@Nullable Object obj) {
        return deepEquals0(obj);
    }

    @Override
    public int deepHashCode() {
        return deepHashCode0();
    }

    // BEGIN FLINK MODIFICATION

    private boolean canBeIgnoredForMultiJoin = false;

    public void setIgnoreForMultiJoin(boolean canBeIgnoredForMultiJoin) {
        this.canBeIgnoredForMultiJoin = canBeIgnoredForMultiJoin;
    }

    public boolean canBeIgnoredForMultiJoin() {
        return canBeIgnoredForMultiJoin;
    }

    // END FLINK MODIFICATION

}
