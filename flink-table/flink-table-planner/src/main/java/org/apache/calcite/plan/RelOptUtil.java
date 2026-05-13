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
package org.apache.calcite.plan;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.LinkedHashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.interpreter.Bindables;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.linq4j.function.Experimental;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.externalize.RelDotWriter;
import org.apache.calcite.rel.externalize.RelJsonWriter;
import org.apache.calcite.rel.externalize.RelWriterImpl;
import org.apache.calcite.rel.externalize.RelXmlWriter;
import org.apache.calcite.rel.hint.HintStrategyTable;
import org.apache.calcite.rel.hint.Hintable;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalAsofJoin;
import org.apache.calcite.rel.logical.LogicalCalc;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rel.rules.MultiJoin;
import org.apache.calcite.rel.rules.ProjectMergeRule;
import org.apache.calcite.rel.stream.StreamRules;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rex.LogicVisitor;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexExecutor;
import org.apache.calcite.rex.RexExecutorImpl;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSqlStandardConvertletTable;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexToSqlNodeConverter;
import org.apache.calcite.rex.RexToSqlNodeConverterImpl;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.runtime.PairList;
import org.apache.calcite.schema.ModifiableView;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.MultisetSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Permutation;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.MappingType;
import org.apache.calcite.util.mapping.Mappings;
import org.checkerframework.checker.initialization.qual.NotOnlyInitialized;
import org.checkerframework.checker.initialization.qual.UnknownInitialization;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.nullness.qual.PolyNull;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.AbstractList;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static org.apache.calcite.rel.type.RelDataTypeImpl.NON_NULLABLE_SUFFIX;

/**
 * <code>RelOptUtil</code> defines static utility methods for use in optimizing {@link RelNode}s.
 *
 * <p>FLINK modifications (backport of CALCITE-6764): Lines 2074 ~ 2106
 */
public abstract class RelOptUtil {
    // ~ Static fields/initializers ---------------------------------------------

    public static final double EPSILON = 1.0e-5;

    /**
     * Default amount by which the complexity of a {@link Project} or {@link Filter} may increase
     * when applying a rule. (Complexity is, roughly, the number of {@link RexNode}s in all
     * expressions.)
     *
     * @see ProjectMergeRule.Config#bloat()
     * @see FilterProjectTransposeRule.Config#bloat()
     * @see RelBuilder.Config#bloat()
     */
    public static final int DEFAULT_BLOAT = 100;

    @SuppressWarnings("Guava")
    @Deprecated // to be removed before 2.0
    public static final com.google.common.base.Predicate<Filter> FILTER_PREDICATE =
            f -> !f.containsOver();

    @SuppressWarnings("Guava")
    @Deprecated // to be removed before 2.0
    public static final com.google.common.base.Predicate<Project> PROJECT_PREDICATE =
            RelOptUtil::notContainsWindowedAgg;

    @SuppressWarnings("Guava")
    @Deprecated // to be removed before 2.0
    public static final com.google.common.base.Predicate<Calc> CALC_PREDICATE =
            RelOptUtil::notContainsWindowedAgg;

    // ~ Methods ----------------------------------------------------------------

    /** Whether this node is a limit without sort specification. */
    public static boolean isPureLimit(RelNode rel) {
        return isLimit(rel) && !isOrder(rel);
    }

    /** Whether this node is a sort without limit specification. */
    public static boolean isPureOrder(RelNode rel) {
        return !isLimit(rel) && isOrder(rel);
    }

    /** Whether this node contains a limit specification. */
    public static boolean isLimit(RelNode rel) {
        return (rel instanceof Sort) && ((Sort) rel).fetch != null;
    }

    /** Whether this node contains a sort specification. */
    public static boolean isOrder(RelNode rel) {
        return (rel instanceof Sort) && !((Sort) rel).getCollation().getFieldCollations().isEmpty();
    }

    /** Whether this node contains a offset specification. */
    public static boolean isOffset(RelNode rel) {
        return (rel instanceof Sort) && ((Sort) rel).offset != null;
    }

    /** Returns a set of tables used by this expression or its children. */
    public static Set<RelOptTable> findTables(RelNode rel) {
        return new LinkedHashSet<>(findAllTables(rel));
    }

    /** Returns a list of all tables used by this expression or its children. */
    public static List<RelOptTable> findAllTables(RelNode rel) {
        final Multimap<Class<? extends RelNode>, RelNode> nodes =
                rel.getCluster().getMetadataQuery().getNodeTypes(rel);
        final List<RelOptTable> usedTables = new ArrayList<>();
        if (nodes == null) {
            return usedTables;
        }
        for (Map.Entry<Class<? extends RelNode>, Collection<RelNode>> e :
                nodes.asMap().entrySet()) {
            if (TableScan.class.isAssignableFrom(e.getKey())) {
                for (RelNode node : e.getValue()) {
                    TableScan scan = (TableScan) node;
                    usedTables.add(scan.getTable());
                }
            }
        }
        return usedTables;
    }

    /** Returns a list of all table qualified names used by this expression or its children. */
    public static List<String> findAllTableQualifiedNames(RelNode rel) {
        return findAllTables(rel).stream()
                .map(table -> table.getQualifiedName().toString())
                .collect(Collectors.toList());
    }

    /** Returns a list of variables set by a relational expression or its descendants. */
    public static Set<CorrelationId> getVariablesSet(RelNode rel) {
        VariableSetVisitor visitor = new VariableSetVisitor();
        go(visitor, rel);
        return visitor.variables;
    }

    @Deprecated // to be removed before 2.0
    @SuppressWarnings("MixedMutabilityReturnType")
    public static List<CorrelationId> getVariablesSetAndUsed(RelNode rel0, RelNode rel1) {
        Set<CorrelationId> set = getVariablesSet(rel0);
        if (set.isEmpty()) {
            return ImmutableList.of();
        }
        Set<CorrelationId> used = getVariablesUsed(rel1);
        if (used.isEmpty()) {
            return ImmutableList.of();
        }
        final List<CorrelationId> result = new ArrayList<>();
        for (CorrelationId s : set) {
            if (used.contains(s) && !result.contains(s)) {
                result.add(s);
            }
        }
        return result;
    }

    /**
     * Returns the set of variables used by a relational expression or its descendants.
     *
     * <p>The set may contain "duplicates" (variables with different ids that, when resolved, will
     * reference the same source relational expression).
     *
     * <p>The item type is the same as {@link org.apache.calcite.rex.RexCorrelVariable#id}.
     */
    public static Set<CorrelationId> getVariablesUsed(RelNode rel) {
        CorrelationCollector visitor = new CorrelationCollector();
        rel.accept(visitor);
        return visitor.vuv.variables;
    }

    /**
     * Returns the set of variables used by the given list of sub-queries and its descendants.
     *
     * @param subQueries The sub-queries containing correlation variables
     * @return A list of correlation identifiers found within the sub-queries. The type of the
     *     [CorrelationId] parameter corresponds to {@link
     *     org.apache.calcite.rex.RexCorrelVariable#id}.
     */
    public static Set<CorrelationId> getVariablesUsed(List<RexSubQuery> subQueries) {
        // Internally this function calls getVariablesUsed on a RelNode to get all the
        // correlated variables in that RelNode
        Set<CorrelationId> correlationIds = new HashSet<>();
        for (RexSubQuery subQ : subQueries) {
            correlationIds.addAll(getVariablesUsed(subQ.rel));
        }
        return correlationIds;
    }

    /** Finds which columns of a correlation variable are used within a relational expression. */
    public static ImmutableBitSet correlationColumns(CorrelationId id, RelNode rel) {
        final CorrelationCollector collector = new CorrelationCollector();
        rel.accept(collector);
        final ImmutableBitSet.Builder builder = ImmutableBitSet.builder();
        for (int field : collector.vuv.variableFields.get(id)) {
            if (field >= 0) {
                builder.set(field);
            }
        }
        return builder.build();
    }

    /**
     * Returns true, and calls {@link Litmus#succeed()} if a given relational expression does not
     * contain a given correlation.
     */
    public static boolean notContainsCorrelation(
            RelNode r, CorrelationId correlationId, Litmus litmus) {
        final Set<CorrelationId> set = getVariablesUsed(r);
        if (!set.contains(correlationId)) {
            return litmus.succeed();
        } else {
            return litmus.fail("contains {}", correlationId);
        }
    }

    /** Sets a {@link RelVisitor} going on a given relational expression, and returns the result. */
    public static void go(RelVisitor visitor, RelNode p) {
        try {
            visitor.go(p);
        } catch (Exception e) {
            throw new RuntimeException("while visiting tree", e);
        }
    }

    /**
     * Returns a list of the types of the fields in a given struct type. The list is immutable.
     *
     * @param type Struct type
     * @return List of field types
     * @see org.apache.calcite.rel.type.RelDataType#getFieldNames()
     */
    public static List<RelDataType> getFieldTypeList(final RelDataType type) {
        return Util.transform(type.getFieldList(), RelDataTypeField::getType);
    }

    public static boolean areRowTypesEqual(
            RelDataType rowType1, RelDataType rowType2, boolean compareNames) {
        if (rowType1 == rowType2) {
            return true;
        }
        if (compareNames) {
            // if types are not identity-equal, then either the names or
            // the types must be different
            return false;
        }
        if (rowType2.getFieldCount() != rowType1.getFieldCount()) {
            return false;
        }
        final List<RelDataTypeField> f1 = rowType1.getFieldList();
        final List<RelDataTypeField> f2 = rowType2.getFieldList();
        for (Pair<RelDataTypeField, RelDataTypeField> pair : Pair.zip(f1, f2)) {
            final RelDataType type1 = pair.left.getType();
            final RelDataType type2 = pair.right.getType();
            // If one of the types is ANY comparison should succeed
            if (type1.getSqlTypeName() == SqlTypeName.ANY
                    || type2.getSqlTypeName() == SqlTypeName.ANY) {
                continue;
            }
            if (!type1.equals(type2)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Verifies that a row type being added to an equivalence class matches the existing type,
     * raising an assertion if this is not the case.
     *
     * @param originalRel canonical rel for equivalence class
     * @param newRel rel being added to equivalence class
     * @param equivalenceClass object representing equivalence class
     */
    public static void verifyTypeEquivalence(
            RelNode originalRel, RelNode newRel, Object equivalenceClass) {
        RelDataType expectedRowType = originalRel.getRowType();
        RelDataType actualRowType = newRel.getRowType();

        // Row types must be the same, except for field names.
        if (areRowTypesEqual(expectedRowType, actualRowType, false)) {
            return;
        }

        String s =
                "Cannot add expression of different type to set:\n"
                        + "set type is "
                        + expectedRowType.getFullTypeString()
                        + "\nexpression type is "
                        + actualRowType.getFullTypeString()
                        + "\nset is "
                        + equivalenceClass
                        + "\nexpression is "
                        + RelOptUtil.toString(newRel)
                        + getFullTypeDifferenceString(
                                "rowtype of original rel",
                                expectedRowType,
                                "rowtype of new rel",
                                actualRowType);
        throw new AssertionError(s);
    }

    /**
     * Copy the {@link org.apache.calcite.rel.hint.RelHint}s from {@code originalRel} to {@code
     * newRel} if both of them are {@link Hintable}.
     *
     * <p>The two relational expressions are assumed as semantically equivalent, that means the
     * hints should be attached to the relational expression that expects to have them.
     *
     * <p>Try to propagate the hints to the first relational expression that matches, this is needed
     * because many planner rules would generate a sub-tree whose root rel type is different with
     * the original matched rel.
     *
     * <p>For the worst case, there is no relational expression that can apply these hints, and the
     * whole sub-tree would be visited. We add a protection here: if the visiting depth is over than
     * 3, just returns, because there are rare cases the new created sub-tree has layers bigger than
     * that.
     *
     * <p>This is a best effort, we do not know exactly how the nodes are transformed in all kinds
     * of planner rules, so for some complex relational expressions, the hints would very probably
     * lost.
     *
     * <p>This function is experimental and would change without any notes.
     *
     * @param originalRel Original relational expression
     * @param equiv New equivalent relational expression
     * @return A copy of {@code newRel} with attached qualified hints from {@code originalRel}, or
     *     {@code newRel} directly if one of them are not {@link Hintable}
     */
    @Experimental
    public static RelNode propagateRelHints(RelNode originalRel, RelNode equiv) {
        if (!(originalRel instanceof Hintable) || ((Hintable) originalRel).getHints().isEmpty()) {
            return equiv;
        }
        final RelShuttle shuttle =
                new SubTreeHintPropagateShuttle(
                        originalRel.getCluster().getHintStrategies(),
                        ((Hintable) originalRel).getHints());
        return equiv.accept(shuttle);
    }

    /**
     * Propagates the relational expression hints from root node to leaf node.
     *
     * @param rel The relational expression
     * @param reset Flag saying if to reset the existing hints before the propagation
     * @return New relational expression with hints propagated
     */
    public static RelNode propagateRelHints(RelNode rel, boolean reset) {
        if (reset) {
            rel = rel.accept(new ResetHintsShuttle());
        }
        final RelShuttle shuttle =
                new RelHintPropagateShuttle(rel.getCluster().getHintStrategies());
        return rel.accept(shuttle);
    }

    /**
     * Copy the {@link org.apache.calcite.rel.hint.RelHint}s from {@code originalRel} to {@code
     * newRel} if both of them are {@link Hintable}.
     *
     * <p>The hints would be attached directly(e.g. without any filtering).
     *
     * @param originalRel Original relational expression
     * @param newRel New relational expression
     * @return A copy of {@code newRel} with attached hints from {@code originalRel}, or {@code
     *     newRel} directly if one of them are not {@link Hintable}
     */
    public static RelNode copyRelHints(RelNode originalRel, RelNode newRel) {
        return copyRelHints(originalRel, newRel, false);
    }

    /**
     * Copy the {@link org.apache.calcite.rel.hint.RelHint}s from {@code originalRel} to {@code
     * newRel} if both of them are {@link Hintable}.
     *
     * <p>The hints would be filtered by the specified hint strategies if {@code filterHints} is
     * true.
     *
     * @param originalRel Original relational expression
     * @param newRel New relational expression
     * @param filterHints Flag saying if to filter out unqualified hints for {@code newRel}
     * @return A copy of {@code newRel} with attached hints from {@code originalRel}, or {@code
     *     newRel} directly if one of them are not {@link Hintable}
     */
    public static RelNode copyRelHints(RelNode originalRel, RelNode newRel, boolean filterHints) {
        if (originalRel == newRel && !filterHints) {
            return originalRel;
        }

        if (originalRel instanceof Hintable
                && newRel instanceof Hintable
                && !((Hintable) originalRel).getHints().isEmpty()) {
            final List<RelHint> hints = ((Hintable) originalRel).getHints();
            if (filterHints) {
                HintStrategyTable hintStrategies = originalRel.getCluster().getHintStrategies();
                return ((Hintable) newRel).attachHints(hintStrategies.apply(hints, newRel));
            } else {
                // Keep all the hints if filterHints is false for 2 reasons:
                // 1. Keep sync with the hints propagation logic,
                // see RelHintPropagateShuttle for details.
                // 2. We may re-propagate these hints when decorrelating a query.
                return ((Hintable) newRel).attachHints(hints);
            }
        }
        return newRel;
    }

    /**
     * Returns a permutation describing where output fields come from. In the returned map, value of
     * {@code map.getTargetOpt(i)} is {@code n} if field {@code i} projects input field {@code n} or
     * applies a cast on {@code n}, -1 if it is another expression.
     */
    public static Mappings.TargetMapping permutationIgnoreCast(
            List<RexNode> nodes, RelDataType inputRowType) {
        final Mappings.TargetMapping mapping =
                Mappings.create(
                        MappingType.PARTIAL_FUNCTION, nodes.size(), inputRowType.getFieldCount());
        for (Ord<RexNode> node : Ord.zip(nodes)) {
            if (node.e instanceof RexInputRef) {
                mapping.set(node.i, ((RexInputRef) node.e).getIndex());
            } else if (node.e.isA(SqlKind.CAST)) {
                final RexNode operand = ((RexCall) node.e).getOperands().get(0);
                if (operand instanceof RexInputRef) {
                    mapping.set(node.i, ((RexInputRef) operand).getIndex());
                }
            }
        }
        return mapping;
    }

    /**
     * Returns a permutation describing where output fields come from. In the returned map, value of
     * {@code map.getTargetOpt(i)} is {@code n} if field {@code i} projects input field {@code n},
     * -1 if it is an expression.
     */
    public static Mappings.TargetMapping permutation(
            List<RexNode> nodes, RelDataType inputRowType) {
        final Mappings.TargetMapping mapping =
                Mappings.create(
                        MappingType.PARTIAL_FUNCTION, nodes.size(), inputRowType.getFieldCount());
        for (Ord<RexNode> node : Ord.zip(nodes)) {
            if (node.e instanceof RexInputRef) {
                mapping.set(node.i, ((RexInputRef) node.e).getIndex());
            }
        }
        return mapping;
    }

    /**
     * Returns a permutation describing where the Project's fields come from after the Project is
     * pushed down.
     */
    public static Mappings.TargetMapping permutationPushDownProject(
            List<RexNode> nodes, RelDataType inputRowType, int sourceOffset, int targetOffset) {
        final Mappings.TargetMapping mapping =
                Mappings.create(
                        MappingType.PARTIAL_FUNCTION,
                        inputRowType.getFieldCount() + sourceOffset,
                        nodes.size() + targetOffset);
        for (Ord<RexNode> node : Ord.zip(nodes)) {
            if (node.e instanceof RexInputRef) {
                mapping.set(
                        ((RexInputRef) node.e).getIndex() + sourceOffset, node.i + targetOffset);
            }
        }
        return mapping;
    }

    @Deprecated // to be removed before 2.0
    public static RelNode createExistsPlan(
            RelOptCluster cluster,
            RelNode seekRel,
            @Nullable List<RexNode> conditions,
            @Nullable RexLiteral extraExpr,
            @Nullable String extraName) {
        assert extraExpr == null || extraName != null;
        RelNode ret = seekRel;

        if (conditions != null && !conditions.isEmpty()) {
            RexNode conditionExp =
                    RexUtil.composeConjunction(cluster.getRexBuilder(), conditions, true);

            if (conditionExp != null) {
                final RelFactories.FilterFactory factory = RelFactories.DEFAULT_FILTER_FACTORY;
                ret = factory.createFilter(ret, conditionExp, ImmutableSet.of());
            }
        }

        if (extraExpr != null) {
            RexBuilder rexBuilder = cluster.getRexBuilder();

            assert extraExpr == rexBuilder.makeLiteral(true);

            // this should only be called for the exists case
            // first stick an Agg on top of the sub-query
            // agg does not like no agg functions so just pretend it is
            // doing a min(TRUE)

            final RelBuilder relBuilder = RelFactories.LOGICAL_BUILDER.create(cluster, null);
            ret =
                    relBuilder
                            .push(ret)
                            .project(extraExpr)
                            .aggregate(
                                    relBuilder.groupKey(),
                                    relBuilder.min(relBuilder.field(0)).as(extraName))
                            .build();
        }

        return ret;
    }

    @Deprecated // to be removed before 2.0
    public static Exists createExistsPlan(
            RelNode seekRel, SubQueryType subQueryType, Logic logic, boolean notIn) {
        final RelBuilder relBuilder =
                RelFactories.LOGICAL_BUILDER.create(seekRel.getCluster(), null);
        return createExistsPlan(seekRel, subQueryType, logic, notIn, relBuilder);
    }

    /**
     * Creates a plan suitable for use in <code>EXISTS</code> or <code>IN</code> statements.
     *
     * @see org.apache.calcite.sql2rel.SqlToRelConverter SqlToRelConverter#convertExists
     * @param seekRel A query rel, for example the resulting rel from 'select * from emp' or 'values
     *     (1,2,3)' or '('Foo', 34)'.
     * @param subQueryType Sub-query type
     * @param logic Whether to use 2- or 3-valued boolean logic
     * @param notIn Whether the operator is NOT IN
     * @param relBuilder Builder for relational expressions
     * @return A pair of a relational expression which outer joins a boolean condition column, and a
     *     numeric offset. The offset is 2 if column 0 is the number of rows and column 1 is the
     *     number of rows with not-null keys; 0 otherwise.
     */
    public static Exists createExistsPlan(
            RelNode seekRel,
            SubQueryType subQueryType,
            Logic logic,
            boolean notIn,
            RelBuilder relBuilder) {
        switch (subQueryType) {
            case SCALAR:
                return new Exists(seekRel, false, true);
            default:
                break;
        }

        switch (logic) {
            case TRUE_FALSE_UNKNOWN:
            case UNKNOWN_AS_TRUE:
                if (notIn && !containsNullableFields(seekRel)) {
                    logic = Logic.TRUE_FALSE;
                }
                break;
            default:
                break;
        }
        RelNode ret = seekRel;
        final RelOptCluster cluster = seekRel.getCluster();
        final RexBuilder rexBuilder = cluster.getRexBuilder();
        final int keyCount = ret.getRowType().getFieldCount();
        final boolean outerJoin = notIn || logic == RelOptUtil.Logic.TRUE_FALSE_UNKNOWN;
        if (!outerJoin) {
            final LogicalAggregate aggregate =
                    LogicalAggregate.create(
                            ret,
                            ImmutableList.of(),
                            ImmutableBitSet.range(keyCount),
                            null,
                            ImmutableList.of());
            return new Exists(aggregate, false, false);
        }

        // for IN/NOT IN, it needs to output the fields
        final List<RexNode> exprs = new ArrayList<>();
        if (subQueryType == SubQueryType.IN) {
            for (int i = 0; i < keyCount; i++) {
                exprs.add(rexBuilder.makeInputRef(ret, i));
            }
        }

        final int projectedKeyCount = exprs.size();
        exprs.add(rexBuilder.makeLiteral(true));

        ret =
                relBuilder
                        .push(ret)
                        .project(exprs)
                        .aggregate(
                                relBuilder.groupKey(ImmutableBitSet.range(projectedKeyCount)),
                                relBuilder.min(relBuilder.field(projectedKeyCount)))
                        .build();

        switch (logic) {
            case TRUE_FALSE_UNKNOWN:
            case UNKNOWN_AS_TRUE:
                return new Exists(ret, true, true);
            default:
                return new Exists(ret, false, true);
        }
    }

    @Deprecated // to be removed before 2.0
    public static RelNode createRenameRel(RelDataType outputType, RelNode rel) {
        RelDataType inputType = rel.getRowType();
        List<RelDataTypeField> inputFields = inputType.getFieldList();
        int n = inputFields.size();

        List<RelDataTypeField> outputFields = outputType.getFieldList();
        assert outputFields.size() == n
                : "rename: field count mismatch: in=" + inputType + ", out" + outputType;

        final PairList<RexNode, String> renames = PairList.of();
        final RexBuilder rexBuilder = rel.getCluster().getRexBuilder();
        Pair.forEach(
                inputFields,
                outputFields,
                (inputField, outputField) -> {
                    assert inputField.getType().equals(outputField.getType());
                    renames.add(
                            rexBuilder.makeInputRef(inputField.getType(), inputField.getIndex()),
                            outputField.getName());
                });
        final RelBuilder relBuilder = RelFactories.LOGICAL_BUILDER.create(rel.getCluster(), null);
        return relBuilder.push(rel).project(renames.leftList(), renames.rightList(), true).build();
    }

    @Deprecated // to be removed before 2.0
    public static RelNode createFilter(RelNode child, RexNode condition) {
        final RelFactories.FilterFactory factory = RelFactories.DEFAULT_FILTER_FACTORY;
        return factory.createFilter(child, condition, ImmutableSet.of());
    }

    @Deprecated // to be removed before 2.0
    public static RelNode createFilter(
            RelNode child, RexNode condition, RelFactories.FilterFactory filterFactory) {
        return filterFactory.createFilter(child, condition, ImmutableSet.of());
    }

    /**
     * Creates a filter, using the default filter factory, or returns the original relational
     * expression if the condition is trivial.
     */
    public static RelNode createFilter(RelNode child, Iterable<? extends RexNode> conditions) {
        return createFilter(child, conditions, RelFactories.DEFAULT_FILTER_FACTORY);
    }

    /**
     * Creates a filter using the default factory, or returns the original relational expression if
     * the condition is trivial.
     */
    public static RelNode createFilter(
            RelNode child,
            Iterable<? extends RexNode> conditions,
            RelFactories.FilterFactory filterFactory) {
        final RelOptCluster cluster = child.getCluster();
        final RexNode condition =
                RexUtil.composeConjunction(cluster.getRexBuilder(), conditions, true);
        if (condition == null) {
            return child;
        } else {
            return filterFactory.createFilter(child, condition, ImmutableSet.of());
        }
    }

    @Deprecated // to be removed before 2.0
    public static RelNode createNullFilter(RelNode rel, Integer[] fieldOrdinals) {
        RexNode condition = null;
        final RexBuilder rexBuilder = rel.getCluster().getRexBuilder();
        RelDataType rowType = rel.getRowType();
        int n;
        if (fieldOrdinals != null) {
            n = fieldOrdinals.length;
        } else {
            n = rowType.getFieldCount();
        }
        List<RelDataTypeField> fields = rowType.getFieldList();
        for (int i = 0; i < n; ++i) {
            int iField;
            if (fieldOrdinals != null) {
                iField = fieldOrdinals[i];
            } else {
                iField = i;
            }
            RelDataType type = fields.get(iField).getType();
            if (!type.isNullable()) {
                continue;
            }
            RexNode newCondition =
                    rexBuilder.makeCall(
                            SqlStdOperatorTable.IS_NOT_NULL, rexBuilder.makeInputRef(type, iField));
            if (condition == null) {
                condition = newCondition;
            } else {
                condition = rexBuilder.makeCall(SqlStdOperatorTable.AND, condition, newCondition);
            }
        }
        if (condition == null) {
            // no filtering required
            return rel;
        }

        final RelFactories.FilterFactory factory = RelFactories.DEFAULT_FILTER_FACTORY;
        return factory.createFilter(rel, condition, ImmutableSet.of());
    }

    /**
     * Creates a projection which casts a rel's output to a desired row type.
     *
     * <p>No need to create new projection if {@code rel} is already a project, instead, create a
     * projection with the input of {@code rel} and the new cast expressions.
     *
     * <p>The desired row type and the row type to be converted must have the same number of fields.
     *
     * @param rel producer of rows to be converted
     * @param castRowType row type after cast
     * @param rename if true, use field names from castRowType; if false, preserve field names from
     *     rel
     * @return conversion rel
     */
    public static RelNode createCastRel(
            final RelNode rel, RelDataType castRowType, boolean rename) {
        return createCastRel(rel, castRowType, rename, RelFactories.DEFAULT_PROJECT_FACTORY);
    }

    /**
     * Creates a projection which casts a rel's output to a desired row type.
     *
     * <p>No need to create new projection if {@code rel} is already a project, instead, create a
     * projection with the input of {@code rel} and the new cast expressions.
     *
     * <p>The desired row type and the row type to be converted must have the same number of fields.
     *
     * @param rel producer of rows to be converted
     * @param castRowType row type after cast
     * @param rename if true, use field names from castRowType; if false, preserve field names from
     *     rel
     * @param projectFactory Project Factory
     * @return conversion rel
     */
    public static RelNode createCastRel(
            final RelNode rel,
            RelDataType castRowType,
            boolean rename,
            RelFactories.ProjectFactory projectFactory) {
        requireNonNull(projectFactory, "projectFactory");
        RelDataType rowType = rel.getRowType();
        if (areRowTypesEqual(rowType, castRowType, rename)) {
            // nothing to do
            return rel;
        }
        if (rowType.getFieldCount() != castRowType.getFieldCount()) {
            throw new IllegalArgumentException(
                    "Field counts are not equal: "
                            + "rowType ["
                            + rowType
                            + "] castRowType ["
                            + castRowType
                            + "]");
        }
        final RexBuilder rexBuilder = rel.getCluster().getRexBuilder();
        List<RexNode> castExps;
        RelNode input;
        List<RelHint> hints = ImmutableList.of();
        Set<CorrelationId> correlationVariables;
        if (rel instanceof Project) {
            // No need to create another project node if the rel
            // is already a project.
            final Project project = (Project) rel;
            castExps =
                    RexUtil.generateCastExpressions(
                            rexBuilder, castRowType, ((Project) rel).getProjects());
            input = rel.getInput(0);
            hints = project.getHints();
            correlationVariables = project.getVariablesSet();
        } else {
            castExps = RexUtil.generateCastExpressions(rexBuilder, castRowType, rowType);
            input = rel;
            correlationVariables = ImmutableSet.of();
        }
        if (rename) {
            // Use names and types from castRowType.
            return projectFactory.createProject(
                    input, hints, castExps, castRowType.getFieldNames(), correlationVariables);
        } else {
            // Use names from rowType, types from castRowType.
            return projectFactory.createProject(
                    input, hints, castExps, rowType.getFieldNames(), correlationVariables);
        }
    }

    /** Gets all fields in an aggregate. */
    public static Set<Integer> getAllFields(Aggregate aggregate) {
        return getAllFields2(aggregate.getGroupSet(), aggregate.getAggCallList());
    }

    /** Gets all fields in an aggregate. */
    public static Set<Integer> getAllFields2(
            ImmutableBitSet groupSet, List<AggregateCall> aggCallList) {
        final Set<Integer> allFields = new TreeSet<>(groupSet.asList());
        for (AggregateCall aggregateCall : aggCallList) {
            allFields.addAll(aggregateCall.getArgList());
            if (aggregateCall.filterArg >= 0) {
                allFields.add(aggregateCall.filterArg);
            }
            if (aggregateCall.distinctKeys != null) {
                allFields.addAll(aggregateCall.distinctKeys.asList());
            }
            allFields.addAll(RelCollations.ordinals(aggregateCall.collation));
        }
        return allFields;
    }

    /**
     * Creates a LogicalAggregate that removes all duplicates from the result of an underlying
     * relational expression.
     *
     * @param rel underlying rel
     * @return rel implementing SingleValueAgg
     */
    public static RelNode createSingleValueAggRel(RelOptCluster cluster, RelNode rel) {
        final int aggCallCnt = rel.getRowType().getFieldCount();
        final List<AggregateCall> aggCalls = new ArrayList<>();

        for (int i = 0; i < aggCallCnt; i++) {
            aggCalls.add(
                    AggregateCall.create(
                            SqlStdOperatorTable.SINGLE_VALUE,
                            false,
                            false,
                            false,
                            ImmutableList.of(),
                            ImmutableList.of(i),
                            -1,
                            null,
                            RelCollations.EMPTY,
                            0,
                            rel,
                            null,
                            null));
        }

        return LogicalAggregate.create(
                rel, ImmutableList.of(), ImmutableBitSet.of(), null, aggCalls);
    }

    // CHECKSTYLE: IGNORE 1
    /**
     * @deprecated Use {@link RelBuilder#distinct()}.
     */
    @Deprecated // to be removed before 2.0
    public static RelNode createDistinctRel(RelNode rel) {
        return LogicalAggregate.create(
                rel,
                ImmutableList.of(),
                ImmutableBitSet.range(rel.getRowType().getFieldCount()),
                null,
                ImmutableList.of());
    }

    @Deprecated // to be removed before 2.0
    public static boolean analyzeSimpleEquiJoin(LogicalJoin join, int[] joinFieldOrdinals) {
        RexNode joinExp = join.getCondition();
        if (joinExp.getKind() != SqlKind.EQUALS) {
            return false;
        }
        RexCall binaryExpression = (RexCall) joinExp;
        RexNode leftComparand = binaryExpression.operands.get(0);
        RexNode rightComparand = binaryExpression.operands.get(1);
        if (!(leftComparand instanceof RexInputRef)) {
            return false;
        }
        if (!(rightComparand instanceof RexInputRef)) {
            return false;
        }

        final int leftFieldCount = join.getLeft().getRowType().getFieldCount();
        RexInputRef leftFieldAccess = (RexInputRef) leftComparand;
        if (!(leftFieldAccess.getIndex() < leftFieldCount)) {
            // left field must access left side of join
            return false;
        }

        RexInputRef rightFieldAccess = (RexInputRef) rightComparand;
        if (!(rightFieldAccess.getIndex() >= leftFieldCount)) {
            // right field must access right side of join
            return false;
        }

        joinFieldOrdinals[0] = leftFieldAccess.getIndex();
        joinFieldOrdinals[1] = rightFieldAccess.getIndex() - leftFieldCount;
        return true;
    }

    /**
     * Splits out the equi-join components of a join condition, and returns what's left. For
     * example, given the condition
     *
     * <blockquote>
     *
     * <code>L.A = R.X AND L.B = L.C AND (L.D = 5 OR L.E =
     * R.Y)</code>
     *
     * </blockquote>
     *
     * <p>returns
     *
     * <ul>
     *   <li>leftKeys = {A}
     *   <li>rightKeys = {X}
     *   <li>rest = L.B = L.C AND (L.D = 5 OR L.E = R.Y)
     * </ul>
     *
     * @param left left input to join
     * @param right right input to join
     * @param condition join condition
     * @param leftKeys The ordinals of the fields from the left input which are equi-join keys
     * @param rightKeys The ordinals of the fields from the right input which are equi-join keys
     * @param filterNulls List of boolean values for each join key position indicating whether the
     *     operator filters out nulls or not. Value is true if the operator is EQUALS and false if
     *     the operator is IS NOT DISTINCT FROM (or an expanded version). If <code>filterNulls
     *     </code> is null, only join conditions with EQUALS operators are considered equi-join
     *     components. Rest (including IS NOT DISTINCT FROM) are returned in remaining join
     *     condition.
     * @return remaining join filters that are not equijoins; may return a {@link RexLiteral} true,
     *     but never null
     */
    public static RexNode splitJoinCondition(
            RelNode left,
            RelNode right,
            RexNode condition,
            List<Integer> leftKeys,
            List<Integer> rightKeys,
            @Nullable List<Boolean> filterNulls) {
        final List<RexNode> nonEquiList = new ArrayList<>();

        splitJoinCondition(left, right, condition, leftKeys, rightKeys, filterNulls, nonEquiList);

        return RexUtil.composeConjunction(left.getCluster().getRexBuilder(), nonEquiList);
    }

    /**
     * As {@link #splitJoinCondition(RelNode, RelNode, RexNode, List, List, List)}, but writes
     * non-equi conditions to a conjunctive list.
     */
    public static void splitJoinCondition(
            RelNode left,
            RelNode right,
            RexNode condition,
            List<Integer> leftKeys,
            List<Integer> rightKeys,
            @Nullable List<Boolean> filterNulls,
            List<RexNode> nonEquiList) {
        splitJoinCondition(
                left.getCluster().getRexBuilder(),
                left.getRowType().getFieldCount(),
                condition,
                leftKeys,
                rightKeys,
                filterNulls,
                nonEquiList);
    }

    @Deprecated // to be removed before 2.0
    public static boolean isEqui(RelNode left, RelNode right, RexNode condition) {
        final List<Integer> leftKeys = new ArrayList<>();
        final List<Integer> rightKeys = new ArrayList<>();
        final List<Boolean> filterNulls = new ArrayList<>();
        final List<RexNode> nonEquiList = new ArrayList<>();
        splitJoinCondition(
                left.getCluster().getRexBuilder(),
                left.getRowType().getFieldCount(),
                condition,
                leftKeys,
                rightKeys,
                filterNulls,
                nonEquiList);
        return nonEquiList.isEmpty();
    }

    /**
     * Splits out the equi-join (and optionally, a single non-equi) components of a join condition,
     * and returns what's left. Projection might be required by the caller to provide join keys that
     * are not direct field references.
     *
     * @param sysFieldList list of system fields
     * @param leftRel left join input
     * @param rightRel right join input
     * @param condition join condition
     * @param leftJoinKeys The join keys from the left input which are equi-join keys
     * @param rightJoinKeys The join keys from the right input which are equi-join keys
     * @param filterNulls The join key positions for which null values will not match. null values
     *     only match for the "is not distinct from" condition.
     * @param rangeOp if null, only locate equi-joins; otherwise, locate a single non-equi join
     *     predicate and return its operator in this list; join keys associated with the non-equi
     *     join predicate are at the end of the key lists returned
     * @return What's left, never null
     */
    public static RexNode splitJoinCondition(
            List<RelDataTypeField> sysFieldList,
            RelNode leftRel,
            RelNode rightRel,
            RexNode condition,
            List<RexNode> leftJoinKeys,
            List<RexNode> rightJoinKeys,
            @Nullable List<Integer> filterNulls,
            @Nullable List<SqlOperator> rangeOp) {
        return splitJoinCondition(
                sysFieldList,
                ImmutableList.of(leftRel, rightRel),
                condition,
                ImmutableList.of(leftJoinKeys, rightJoinKeys),
                filterNulls,
                rangeOp);
    }

    /**
     * Splits out the equi-join (and optionally, a single non-equi) components of a join condition,
     * and returns what's left. Projection might be required by the caller to provide join keys that
     * are not direct field references.
     *
     * @param sysFieldList list of system fields
     * @param inputs join inputs
     * @param condition join condition
     * @param joinKeys The join keys from the inputs which are equi-join keys
     * @param filterNulls The join key positions for which null values will not match. null values
     *     only match for the "is not distinct from" condition.
     * @param rangeOp if null, only locate equi-joins; otherwise, locate a single non-equi join
     *     predicate and return its operator in this list; join keys associated with the non-equi
     *     join predicate are at the end of the key lists returned
     * @return What's left, never null
     */
    public static RexNode splitJoinCondition(
            List<RelDataTypeField> sysFieldList,
            List<RelNode> inputs,
            RexNode condition,
            List<List<RexNode>> joinKeys,
            @Nullable List<Integer> filterNulls,
            @Nullable List<SqlOperator> rangeOp) {
        final List<RexNode> nonEquiList = new ArrayList<>();

        splitJoinCondition(
                sysFieldList, inputs, condition, joinKeys, filterNulls, rangeOp, nonEquiList);

        // Convert the remainders into a list that are AND'ed together.
        return RexUtil.composeConjunction(inputs.get(0).getCluster().getRexBuilder(), nonEquiList);
    }

    @Deprecated // to be removed before 2.0
    public static @Nullable RexNode splitCorrelatedFilterCondition(
            LogicalFilter filter, List<RexInputRef> joinKeys, List<RexNode> correlatedJoinKeys) {
        final List<RexNode> nonEquiList = new ArrayList<>();

        splitCorrelatedFilterCondition(
                filter, filter.getCondition(), joinKeys, correlatedJoinKeys, nonEquiList);

        // Convert the remainders into a list that are AND'ed together.
        return RexUtil.composeConjunction(filter.getCluster().getRexBuilder(), nonEquiList, true);
    }

    public static @Nullable RexNode splitCorrelatedFilterCondition(
            LogicalFilter filter,
            List<RexNode> joinKeys,
            List<RexNode> correlatedJoinKeys,
            boolean extractCorrelatedFieldAccess) {
        return splitCorrelatedFilterCondition(
                (Filter) filter, joinKeys, correlatedJoinKeys, extractCorrelatedFieldAccess);
    }

    public static @Nullable RexNode splitCorrelatedFilterCondition(
            Filter filter,
            List<RexNode> joinKeys,
            List<RexNode> correlatedJoinKeys,
            boolean extractCorrelatedFieldAccess) {
        final List<RexNode> nonEquiList = new ArrayList<>();

        splitCorrelatedFilterCondition(
                filter,
                filter.getCondition(),
                joinKeys,
                correlatedJoinKeys,
                nonEquiList,
                extractCorrelatedFieldAccess);

        // Convert the remainders into a list that are AND'ed together.
        return RexUtil.composeConjunction(filter.getCluster().getRexBuilder(), nonEquiList, true);
    }

    private static void splitJoinCondition(
            List<RelDataTypeField> sysFieldList,
            List<RelNode> inputs,
            RexNode condition,
            List<List<RexNode>> joinKeys,
            @Nullable List<Integer> filterNulls,
            @Nullable List<SqlOperator> rangeOp,
            List<RexNode> nonEquiList) {
        final int sysFieldCount = sysFieldList.size();
        final RelOptCluster cluster = inputs.get(0).getCluster();
        final RexBuilder rexBuilder = cluster.getRexBuilder();
        final RelDataTypeFactory typeFactory = cluster.getTypeFactory();

        final ImmutableBitSet[] inputsRange = new ImmutableBitSet[inputs.size()];
        int totalFieldCount = 0;
        for (int i = 0; i < inputs.size(); i++) {
            final int firstField = totalFieldCount + sysFieldCount;
            totalFieldCount = firstField + inputs.get(i).getRowType().getFieldCount();
            inputsRange[i] = ImmutableBitSet.range(firstField, totalFieldCount);
        }

        // adjustment array
        int[] adjustments = new int[totalFieldCount];
        for (int i = 0; i < inputs.size(); i++) {
            final int adjustment = inputsRange[i].nextSetBit(0);
            for (int j = adjustment; j < inputsRange[i].length(); j++) {
                adjustments[j] = -adjustment;
            }
        }

        if (condition.getKind() == SqlKind.AND) {
            for (RexNode operand : ((RexCall) condition).getOperands()) {
                splitJoinCondition(
                        sysFieldList, inputs, operand, joinKeys, filterNulls, rangeOp, nonEquiList);
            }
            return;
        }

        if (condition instanceof RexCall) {
            RexNode leftKey = null;
            RexNode rightKey = null;
            int leftInput = 0;
            int rightInput = 0;
            List<RelDataTypeField> leftFields = null;
            List<RelDataTypeField> rightFields = null;
            boolean reverse = false;

            final RexCall call =
                    collapseExpandedIsNotDistinctFromExpr((RexCall) condition, rexBuilder);
            SqlKind kind = call.getKind();

            // Only consider range operators if we haven't already seen one
            if ((kind == SqlKind.EQUALS)
                    || (filterNulls != null && kind == SqlKind.IS_NOT_DISTINCT_FROM)
                    || (rangeOp != null
                            && rangeOp.isEmpty()
                            && (kind == SqlKind.GREATER_THAN
                                    || kind == SqlKind.GREATER_THAN_OR_EQUAL
                                    || kind == SqlKind.LESS_THAN
                                    || kind == SqlKind.LESS_THAN_OR_EQUAL))) {
                final List<RexNode> operands = call.getOperands();
                RexNode op0 = operands.get(0);
                RexNode op1 = operands.get(1);

                final ImmutableBitSet projRefs0 = InputFinder.bits(op0);
                final ImmutableBitSet projRefs1 = InputFinder.bits(op1);

                boolean foundBothInputs = false;
                for (int i = 0; i < inputs.size() && !foundBothInputs; i++) {
                    if (projRefs0.intersects(inputsRange[i])
                            && projRefs0.union(inputsRange[i]).equals(inputsRange[i])) {
                        if (leftKey == null) {
                            leftKey = op0;
                            leftInput = i;
                            leftFields = inputs.get(leftInput).getRowType().getFieldList();
                        } else {
                            rightKey = op0;
                            rightInput = i;
                            rightFields = inputs.get(rightInput).getRowType().getFieldList();
                            reverse = true;
                            foundBothInputs = true;
                        }
                    } else if (projRefs1.intersects(inputsRange[i])
                            && projRefs1.union(inputsRange[i]).equals(inputsRange[i])) {
                        if (leftKey == null) {
                            leftKey = op1;
                            leftInput = i;
                            leftFields = inputs.get(leftInput).getRowType().getFieldList();
                        } else {
                            rightKey = op1;
                            rightInput = i;
                            rightFields = inputs.get(rightInput).getRowType().getFieldList();
                            foundBothInputs = true;
                        }
                    }
                }

                if ((leftKey != null) && (rightKey != null)) {
                    // replace right Key input ref
                    rightKey =
                            rightKey.accept(
                                    new RelOptUtil.RexInputConverter(
                                            rexBuilder, rightFields, rightFields, adjustments));

                    // left key only needs to be adjusted if there are system
                    // fields, but do it for uniformity
                    leftKey =
                            leftKey.accept(
                                    new RelOptUtil.RexInputConverter(
                                            rexBuilder, leftFields, leftFields, adjustments));

                    RelDataType leftKeyType = leftKey.getType();
                    RelDataType rightKeyType = rightKey.getType();

                    if (leftKeyType != rightKeyType) {
                        // perform casting
                        RelDataType targetKeyType =
                                typeFactory.leastRestrictive(
                                        ImmutableList.of(leftKeyType, rightKeyType));

                        if (targetKeyType == null) {
                            throw new AssertionError(
                                    "Cannot find common type for join keys "
                                            + leftKey
                                            + " (type "
                                            + leftKeyType
                                            + ") and "
                                            + rightKey
                                            + " (type "
                                            + rightKeyType
                                            + ")");
                        }

                        if (leftKeyType != targetKeyType) {
                            leftKey = rexBuilder.makeCast(targetKeyType, leftKey);
                        }

                        if (rightKeyType != targetKeyType) {
                            rightKey = rexBuilder.makeCast(targetKeyType, rightKey);
                        }
                    }
                }
            }

            if ((leftKey != null) && (rightKey != null)) {
                // found suitable join keys
                // add them to key list, ensuring that if there is a
                // non-equi join predicate, it appears at the end of the
                // key list; also mark the null filtering property
                addJoinKey(
                        joinKeys.get(leftInput), leftKey, (rangeOp != null) && !rangeOp.isEmpty());
                addJoinKey(
                        joinKeys.get(rightInput),
                        rightKey,
                        (rangeOp != null) && !rangeOp.isEmpty());
                if (filterNulls != null && kind == SqlKind.EQUALS) {
                    // nulls are considered not matching for equality comparison
                    // add the position of the most recently inserted key
                    filterNulls.add(joinKeys.get(leftInput).size() - 1);
                }
                if (rangeOp != null && kind != SqlKind.EQUALS && kind != SqlKind.IS_DISTINCT_FROM) {
                    SqlOperator op = call.getOperator();
                    if (reverse) {
                        op = requireNonNull(op.reverse());
                    }
                    rangeOp.add(op);
                }
                return;
            } // else fall through and add this condition as nonEqui condition
        }

        // The operator is not of RexCall type
        // So we fail. Fall through.
        // Add this condition to the list of non-equi-join conditions.
        nonEquiList.add(condition);
    }

    /** Builds an equi-join condition from a set of left and right keys. */
    public static RexNode createEquiJoinCondition(
            final RelNode left,
            final List<Integer> leftKeys,
            final RelNode right,
            final List<Integer> rightKeys,
            final RexBuilder rexBuilder) {
        final List<RelDataType> leftTypes = RelOptUtil.getFieldTypeList(left.getRowType());
        final List<RelDataType> rightTypes = RelOptUtil.getFieldTypeList(right.getRowType());
        return RexUtil.composeConjunction(
                rexBuilder,
                new AbstractList<RexNode>() {
                    @Override
                    public RexNode get(int index) {
                        final int leftKey = leftKeys.get(index);
                        final int rightKey = rightKeys.get(index);
                        return rexBuilder.makeCall(
                                SqlStdOperatorTable.EQUALS,
                                rexBuilder.makeInputRef(leftTypes.get(leftKey), leftKey),
                                rexBuilder.makeInputRef(
                                        rightTypes.get(rightKey), leftTypes.size() + rightKey));
                    }

                    @Override
                    public int size() {
                        return leftKeys.size();
                    }
                });
    }

    /**
     * Returns {@link SqlOperator} for given {@link SqlKind} or returns {@code operator} when {@link
     * SqlKind} is not known.
     *
     * @param kind input kind
     * @param operator default operator value
     * @return SqlOperator for the given kind
     * @see RexUtil#op(SqlKind)
     */
    public static SqlOperator op(SqlKind kind, SqlOperator operator) {
        switch (kind) {
            case EQUALS:
                return SqlStdOperatorTable.EQUALS;
            case NOT_EQUALS:
                return SqlStdOperatorTable.NOT_EQUALS;
            case GREATER_THAN:
                return SqlStdOperatorTable.GREATER_THAN;
            case GREATER_THAN_OR_EQUAL:
                return SqlStdOperatorTable.GREATER_THAN_OR_EQUAL;
            case LESS_THAN:
                return SqlStdOperatorTable.LESS_THAN;
            case LESS_THAN_OR_EQUAL:
                return SqlStdOperatorTable.LESS_THAN_OR_EQUAL;
            case IS_DISTINCT_FROM:
                return SqlStdOperatorTable.IS_DISTINCT_FROM;
            case IS_NOT_DISTINCT_FROM:
                return SqlStdOperatorTable.IS_NOT_DISTINCT_FROM;
            default:
                return operator;
        }
    }

    private static void addJoinKey(
            List<RexNode> joinKeyList, RexNode key, boolean preserveLastElementInList) {
        if (!joinKeyList.isEmpty() && preserveLastElementInList) {
            joinKeyList.add(joinKeyList.size() - 1, key);
        } else {
            joinKeyList.add(key);
        }
    }

    private static void splitCorrelatedFilterCondition(
            LogicalFilter filter,
            RexNode condition,
            List<RexInputRef> joinKeys,
            List<RexNode> correlatedJoinKeys,
            List<RexNode> nonEquiList) {
        if (condition instanceof RexCall) {
            RexCall call = (RexCall) condition;
            if (call.getOperator().getKind() == SqlKind.AND) {
                for (RexNode operand : call.getOperands()) {
                    splitCorrelatedFilterCondition(
                            filter, operand, joinKeys, correlatedJoinKeys, nonEquiList);
                }
                return;
            }

            if (call.getOperator().getKind() == SqlKind.EQUALS) {
                final List<RexNode> operands = call.getOperands();
                RexNode op0 = operands.get(0);
                RexNode op1 = operands.get(1);

                if (!RexUtil.containsInputRef(op0) && op1 instanceof RexInputRef) {
                    correlatedJoinKeys.add(op0);
                    joinKeys.add((RexInputRef) op1);
                    return;
                } else if (op0 instanceof RexInputRef && !RexUtil.containsInputRef(op1)) {
                    joinKeys.add((RexInputRef) op0);
                    correlatedJoinKeys.add(op1);
                    return;
                }
            }
        }

        // The operator is not of RexCall type
        // So we fail. Fall through.
        // Add this condition to the list of non-equi-join conditions.
        nonEquiList.add(condition);
    }

    @SuppressWarnings("unused")
    private static void splitCorrelatedFilterCondition(
            LogicalFilter filter,
            RexNode condition,
            List<RexNode> joinKeys,
            List<RexNode> correlatedJoinKeys,
            List<RexNode> nonEquiList,
            boolean extractCorrelatedFieldAccess) {
        splitCorrelatedFilterCondition(
                (Filter) filter,
                condition,
                joinKeys,
                correlatedJoinKeys,
                nonEquiList,
                extractCorrelatedFieldAccess);
    }

    private static void splitCorrelatedFilterCondition(
            Filter filter,
            RexNode condition,
            List<RexNode> joinKeys,
            List<RexNode> correlatedJoinKeys,
            List<RexNode> nonEquiList,
            boolean extractCorrelatedFieldAccess) {
        if (condition instanceof RexCall) {
            RexCall call = (RexCall) condition;
            if (call.getOperator().getKind() == SqlKind.AND) {
                for (RexNode operand : call.getOperands()) {
                    splitCorrelatedFilterCondition(
                            filter,
                            operand,
                            joinKeys,
                            correlatedJoinKeys,
                            nonEquiList,
                            extractCorrelatedFieldAccess);
                }
                return;
            }

            if (call.getOperator().getKind() == SqlKind.EQUALS) {
                final List<RexNode> operands = call.getOperands();
                RexNode op0 = operands.get(0);
                RexNode op1 = operands.get(1);

                if (extractCorrelatedFieldAccess) {
                    if (!RexUtil.containsFieldAccess(op0) && op1 instanceof RexFieldAccess) {
                        joinKeys.add(op0);
                        correlatedJoinKeys.add(op1);
                        return;
                    } else if (op0 instanceof RexFieldAccess && !RexUtil.containsFieldAccess(op1)) {
                        correlatedJoinKeys.add(op0);
                        joinKeys.add(op1);
                        return;
                    }
                } else {
                    if (!RexUtil.containsInputRef(op0) && op1 instanceof RexInputRef) {
                        correlatedJoinKeys.add(op0);
                        joinKeys.add(op1);
                        return;
                    } else if (op0 instanceof RexInputRef && !RexUtil.containsInputRef(op1)) {
                        joinKeys.add(op0);
                        correlatedJoinKeys.add(op1);
                        return;
                    }
                }
            }
        }

        // The operator is not of RexCall type
        // So we fail. Fall through.
        // Add this condition to the list of non-equi-join conditions.
        nonEquiList.add(condition);
    }

    private static void splitJoinCondition(
            final RexBuilder rexBuilder,
            final int leftFieldCount,
            RexNode condition,
            List<Integer> leftKeys,
            List<Integer> rightKeys,
            @Nullable List<Boolean> filterNulls,
            List<RexNode> nonEquiList) {
        if (condition instanceof RexCall) {
            RexCall call = (RexCall) condition;
            SqlKind kind = call.getKind();
            if (kind == SqlKind.AND) {
                for (RexNode operand : call.getOperands()) {
                    splitJoinCondition(
                            rexBuilder,
                            leftFieldCount,
                            operand,
                            leftKeys,
                            rightKeys,
                            filterNulls,
                            nonEquiList);
                }
                return;
            }

            if (filterNulls != null) {
                call = collapseExpandedIsNotDistinctFromExpr(call, rexBuilder);
                kind = call.getKind();
            }

            // "=" and "IS NOT DISTINCT FROM" are the same except for how they
            // treat nulls.
            if (kind == SqlKind.EQUALS
                    || (filterNulls != null && kind == SqlKind.IS_NOT_DISTINCT_FROM)) {
                final List<RexNode> operands = call.getOperands();
                if ((operands.get(0) instanceof RexInputRef)
                        && (operands.get(1) instanceof RexInputRef)) {
                    RexInputRef op0 = (RexInputRef) operands.get(0);
                    RexInputRef op1 = (RexInputRef) operands.get(1);

                    RexInputRef leftField;
                    RexInputRef rightField;
                    if ((op0.getIndex() < leftFieldCount) && (op1.getIndex() >= leftFieldCount)) {
                        // Arguments were of form 'op0 = op1'
                        leftField = op0;
                        rightField = op1;
                    } else if ((op1.getIndex() < leftFieldCount)
                            && (op0.getIndex() >= leftFieldCount)) {
                        // Arguments were of form 'op1 = op0'
                        leftField = op1;
                        rightField = op0;
                    } else {
                        nonEquiList.add(condition);
                        return;
                    }

                    leftKeys.add(leftField.getIndex());
                    rightKeys.add(rightField.getIndex() - leftFieldCount);
                    if (filterNulls != null) {
                        filterNulls.add(kind == SqlKind.EQUALS);
                    }
                    return;
                }
                // Arguments were not field references, one from each side, so
                // we fail. Fall through.
            }
        }

        // Add this condition to the list of non-equi-join conditions.
        if (!condition.isAlwaysTrue()) {
            nonEquiList.add(condition);
        }
    }

    /**
     * Collapses an expanded version of {@code IS NOT DISTINCT FROM} expression.
     *
     * <p>Helper method for {@link #splitJoinCondition(RexBuilder, int, RexNode, List, List, List,
     * List)} and {@link #splitJoinCondition(List, List, RexNode, List, List, List, List)}.
     *
     * <p>If the given expr <code>call</code> is an expanded version of {@code IS NOT DISTINCT FROM}
     * function call, collapses it and return a {@code IS NOT DISTINCT FROM} function call.
     *
     * <p>For example: {@code t1.key IS NOT DISTINCT FROM t2.key} can rewritten in expanded form as
     * {@code t1.key = t2.key OR (t1.key IS NULL AND t2.key IS NULL)}.
     *
     * @param call Function expression to try collapsing
     * @param rexBuilder {@link RexBuilder} instance to create new {@link RexCall} instances.
     * @return If the given function is an expanded IS NOT DISTINCT FROM function call, return a IS
     *     NOT DISTINCT FROM function call. Otherwise return the input function call as it is.
     */
    public static RexCall collapseExpandedIsNotDistinctFromExpr(
            final RexCall call, final RexBuilder rexBuilder) {
        switch (call.getKind()) {
            case OR:
                return doCollapseExpandedIsNotDistinctFromOrExpr(call, rexBuilder);

            case CASE:
                return doCollapseExpandedIsNotDistinctFromCaseExpr(call, rexBuilder);

            default:
                return call;
        }
    }

    private static RexCall doCollapseExpandedIsNotDistinctFromOrExpr(
            final RexCall call, final RexBuilder rexBuilder) {
        if (call.getKind() != SqlKind.OR || call.getOperands().size() != 2) {
            return call;
        }

        final RexNode op0 = call.getOperands().get(0);
        final RexNode op1 = call.getOperands().get(1);

        if (!(op0 instanceof RexCall) || !(op1 instanceof RexCall)) {
            return call;
        }

        RexCall opEqCall = (RexCall) op0;
        RexCall opNullEqCall = (RexCall) op1;

        // Swapping the operands if necessary
        if (opEqCall.getKind() == SqlKind.AND
                && (opNullEqCall.getKind() == SqlKind.EQUALS
                        || opNullEqCall.getKind() == SqlKind.IS_TRUE)) {
            RexCall temp = opEqCall;
            opEqCall = opNullEqCall;
            opNullEqCall = temp;
        }

        // Check if EQUALS is actually wrapped in IS TRUE expression
        if (opEqCall.getKind() == SqlKind.IS_TRUE) {
            RexNode tmp = opEqCall.getOperands().get(0);
            if (!(tmp instanceof RexCall)) {
                return call;
            }
            opEqCall = (RexCall) tmp;
        }

        if (opNullEqCall.getKind() != SqlKind.AND
                || opNullEqCall.getOperands().size() != 2
                || opEqCall.getKind() != SqlKind.EQUALS) {
            return call;
        }

        final RexNode op10 = opNullEqCall.getOperands().get(0);
        final RexNode op11 = opNullEqCall.getOperands().get(1);
        if (op10.getKind() != SqlKind.IS_NULL || op11.getKind() != SqlKind.IS_NULL) {
            return call;
        }

        return doCollapseExpandedIsNotDistinctFrom(
                rexBuilder, call, (RexCall) op10, (RexCall) op11, opEqCall);
    }

    private static RexCall doCollapseExpandedIsNotDistinctFromCaseExpr(
            final RexCall call, final RexBuilder rexBuilder) {
        if (call.getKind() != SqlKind.CASE || call.getOperands().size() != 5) {
            return call;
        }

        final RexNode op0 = call.getOperands().get(0);
        final RexNode op1 = call.getOperands().get(1);
        final RexNode op2 = call.getOperands().get(2);
        final RexNode op3 = call.getOperands().get(3);
        final RexNode op4 = call.getOperands().get(4);

        if (!(op0 instanceof RexCall)
                || !(op1 instanceof RexCall)
                || !(op2 instanceof RexCall)
                || !(op3 instanceof RexCall)
                || !(op4 instanceof RexCall)) {
            return call;
        }

        RexCall ifCall = (RexCall) op0;
        RexCall thenCall = (RexCall) op1;
        RexCall elseIfCall = (RexCall) op2;
        RexCall elseIfThenCall = (RexCall) op3;
        RexCall elseCall = (RexCall) op4;

        if (ifCall.getKind() != SqlKind.IS_NULL
                || thenCall.getKind() != SqlKind.IS_NULL
                || elseIfCall.getKind() != SqlKind.IS_NULL
                || elseIfThenCall.getKind() != SqlKind.IS_NULL
                || elseCall.getKind() != SqlKind.EQUALS) {
            return call;
        }

        if (!ifCall.equals(elseIfThenCall) || !thenCall.equals(elseIfCall)) {
            return call;
        }

        return doCollapseExpandedIsNotDistinctFrom(rexBuilder, call, ifCall, elseIfCall, elseCall);
    }

    private static RexCall doCollapseExpandedIsNotDistinctFrom(
            final RexBuilder rexBuilder,
            final RexCall call,
            RexCall ifNull0Call,
            RexCall ifNull1Call,
            RexCall equalsCall) {
        final RexNode isNullInput0 = ifNull0Call.getOperands().get(0);
        final RexNode isNullInput1 = ifNull1Call.getOperands().get(0);

        final RexNode equalsInput0 =
                RexUtil.removeNullabilityCast(
                        rexBuilder.getTypeFactory(), equalsCall.getOperands().get(0));
        final RexNode equalsInput1 =
                RexUtil.removeNullabilityCast(
                        rexBuilder.getTypeFactory(), equalsCall.getOperands().get(1));

        if ((isNullInput0.equals(equalsInput0) && isNullInput1.equals(equalsInput1))
                || (isNullInput1.equals(equalsInput0) && isNullInput0.equals(equalsInput1))) {
            return (RexCall)
                    rexBuilder.makeCall(
                            SqlStdOperatorTable.IS_NOT_DISTINCT_FROM,
                            ImmutableList.of(isNullInput0, isNullInput1));
        }

        return call;
    }

    @Deprecated // to be removed before 2.0
    public static void projectJoinInputs(
            RelNode[] inputRels,
            List<RexNode> leftJoinKeys,
            List<RexNode> rightJoinKeys,
            int systemColCount,
            List<Integer> leftKeys,
            List<Integer> rightKeys,
            List<Integer> outputProj) {
        RelNode leftRel = inputRels[0];
        RelNode rightRel = inputRels[1];
        final RelOptCluster cluster = leftRel.getCluster();
        final RexBuilder rexBuilder = cluster.getRexBuilder();

        int origLeftInputSize = leftRel.getRowType().getFieldCount();
        int origRightInputSize = rightRel.getRowType().getFieldCount();

        final List<RexNode> newLeftFields = new ArrayList<>();
        final List<@Nullable String> newLeftFieldNames = new ArrayList<>();

        final List<RexNode> newRightFields = new ArrayList<>();
        final List<@Nullable String> newRightFieldNames = new ArrayList<>();
        int leftKeyCount = leftJoinKeys.size();
        int rightKeyCount = rightJoinKeys.size();
        int i;

        for (i = 0; i < systemColCount; i++) {
            outputProj.add(i);
        }

        for (i = 0; i < origLeftInputSize; i++) {
            final RelDataTypeField field = leftRel.getRowType().getFieldList().get(i);
            newLeftFields.add(rexBuilder.makeInputRef(field.getType(), i));
            newLeftFieldNames.add(field.getName());
            outputProj.add(systemColCount + i);
        }

        int newLeftKeyCount = 0;
        for (i = 0; i < leftKeyCount; i++) {
            RexNode leftKey = leftJoinKeys.get(i);

            if (leftKey instanceof RexInputRef) {
                // already added to the projected left fields
                // only need to remember the index in the join key list
                leftKeys.add(((RexInputRef) leftKey).getIndex());
            } else {
                newLeftFields.add(leftKey);
                newLeftFieldNames.add(null);
                leftKeys.add(origLeftInputSize + newLeftKeyCount);
                newLeftKeyCount++;
            }
        }

        int leftFieldCount = origLeftInputSize + newLeftKeyCount;
        for (i = 0; i < origRightInputSize; i++) {
            final RelDataTypeField field = rightRel.getRowType().getFieldList().get(i);
            newRightFields.add(rexBuilder.makeInputRef(field.getType(), i));
            newRightFieldNames.add(field.getName());
            outputProj.add(systemColCount + leftFieldCount + i);
        }

        int newRightKeyCount = 0;
        for (i = 0; i < rightKeyCount; i++) {
            RexNode rightKey = rightJoinKeys.get(i);

            if (rightKey instanceof RexInputRef) {
                // already added to the projected left fields
                // only need to remember the index in the join key list
                rightKeys.add(((RexInputRef) rightKey).getIndex());
            } else {
                newRightFields.add(rightKey);
                newRightFieldNames.add(null);
                rightKeys.add(origRightInputSize + newRightKeyCount);
                newRightKeyCount++;
            }
        }

        final RelBuilder relBuilder = RelFactories.LOGICAL_BUILDER.create(cluster, null);

        // added project if need to produce new keys than the original input
        // fields
        if (newLeftKeyCount > 0) {
            leftRel =
                    relBuilder
                            .push(leftRel)
                            .project(newLeftFields, newLeftFieldNames, true)
                            .build();
        }

        if (newRightKeyCount > 0) {
            rightRel =
                    relBuilder.push(rightRel).project(newRightFields, newRightFieldNames).build();
        }

        inputRels[0] = leftRel;
        inputRels[1] = rightRel;
    }

    @Deprecated // to be removed before 2.0
    public static RelNode createProjectJoinRel(List<Integer> outputProj, RelNode joinRel) {
        int newProjectOutputSize = outputProj.size();
        List<RelDataTypeField> joinOutputFields = joinRel.getRowType().getFieldList();

        // If no projection was passed in, or the number of desired projection
        // columns is the same as the number of columns returned from the
        // join, then no need to create a projection
        if (newProjectOutputSize > 0 && newProjectOutputSize < joinOutputFields.size()) {
            final PairList<RexNode, String> newProjects = PairList.of();
            final RelBuilder relBuilder =
                    RelFactories.LOGICAL_BUILDER.create(joinRel.getCluster(), null);
            final RexBuilder rexBuilder = relBuilder.getRexBuilder();
            for (int fieldIndex : outputProj) {
                final RelDataTypeField field = joinOutputFields.get(fieldIndex);
                newProjects.add(
                        rexBuilder.makeInputRef(field.getType(), fieldIndex), field.getName());
            }

            // Create a project rel on the output of the join.
            return relBuilder
                    .push(joinRel)
                    .project(newProjects.leftList(), newProjects.rightList(), true)
                    .build();
        }

        return joinRel;
    }

    @Deprecated // to be removed before 2.0
    public static void registerAbstractRels(RelOptPlanner planner) {
        registerAbstractRules(planner);
    }

    @Experimental
    public static void registerAbstractRules(RelOptPlanner planner) {
        RelOptRules.ABSTRACT_RULES.forEach(planner::addRule);
    }

    @Experimental
    public static void registerAbstractRelationalRules(RelOptPlanner planner) {
        RelOptRules.ABSTRACT_RELATIONAL_RULES.forEach(planner::addRule);
        if (CalciteSystemProperty.COMMUTE.value()) {
            planner.addRule(CoreRules.JOIN_ASSOCIATE);
        }
        // todo: rule which makes Project({OrdinalRef}) disappear
    }

    private static void registerEnumerableRules(RelOptPlanner planner) {
        EnumerableRules.ENUMERABLE_RULES.forEach(planner::addRule);
    }

    private static void registerBaseRules(RelOptPlanner planner) {
        RelOptRules.BASE_RULES.forEach(planner::addRule);
    }

    @SuppressWarnings("unused")
    private static void registerReductionRules(RelOptPlanner planner) {
        RelOptRules.CONSTANT_REDUCTION_RULES.forEach(planner::addRule);
    }

    private static void registerMaterializationRules(RelOptPlanner planner) {
        RelOptRules.MATERIALIZATION_RULES.forEach(planner::addRule);
    }

    @SuppressWarnings("unused")
    private static void registerCalcRules(RelOptPlanner planner) {
        RelOptRules.CALC_RULES.forEach(planner::addRule);
    }

    @Experimental
    public static void registerDefaultRules(
            RelOptPlanner planner, boolean enableMaterializations, boolean enableBindable) {
        if (CalciteSystemProperty.ENABLE_COLLATION_TRAIT.value()) {
            registerAbstractRelationalRules(planner);
        }
        registerAbstractRules(planner);
        registerBaseRules(planner);

        if (enableMaterializations) {
            registerMaterializationRules(planner);
        }
        if (enableBindable) {
            for (RelOptRule rule : Bindables.RULES) {
                planner.addRule(rule);
            }
        }
        // Registers this rule for default ENUMERABLE convention
        // because:
        // 1. ScannableTable can bind data directly;
        // 2. Only BindableTable supports project push down now.

        // EnumerableInterpreterRule.INSTANCE would then transform
        // the BindableTableScan to
        // EnumerableInterpreter + BindableTableScan.

        // Note: the cost of EnumerableInterpreter + BindableTableScan
        // is always bigger that EnumerableTableScan because of the additional
        // EnumerableInterpreter node, but if there are pushing projects or filter,
        // we prefer BindableTableScan instead,
        // see BindableTableScan#computeSelfCost.
        planner.addRule(Bindables.BINDABLE_TABLE_SCAN_RULE);
        planner.addRule(CoreRules.PROJECT_TABLE_SCAN);
        planner.addRule(CoreRules.PROJECT_INTERPRETER_TABLE_SCAN);

        if (CalciteSystemProperty.ENABLE_ENUMERABLE.value()) {
            registerEnumerableRules(planner);
            planner.addRule(EnumerableRules.TO_INTERPRETER);
        }

        if (enableBindable && CalciteSystemProperty.ENABLE_ENUMERABLE.value()) {
            planner.addRule(EnumerableRules.TO_BINDABLE);
        }

        if (CalciteSystemProperty.ENABLE_STREAM.value()) {
            for (RelOptRule rule : StreamRules.RULES) {
                planner.addRule(rule);
            }
        }

        planner.addRule(CoreRules.FILTER_REDUCE_EXPRESSIONS);
    }

    /**
     * Dumps a plan as a string.
     *
     * @param header Header to print before the plan. Ignored if the format is XML
     * @param rel Relational expression to explain
     * @param format Output format
     * @param detailLevel Detail level
     * @return Plan
     */
    public static String dumpPlan(
            String header, RelNode rel, SqlExplainFormat format, SqlExplainLevel detailLevel) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        if (!header.isEmpty()) {
            pw.println(header);
        }
        RelWriter planWriter;
        switch (format) {
            case XML:
                planWriter = new RelXmlWriter(pw, detailLevel);
                break;
            case JSON:
                planWriter = new RelJsonWriter();
                rel.explain(planWriter);
                return ((RelJsonWriter) planWriter).asString();
            case DOT:
                planWriter = new RelDotWriter(pw, detailLevel, false);
                break;
            default:
                planWriter = new RelWriterImpl(pw, detailLevel, false);
        }
        rel.explain(planWriter);
        pw.flush();
        return sw.toString();
    }

    @Deprecated // to be removed before 2.0
    public static String dumpPlan(
            String header, RelNode rel, boolean asXml, SqlExplainLevel detailLevel) {
        return dumpPlan(
                header, rel, asXml ? SqlExplainFormat.XML : SqlExplainFormat.TEXT, detailLevel);
    }

    /**
     * Creates the row type descriptor for the result of a DML operation, which is a single column
     * named ROWCOUNT of type BIGINT for INSERT; a single column named PLAN for EXPLAIN.
     *
     * @param kind Kind of node
     * @param typeFactory factory to use for creating type descriptor
     * @return created type
     */
    public static RelDataType createDmlRowType(SqlKind kind, RelDataTypeFactory typeFactory) {
        switch (kind) {
            case INSERT:
            case DELETE:
            case UPDATE:
            case MERGE:
                return typeFactory.createStructType(
                        PairList.of(
                                AvaticaConnection.ROWCOUNT_COLUMN_NAME,
                                typeFactory.createSqlType(SqlTypeName.BIGINT)));
            case EXPLAIN:
                return typeFactory.createStructType(
                        PairList.of(
                                AvaticaConnection.PLAN_COLUMN_NAME,
                                typeFactory.createSqlType(
                                        SqlTypeName.VARCHAR, RelDataType.PRECISION_NOT_SPECIFIED)));
            default:
                throw Util.unexpected(kind);
        }
    }

    /**
     * Returns whether two types are equal using 'equals'.
     *
     * @param desc1 Description of first type
     * @param type1 First type
     * @param desc2 Description of second type
     * @param type2 Second type
     * @param litmus What to do if an error is detected (types are not equal)
     * @return Whether the types are equal
     */
    public static boolean eq(
            final String desc1,
            RelDataType type1,
            final String desc2,
            RelDataType type2,
            Litmus litmus) {
        // if any one of the types is ANY return true
        if (type1.getSqlTypeName() == SqlTypeName.ANY
                || type2.getSqlTypeName() == SqlTypeName.ANY) {
            return litmus.succeed();
        }

        if (!type1.equals(type2)) {
            return litmus.fail(
                    "type mismatch:\n{}:\n{}\n{}:\n{}",
                    desc1,
                    type1.getFullTypeString(),
                    desc2,
                    type2.getFullTypeString());
        }
        return litmus.succeed();
    }

    // ----- FLINK MODIFICATION BEGIN -----
    // Backport from Calcite (CALCITE-6764)
    public static boolean eqUpToNullability(
            boolean ignoreNullability,
            final String desc1,
            RelDataType type1,
            final String desc2,
            RelDataType type2,
            Litmus litmus) {
        if (type1.getSqlTypeName() == SqlTypeName.ANY
                || type2.getSqlTypeName() == SqlTypeName.ANY) {
            return litmus.succeed();
        }

        boolean success;
        if (ignoreNullability) {
            success = SqlTypeUtil.equalSansNullability(type1, type2);
        } else {
            success = type1.equals(type2);
        }

        if (!success) {
            return litmus.fail(
                    "type mismatch:\n{}:\n{}\n{}:\n{}",
                    desc1,
                    type1.getFullTypeString(),
                    desc2,
                    type2.getFullTypeString());
        }
        return litmus.succeed();
    }

    // ----- FLINK MODIFICATION END -----

    /**
     * Returns whether two types are equal using {@link #areRowTypesEqual(RelDataType, RelDataType,
     * boolean)}. Both types must not be null.
     *
     * @param desc1 Description of role of first type
     * @param type1 First type
     * @param desc2 Description of role of second type
     * @param type2 Second type
     * @param litmus Whether to assert if they are not equal
     * @return Whether the types are equal
     */
    public static boolean equal(
            final String desc1,
            RelDataType type1,
            final String desc2,
            RelDataType type2,
            Litmus litmus) {
        if (!areRowTypesEqual(type1, type2, false)) {
            return litmus.fail(getFullTypeDifferenceString(desc1, type1, desc2, type2));
        }
        return litmus.succeed();
    }

    /**
     * Returns the detailed difference of two types.
     *
     * @param sourceDesc description of role of source type
     * @param sourceType source type
     * @param targetDesc description of role of target type
     * @param targetType target type
     * @return the detailed difference of two types
     */
    public static String getFullTypeDifferenceString(
            final String sourceDesc,
            RelDataType sourceType,
            final String targetDesc,
            RelDataType targetType) {
        if (sourceType == targetType) {
            return "";
        }

        final int sourceFieldCount = sourceType.getFieldCount();
        final int targetFieldCount = targetType.getFieldCount();
        if (sourceFieldCount != targetFieldCount) {
            return "Type mismatch: the field sizes are not equal.\n"
                    + sourceDesc
                    + ": "
                    + sourceType.getFullTypeString()
                    + "\n"
                    + targetDesc
                    + ": "
                    + targetType.getFullTypeString();
        }

        final StringBuilder stringBuilder = new StringBuilder();
        final List<RelDataTypeField> f1 = sourceType.getFieldList();
        final List<RelDataTypeField> f2 = targetType.getFieldList();
        for (Pair<RelDataTypeField, RelDataTypeField> pair : Pair.zip(f1, f2)) {
            final RelDataType t1 = pair.left.getType();
            final RelDataType t2 = pair.right.getType();
            // If one of the types is ANY comparison should succeed
            if (sourceType.getSqlTypeName() == SqlTypeName.ANY
                    || targetType.getSqlTypeName() == SqlTypeName.ANY) {
                continue;
            }
            if (!t1.equals(t2)) {
                stringBuilder.append(pair.left.getName());
                stringBuilder.append(": ");
                stringBuilder.append(t1.getFullTypeString());
                stringBuilder.append(" -> ");
                stringBuilder.append(t2.getFullTypeString());
                stringBuilder.append("\n");
            }
        }
        final String difference = stringBuilder.toString();
        if (!difference.isEmpty()) {
            return "Type mismatch:\n"
                    + sourceDesc
                    + ": "
                    + sourceType.getFullTypeString()
                    + "\n"
                    + targetDesc
                    + ": "
                    + targetType.getFullTypeString()
                    + "\n"
                    + "Difference:\n"
                    + difference;
        } else {
            return "";
        }
    }

    /** Returns whether two relational expressions have the same row-type. */
    public static boolean equalType(
            String desc0, RelNode rel0, String desc1, RelNode rel1, Litmus litmus) {
        // TODO: change 'equal' to 'eq', which is stronger.
        return equal(desc0, rel0.getRowType(), desc1, rel1.getRowType(), litmus);
    }

    /**
     * Returns a translation of the <code>IS DISTINCT FROM</code> (or <code>IS
     * NOT DISTINCT FROM</code>) sql operator.
     *
     * @param neg if false, returns a translation of IS NOT DISTINCT FROM
     */
    public static RexNode isDistinctFrom(RexBuilder rexBuilder, RexNode x, RexNode y, boolean neg) {
        RexNode ret = null;
        if (x.getType().isStruct()) {
            assert y.getType().isStruct();
            List<RelDataTypeField> xFields = x.getType().getFieldList();
            List<RelDataTypeField> yFields = y.getType().getFieldList();
            assert xFields.size() == yFields.size();
            for (Pair<RelDataTypeField, RelDataTypeField> pair : Pair.zip(xFields, yFields)) {
                RelDataTypeField xField = pair.left;
                RelDataTypeField yField = pair.right;
                RexNode newX = rexBuilder.makeFieldAccess(x, xField.getIndex());
                RexNode newY = rexBuilder.makeFieldAccess(y, yField.getIndex());
                RexNode newCall = isDistinctFromInternal(rexBuilder, newX, newY, neg);
                if (ret == null) {
                    ret = newCall;
                } else {
                    ret = rexBuilder.makeCall(SqlStdOperatorTable.AND, ret, newCall);
                }
            }
        } else {
            ret = isDistinctFromInternal(rexBuilder, x, y, neg);
        }

        // The result of IS DISTINCT FROM is NOT NULL because it can
        // only return TRUE or FALSE.
        requireNonNull(ret, "ret");
        checkArgument(!ret.getType().isNullable());

        return ret;
    }

    private static RexNode isDistinctFromInternal(
            RexBuilder rexBuilder, RexNode x, RexNode y, boolean neg) {

        if (neg) {
            // x is not distinct from y
            // x=y IS TRUE or ((x is null) and (y is null)),
            return rexBuilder.makeCall(
                    SqlStdOperatorTable.OR,
                    rexBuilder.makeCall(
                            SqlStdOperatorTable.AND,
                            rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL, x),
                            rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL, y)),
                    rexBuilder.makeCall(
                            SqlStdOperatorTable.IS_TRUE,
                            rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, x, y)));
        } else {
            // x is distinct from y
            // x=y IS NOT TRUE and ((x is not null) or (y is not null)),
            return rexBuilder.makeCall(
                    SqlStdOperatorTable.AND,
                    rexBuilder.makeCall(
                            SqlStdOperatorTable.OR,
                            rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, x),
                            rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, y)),
                    rexBuilder.makeCall(
                            SqlStdOperatorTable.IS_NOT_TRUE,
                            rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, x, y)));
        }
    }

    /** Converts a relational expression to a string, showing just basic attributes. */
    public static String toString(final RelNode rel) {
        return toString(rel, SqlExplainLevel.EXPPLAN_ATTRIBUTES);
    }

    /**
     * Converts a relational expression to a string; returns null if and only if {@code rel} is
     * null.
     */
    public static @PolyNull String toString(
            final @PolyNull RelNode rel, SqlExplainLevel detailLevel) {
        if (rel == null) {
            return null;
        }
        final StringWriter sw = new StringWriter();
        final RelWriter planWriter = new RelWriterImpl(new PrintWriter(sw), detailLevel, false);
        rel.explain(planWriter);
        return sw.toString();
    }

    @Deprecated // to be removed before 2.0
    public static RelNode renameIfNecessary(RelNode rel, RelDataType desiredRowType) {
        final RelDataType rowType = rel.getRowType();
        if (rowType == desiredRowType) {
            // Nothing to do.
            return rel;
        }
        assert !rowType.equals(desiredRowType);

        if (!areRowTypesEqual(rowType, desiredRowType, false)) {
            // The row types are different ignoring names. Nothing we can do.
            return rel;
        }
        rel = createRename(rel, desiredRowType.getFieldNames());
        return rel;
    }

    public static String dumpType(RelDataType type) {
        final StringWriter sw = new StringWriter();
        final PrintWriter pw = new PrintWriter(sw);
        final TypeDumper typeDumper = new TypeDumper(pw);
        if (type.isStruct()) {
            typeDumper.acceptFields(type.getFieldList());
        } else {
            typeDumper.accept(type);
        }
        pw.flush();
        return sw.toString();
    }

    /**
     * Returns the set of columns with unique names, with prior columns taking precedence over
     * columns that appear later in the list.
     */
    public static List<RelDataTypeField> deduplicateColumns(
            List<RelDataTypeField> baseColumns, List<RelDataTypeField> extendedColumns) {
        final Set<String> dedupedFieldNames = new HashSet<>();
        final ImmutableList.Builder<RelDataTypeField> dedupedFields = ImmutableList.builder();
        for (RelDataTypeField f : Iterables.concat(baseColumns, extendedColumns)) {
            if (dedupedFieldNames.add(f.getName())) {
                dedupedFields.add(f);
            }
        }
        return dedupedFields.build();
    }

    /**
     * Decomposes a predicate into a list of expressions that are AND'ed together.
     *
     * @param rexPredicate predicate to be analyzed
     * @param rexList list of decomposed RexNodes
     */
    public static void decomposeConjunction(@Nullable RexNode rexPredicate, List<RexNode> rexList) {
        if (rexPredicate == null || rexPredicate.isAlwaysTrue()) {
            return;
        }
        if (rexPredicate.isA(SqlKind.AND)) {
            for (RexNode operand : ((RexCall) rexPredicate).getOperands()) {
                decomposeConjunction(operand, rexList);
            }
        } else {
            rexList.add(rexPredicate);
        }
    }

    /**
     * Decomposes a predicate into a list of expressions that are AND'ed together, and a list of
     * expressions that are preceded by NOT.
     *
     * <p>For example, {@code a AND NOT b AND NOT (c and d) AND TRUE AND NOT FALSE} returns {@code
     * rexList = [a], notList = [b, c AND d]}.
     *
     * <p>TRUE and NOT FALSE expressions are ignored. FALSE and NOT TRUE expressions are placed on
     * {@code rexList} and {@code notList} as other expressions.
     *
     * <p>For example, {@code a AND TRUE AND NOT TRUE} returns {@code rexList = [a], notList =
     * [TRUE]}.
     *
     * @param rexPredicate predicate to be analyzed
     * @param rexList list of decomposed RexNodes (except those with NOT)
     * @param notList list of decomposed RexNodes that were prefixed NOT
     */
    public static void decomposeConjunction(
            @Nullable RexNode rexPredicate, List<RexNode> rexList, List<RexNode> notList) {
        if (rexPredicate == null || rexPredicate.isAlwaysTrue()) {
            return;
        }
        switch (rexPredicate.getKind()) {
            case AND:
                for (RexNode operand : ((RexCall) rexPredicate).getOperands()) {
                    decomposeConjunction(operand, rexList, notList);
                }
                break;
            case NOT:
                final RexNode e = ((RexCall) rexPredicate).getOperands().get(0);
                if (e.isAlwaysFalse()) {
                    return;
                }
                switch (e.getKind()) {
                    case OR:
                        final List<RexNode> ors = new ArrayList<>();
                        decomposeDisjunction(e, ors);
                        for (RexNode or : ors) {
                            switch (or.getKind()) {
                                case NOT:
                                    rexList.add(((RexCall) or).operands.get(0));
                                    break;
                                default:
                                    notList.add(or);
                            }
                        }
                        break;
                    default:
                        notList.add(e);
                }
                break;
            case LITERAL:
                if (!RexLiteral.isNullLiteral(rexPredicate)
                        && RexLiteral.booleanValue(rexPredicate)) {
                    return; // ignore TRUE
                }
            // fall through
            default:
                rexList.add(rexPredicate);
                break;
        }
    }

    /**
     * Decomposes a predicate into a list of expressions that are OR'ed together.
     *
     * @param rexPredicate predicate to be analyzed
     * @param rexList list of decomposed RexNodes
     */
    public static void decomposeDisjunction(@Nullable RexNode rexPredicate, List<RexNode> rexList) {
        if (rexPredicate == null || rexPredicate.isAlwaysFalse()) {
            return;
        }
        if (rexPredicate.isA(SqlKind.OR)) {
            for (RexNode operand : ((RexCall) rexPredicate).getOperands()) {
                decomposeDisjunction(operand, rexList);
            }
        } else {
            rexList.add(rexPredicate);
        }
    }

    /**
     * Returns a condition decomposed by AND.
     *
     * <p>For example, {@code conjunctions(TRUE)} returns the empty list; {@code
     * conjunctions(FALSE)} returns list {@code {FALSE}}.
     */
    public static List<RexNode> conjunctions(@Nullable RexNode rexPredicate) {
        final List<RexNode> list = new ArrayList<>();
        decomposeConjunction(rexPredicate, list);
        return list;
    }

    /**
     * Returns a condition decomposed by OR.
     *
     * <p>For example, {@code disjunctions(FALSE)} returns the empty list.
     */
    public static List<RexNode> disjunctions(RexNode rexPredicate) {
        final List<RexNode> list = new ArrayList<>();
        decomposeDisjunction(rexPredicate, list);
        return list;
    }

    /**
     * Ands two sets of join filters together, either of which can be null.
     *
     * @param rexBuilder rexBuilder to create AND expression
     * @param left filter on the left that the right will be AND'd to
     * @param right filter on the right
     * @return AND'd filter
     * @see org.apache.calcite.rex.RexUtil#composeConjunction
     */
    public static RexNode andJoinFilters(
            RexBuilder rexBuilder, @Nullable RexNode left, @Nullable RexNode right) {
        // don't bother AND'ing in expressions that always evaluate to
        // true
        if ((left != null) && !left.isAlwaysTrue()) {
            if ((right != null) && !right.isAlwaysTrue()) {
                left = rexBuilder.makeCall(SqlStdOperatorTable.AND, left, right);
            }
        } else {
            left = right;
        }

        // Joins must have some filter
        if (left == null) {
            left = rexBuilder.makeLiteral(true);
        }
        return left;
    }

    /**
     * Decomposes the WHERE clause of a view into predicates that constraint a column to a
     * particular value.
     *
     * <p>This method is key to the validation of a modifiable view. Columns that are constrained to
     * a single value can be omitted from the SELECT clause of a modifiable view.
     *
     * @param projectMap Mapping from column ordinal to the expression that populate that column, to
     *     be populated by this method
     * @param filters List of remaining filters, to be populated by this method
     * @param constraint Constraint to be analyzed
     */
    public static void inferViewPredicates(
            Map<Integer, RexNode> projectMap, List<RexNode> filters, RexNode constraint) {
        for (RexNode node : conjunctions(constraint)) {
            switch (node.getKind()) {
                case EQUALS:
                    final List<RexNode> operands = ((RexCall) node).getOperands();
                    RexNode o0 = operands.get(0);
                    RexNode o1 = operands.get(1);
                    if (o0 instanceof RexLiteral) {
                        o0 = operands.get(1);
                        o1 = operands.get(0);
                    }
                    if (o0.getKind() == SqlKind.CAST) {
                        o0 = ((RexCall) o0).getOperands().get(0);
                    }
                    if (o0 instanceof RexInputRef && o1 instanceof RexLiteral) {
                        final int index = ((RexInputRef) o0).getIndex();
                        if (projectMap.get(index) == null) {
                            projectMap.put(index, o1);
                            continue;
                        }
                    }
                    break;
                default:
                    break;
            }
            filters.add(node);
        }
    }

    /**
     * Returns a mapping of the column ordinal in the underlying table to a column constraint of the
     * modifiable view.
     *
     * @param modifiableViewTable The modifiable view which has a constraint
     * @param targetRowType The target type
     */
    public static Map<Integer, RexNode> getColumnConstraints(
            ModifiableView modifiableViewTable,
            RelDataType targetRowType,
            RelDataTypeFactory typeFactory) {
        final RexBuilder rexBuilder = new RexBuilder(typeFactory);
        final RexNode constraint = modifiableViewTable.getConstraint(rexBuilder, targetRowType);
        final Map<Integer, RexNode> projectMap = new HashMap<>();
        final List<RexNode> filters = new ArrayList<>();
        RelOptUtil.inferViewPredicates(projectMap, filters, constraint);
        assert filters.isEmpty();
        return projectMap;
    }

    /**
     * Ensures that a source value does not violate the constraint of the target column.
     *
     * @param sourceValue The insert value being validated
     * @param targetConstraint The constraint applied to sourceValue for validation
     * @param errorSupplier The function to apply when validation fails
     */
    public static void validateValueAgainstConstraint(
            SqlNode sourceValue,
            RexNode targetConstraint,
            Supplier<CalciteContextException> errorSupplier) {
        if (!(sourceValue instanceof SqlLiteral)) {
            // We cannot guarantee that the value satisfies the constraint.
            throw errorSupplier.get();
        }
        final SqlLiteral insertValue = (SqlLiteral) sourceValue;
        final RexLiteral columnConstraint = (RexLiteral) targetConstraint;

        final RexSqlStandardConvertletTable convertletTable = new RexSqlStandardConvertletTable();
        final RexToSqlNodeConverter sqlNodeToRexConverter =
                new RexToSqlNodeConverterImpl(convertletTable);
        final SqlLiteral constraintValue =
                (SqlLiteral) sqlNodeToRexConverter.convertLiteral(columnConstraint);

        if (!insertValue.equals(constraintValue)) {
            // The value does not satisfy the constraint.
            throw errorSupplier.get();
        }
    }

    /**
     * Adjusts key values in a list by some fixed amount.
     *
     * @param keys list of key values
     * @param adjustment the amount to adjust the key values by
     * @return modified list
     */
    public static List<Integer> adjustKeys(List<Integer> keys, int adjustment) {
        if (adjustment == 0) {
            return keys;
        }
        final List<Integer> newKeys = new ArrayList<>();
        for (int key : keys) {
            newKeys.add(key + adjustment);
        }
        return newKeys;
    }

    /**
     * Simplifies outer joins if filter above would reject nulls.
     *
     * @param joinRel Join
     * @param aboveFilters Filters from above
     * @param joinType Join type, can not be inner join
     */
    public static JoinRelType simplifyJoin(
            RelNode joinRel, ImmutableList<RexNode> aboveFilters, JoinRelType joinType) {
        // No need to simplify if the join only outputs left side.
        if (!joinType.projectsRight()) {
            return joinType;
        }
        final int nTotalFields = joinRel.getRowType().getFieldCount();
        final int nSysFields = 0;
        final int nFieldsLeft = joinRel.getInputs().get(0).getRowType().getFieldCount();
        final int nFieldsRight = joinRel.getInputs().get(1).getRowType().getFieldCount();
        assert nTotalFields == nSysFields + nFieldsLeft + nFieldsRight;

        // set the reference bitmaps for the left and right children
        ImmutableBitSet leftBitmap = ImmutableBitSet.range(nSysFields, nSysFields + nFieldsLeft);
        ImmutableBitSet rightBitmap = ImmutableBitSet.range(nSysFields + nFieldsLeft, nTotalFields);

        for (RexNode filter : aboveFilters) {
            if (joinType.generatesNullsOnLeft() && Strong.isNotTrue(filter, leftBitmap)) {
                joinType = joinType.cancelNullsOnLeft();
            }
            if (joinType.generatesNullsOnRight() && Strong.isNotTrue(filter, rightBitmap)) {
                joinType = joinType.cancelNullsOnRight();
            }
            if (!joinType.isOuterJoin()) {
                break;
            }
        }
        return joinType;
    }

    /**
     * Classifies filters according to where they should be processed. They either stay where they
     * are, are pushed to the join (if they originated from above the join), or are pushed to one of
     * the children. Filters that are pushed are added to list passed in as input parameters.
     *
     * @param joinRel join node
     * @param filters filters to be classified
     * @param pushInto whether filters can be pushed into the join
     * @param pushLeft true if filters can be pushed to the left
     * @param pushRight true if filters can be pushed to the right
     * @param joinFilters list of filters to push to the join
     * @param leftFilters list of filters to push to the left child
     * @param rightFilters list of filters to push to the right child
     * @return whether at least one filter was pushed
     */
    public static boolean classifyFilters(
            RelNode joinRel,
            List<RexNode> filters,
            boolean pushInto,
            boolean pushLeft,
            boolean pushRight,
            List<RexNode> joinFilters,
            List<RexNode> leftFilters,
            List<RexNode> rightFilters) {
        RexBuilder rexBuilder = joinRel.getCluster().getRexBuilder();
        List<RelDataTypeField> joinFields = joinRel.getRowType().getFieldList();
        final int nSysFields = 0; // joinRel.getSystemFieldList().size();
        final List<RelDataTypeField> leftFields =
                joinRel.getInputs().get(0).getRowType().getFieldList();
        final int nFieldsLeft = leftFields.size();
        final List<RelDataTypeField> rightFields =
                joinRel.getInputs().get(1).getRowType().getFieldList();
        final int nFieldsRight = rightFields.size();
        final int nTotalFields = nFieldsLeft + nFieldsRight;

        // set the reference bitmaps for the left and right children
        ImmutableBitSet leftBitmap = ImmutableBitSet.range(nSysFields, nSysFields + nFieldsLeft);
        ImmutableBitSet rightBitmap = ImmutableBitSet.range(nSysFields + nFieldsLeft, nTotalFields);

        final List<RexNode> filtersToRemove = new ArrayList<>();
        for (RexNode filter : filters) {
            final InputFinder inputFinder = InputFinder.analyze(filter);
            final ImmutableBitSet inputBits = inputFinder.build();

            // REVIEW - are there any expressions that need special handling
            // and therefore cannot be pushed?

            if (pushLeft && leftBitmap.contains(inputBits)) {
                // ignore filters that always evaluate to true
                if (!filter.isAlwaysTrue()) {
                    // adjust the field references in the filter to reflect
                    // that fields in the left now shift over by the number
                    // of system fields
                    final RexNode shiftedFilter =
                            shiftFilter(
                                    nSysFields,
                                    nSysFields + nFieldsLeft,
                                    -nSysFields,
                                    rexBuilder,
                                    joinFields,
                                    nTotalFields,
                                    leftFields,
                                    filter);

                    leftFilters.add(shiftedFilter);
                }
                filtersToRemove.add(filter);
            } else if (pushRight && rightBitmap.contains(inputBits)) {
                if (!filter.isAlwaysTrue()) {
                    // adjust the field references in the filter to reflect
                    // that fields in the right now shift over to the left
                    final RexNode shiftedFilter =
                            shiftFilter(
                                    nSysFields + nFieldsLeft,
                                    nTotalFields,
                                    -(nSysFields + nFieldsLeft),
                                    rexBuilder,
                                    joinFields,
                                    nTotalFields,
                                    rightFields,
                                    filter);
                    rightFilters.add(shiftedFilter);
                }
                filtersToRemove.add(filter);

            } else {
                // If the filter can't be pushed to either child, we may push them into the join
                if (pushInto) {
                    if (!joinFilters.contains(filter)) {
                        joinFilters.add(filter);
                    }
                    filtersToRemove.add(filter);
                }
            }
        }

        // Remove filters after the loop, to prevent concurrent modification.
        if (!filtersToRemove.isEmpty()) {
            filters.removeAll(filtersToRemove);
        }

        // Did anything change?
        return !filtersToRemove.isEmpty();
    }

    /**
     * Classifies filters according to where they should be processed. They either stay where they
     * are, are pushed to the join (if they originated from above the join), or are pushed to one of
     * the children. Filters that are pushed are added to list passed in as input parameters.
     *
     * @param joinRel join node
     * @param filters filters to be classified
     * @param joinType join type
     * @param pushInto whether filters can be pushed into the ON clause
     * @param pushLeft true if filters can be pushed to the left
     * @param pushRight true if filters can be pushed to the right
     * @param joinFilters list of filters to push to the join
     * @param leftFilters list of filters to push to the left child
     * @param rightFilters list of filters to push to the right child
     * @return whether at least one filter was pushed
     * @deprecated Use {@link RelOptUtil#classifyFilters(RelNode, List, boolean, boolean, boolean,
     *     List, List, List)}
     */
    @Deprecated // to be removed before 2.0
    public static boolean classifyFilters(
            RelNode joinRel,
            List<RexNode> filters,
            JoinRelType joinType,
            boolean pushInto,
            boolean pushLeft,
            boolean pushRight,
            List<RexNode> joinFilters,
            List<RexNode> leftFilters,
            List<RexNode> rightFilters) {
        RexBuilder rexBuilder = joinRel.getCluster().getRexBuilder();
        List<RelDataTypeField> joinFields = joinRel.getRowType().getFieldList();
        final int nTotalFields = joinFields.size();
        final int nSysFields = 0; // joinRel.getSystemFieldList().size();
        final List<RelDataTypeField> leftFields =
                joinRel.getInputs().get(0).getRowType().getFieldList();
        final int nFieldsLeft = leftFields.size();
        final List<RelDataTypeField> rightFields =
                joinRel.getInputs().get(1).getRowType().getFieldList();
        final int nFieldsRight = rightFields.size();

        // SemiJoin, CorrelateSemiJoin, CorrelateAntiJoin: right fields are not returned
        assert nTotalFields
                == (!joinType.projectsRight()
                        ? nSysFields + nFieldsLeft
                        : nSysFields + nFieldsLeft + nFieldsRight);

        // set the reference bitmaps for the left and right children
        ImmutableBitSet leftBitmap = ImmutableBitSet.range(nSysFields, nSysFields + nFieldsLeft);
        ImmutableBitSet rightBitmap = ImmutableBitSet.range(nSysFields + nFieldsLeft, nTotalFields);

        final List<RexNode> filtersToRemove = new ArrayList<>();
        for (RexNode filter : filters) {
            final InputFinder inputFinder = InputFinder.analyze(filter);
            final ImmutableBitSet inputBits = inputFinder.build();

            // REVIEW - are there any expressions that need special handling
            // and therefore cannot be pushed?

            // filters can be pushed to the left child if the left child
            // does not generate NULLs and the only columns referenced in
            // the filter originate from the left child
            if (pushLeft && leftBitmap.contains(inputBits)) {
                // ignore filters that always evaluate to true
                if (!filter.isAlwaysTrue()) {
                    // adjust the field references in the filter to reflect
                    // that fields in the left now shift over by the number
                    // of system fields
                    final RexNode shiftedFilter =
                            shiftFilter(
                                    nSysFields,
                                    nSysFields + nFieldsLeft,
                                    -nSysFields,
                                    rexBuilder,
                                    joinFields,
                                    nTotalFields,
                                    leftFields,
                                    filter);

                    leftFilters.add(shiftedFilter);
                }
                filtersToRemove.add(filter);

                // filters can be pushed to the right child if the right child
                // does not generate NULLs and the only columns referenced in
                // the filter originate from the right child
            } else if (pushRight && rightBitmap.contains(inputBits)) {
                if (!filter.isAlwaysTrue()) {
                    // adjust the field references in the filter to reflect
                    // that fields in the right now shift over to the left;
                    // since we never push filters to a NULL generating
                    // child, the types of the source should match the dest
                    // so we don't need to explicitly pass the destination
                    // fields to RexInputConverter
                    final RexNode shiftedFilter =
                            shiftFilter(
                                    nSysFields + nFieldsLeft,
                                    nTotalFields,
                                    -(nSysFields + nFieldsLeft),
                                    rexBuilder,
                                    joinFields,
                                    nTotalFields,
                                    rightFields,
                                    filter);
                    rightFilters.add(shiftedFilter);
                }
                filtersToRemove.add(filter);

            } else {
                // If the filter can't be pushed to either child and the join
                // is an inner join, push them to the join if they originated
                // from above the join
                if (!joinType.isOuterJoin() && pushInto) {
                    if (!joinFilters.contains(filter)) {
                        joinFilters.add(filter);
                    }
                    filtersToRemove.add(filter);
                }
            }
        }

        // Remove filters after the loop, to prevent concurrent modification.
        if (!filtersToRemove.isEmpty()) {
            filters.removeAll(filtersToRemove);
        }

        // Did anything change?
        return !filtersToRemove.isEmpty();
    }

    private static RexNode shiftFilter(
            int start,
            int end,
            int offset,
            RexBuilder rexBuilder,
            List<RelDataTypeField> joinFields,
            int nTotalFields,
            List<RelDataTypeField> rightFields,
            RexNode filter) {
        int[] adjustments = new int[nTotalFields];
        for (int i = start; i < end; i++) {
            adjustments[i] = offset;
        }
        return filter.accept(
                new RexInputConverter(rexBuilder, joinFields, rightFields, adjustments));
    }

    /**
     * Splits a filter into two lists, depending on whether or not the filter only references its
     * child input.
     *
     * @param childBitmap Fields in the child
     * @param predicate filters that will be split
     * @param pushable returns the list of filters that can be pushed to the child input
     * @param notPushable returns the list of filters that cannot be pushed to the child input
     */
    public static void splitFilters(
            ImmutableBitSet childBitmap,
            @Nullable RexNode predicate,
            List<RexNode> pushable,
            List<RexNode> notPushable) {
        // for each filter, if the filter only references the child inputs,
        // then it can be pushed
        for (RexNode filter : conjunctions(predicate)) {
            ImmutableBitSet filterRefs = InputFinder.bits(filter);
            if (childBitmap.contains(filterRefs)) {
                pushable.add(filter);
            } else {
                notPushable.add(filter);
            }
        }
    }

    @Deprecated // to be removed before 2.0
    public static boolean checkProjAndChildInputs(Project project, boolean checkNames) {
        int n = project.getProjects().size();
        RelDataType inputType = project.getInput().getRowType();
        if (inputType.getFieldList().size() != n) {
            return false;
        }
        List<RelDataTypeField> projFields = project.getRowType().getFieldList();
        List<RelDataTypeField> inputFields = inputType.getFieldList();
        boolean namesDifferent = false;
        for (int i = 0; i < n; ++i) {
            RexNode exp = project.getProjects().get(i);
            if (!(exp instanceof RexInputRef)) {
                return false;
            }
            RexInputRef fieldAccess = (RexInputRef) exp;
            if (i != fieldAccess.getIndex()) {
                // can't support reorder yet
                return false;
            }
            if (checkNames) {
                String inputFieldName = inputFields.get(i).getName();
                String projFieldName = projFields.get(i).getName();
                if (!projFieldName.equals(inputFieldName)) {
                    namesDifferent = true;
                }
            }
        }

        // inputs are the same; return value depends on the checkNames
        // parameter
        return !checkNames || namesDifferent;
    }

    /**
     * Creates projection expressions reflecting the swapping of a join's input.
     *
     * @param newJoin the RelNode corresponding to the join with its inputs swapped
     * @param origJoin original LogicalJoin
     * @param origOrder if true, create the projection expressions to reflect the original
     *     (pre-swapped) join projection; otherwise, create the projection to reflect the order of
     *     the swapped projection
     * @return array of expression representing the swapped join inputs
     */
    public static List<RexNode> createSwappedJoinExprs(
            RelNode newJoin, Join origJoin, boolean origOrder) {
        final List<RelDataTypeField> newJoinFields = newJoin.getRowType().getFieldList();
        final RexBuilder rexBuilder = newJoin.getCluster().getRexBuilder();
        final List<RexNode> exps = new ArrayList<>();
        final int nFields =
                origOrder
                        ? origJoin.getRight().getRowType().getFieldCount()
                        : origJoin.getLeft().getRowType().getFieldCount();
        for (int i = 0; i < newJoinFields.size(); i++) {
            final int source = (i + nFields) % newJoinFields.size();
            RelDataTypeField field = origOrder ? newJoinFields.get(source) : newJoinFields.get(i);
            exps.add(rexBuilder.makeInputRef(field.getType(), source));
        }
        return exps;
    }

    @Deprecated // to be removed before 2.0
    public static RexNode pushFilterPastProject(RexNode filter, final Project projRel) {
        return pushPastProject(filter, projRel);
    }

    /**
     * Converts an expression that is based on the output fields of a {@link Project} to an
     * equivalent expression on the Project's input fields.
     *
     * @param node The expression to be converted
     * @param project Project underneath the expression
     * @return converted expression
     */
    public static RexNode pushPastProject(RexNode node, Project project) {
        return node.accept(pushShuttle(project));
    }

    /**
     * Converts a list of expressions that are based on the output fields of a {@link Project} to
     * equivalent expressions on the Project's input fields.
     *
     * @param nodes The expressions to be converted
     * @param project Project underneath the expression
     * @return converted expressions
     */
    public static List<RexNode> pushPastProject(List<? extends RexNode> nodes, Project project) {
        return pushShuttle(project).visitList(nodes);
    }

    public static @Nullable RexNode pushPastProjectUnlessBloat(
            RexNode node, Project project, int bloat) {
        List<RexNode> newConditions =
                pushPastProjectUnlessBloat(Collections.singletonList(node), project, bloat);
        if (newConditions == null || newConditions.size() != 1) {
            return null;
        }
        return newConditions.get(0);
    }

    /**
     * As {@link #pushPastProject}, but returns null if the resulting expressions are significantly
     * more complex.
     *
     * @param bloat Maximum allowable increase in complexity
     */
    public static @Nullable List<RexNode> pushPastProjectUnlessBloat(
            List<? extends RexNode> nodes, Project project, int bloat) {
        if (bloat < 0) {
            // If bloat is negative never merge.
            return null;
        }
        if (RexOver.containsOver(nodes, null) && project.containsOver()) {
            // Is it valid relational algebra to apply windowed function to a windowed
            // function? Possibly. But it's invalid SQL, so don't go there.
            return null;
        }
        final List<RexNode> list = pushPastProject(nodes, project);
        final int bottomCount = RexUtil.nodeCount(project.getProjects());
        final int topCount = RexUtil.nodeCount(nodes);
        final int mergedCount = RexUtil.nodeCount(list);
        if (mergedCount > bottomCount + topCount + bloat) {
            // The merged expression is more complex than the input expressions.
            // Do not merge.
            return null;
        }
        return list;
    }

    private static RexShuttle pushShuttle(final Project project) {
        return new RexShuttle() {
            @Override
            public RexNode visitInputRef(RexInputRef ref) {
                return project.getProjects().get(ref.getIndex());
            }
        };
    }

    /**
     * Converts an expression that is based on the output fields of a {@link Calc} to an equivalent
     * expression on the Calc's input fields.
     *
     * @param node The expression to be converted
     * @param calc Calc underneath the expression
     * @return converted expression
     */
    public static RexNode pushPastCalc(RexNode node, Calc calc) {
        return node.accept(pushShuttle(calc));
    }

    private static RexShuttle pushShuttle(final Calc calc) {
        final List<RexNode> projects =
                Util.transform(
                        calc.getProgram().getProjectList(), calc.getProgram()::expandLocalRef);
        return new RexShuttle() {
            @Override
            public RexNode visitInputRef(RexInputRef ref) {
                return projects.get(ref.getIndex());
            }
        };
    }

    /**
     * Creates a new {@link org.apache.calcite.rel.rules.MultiJoin} to reflect projection references
     * from a {@link Project} that is on top of the {@link org.apache.calcite.rel.rules.MultiJoin}.
     *
     * @param multiJoin the original MultiJoin
     * @param project the Project on top of the MultiJoin
     * @return the new MultiJoin
     */
    public static MultiJoin projectMultiJoin(MultiJoin multiJoin, Project project) {
        // Locate all input references in the projection expressions as well
        // the post-join filter.  Since the filter effectively sits in
        // between the LogicalProject and the MultiJoin, the projection needs
        // to include those filter references.
        ImmutableBitSet inputRefs =
                InputFinder.bits(project.getProjects(), multiJoin.getPostJoinFilter());

        // create new copies of the bitmaps
        List<RelNode> multiJoinInputs = multiJoin.getInputs();
        List<BitSet> newProjFields = new ArrayList<>();
        for (RelNode multiJoinInput : multiJoinInputs) {
            newProjFields.add(new BitSet(multiJoinInput.getRowType().getFieldCount()));
        }

        // set the bits found in the expressions
        int currInput = -1;
        int startField = 0;
        int nFields = 0;
        for (int bit : inputRefs) {
            while (bit >= (startField + nFields)) {
                startField += nFields;
                currInput++;
                assert currInput < multiJoinInputs.size();
                nFields = multiJoinInputs.get(currInput).getRowType().getFieldCount();
            }
            newProjFields.get(currInput).set(bit - startField);
        }

        // create a new MultiJoin containing the new field bitmaps
        // for each input
        return new MultiJoin(
                multiJoin.getCluster(),
                multiJoin.getInputs(),
                multiJoin.getJoinFilter(),
                multiJoin.getRowType(),
                multiJoin.isFullOuterJoin(),
                multiJoin.getOuterJoinConditions(),
                multiJoin.getJoinTypes(),
                Util.transform(newProjFields, ImmutableBitSet::fromBitSet),
                multiJoin.getJoinFieldRefCountsMap(),
                multiJoin.getPostJoinFilter());
    }

    public static <T extends RelNode> T addTrait(T rel, RelTrait trait) {
        //noinspection unchecked
        return (T) rel.copy(rel.getTraitSet().replace(trait), rel.getInputs());
    }

    /** Returns a shallow copy of a relational expression with a particular input replaced. */
    public static RelNode replaceInput(RelNode parent, int ordinal, RelNode newInput) {
        final List<RelNode> inputs = new ArrayList<>(parent.getInputs());
        if (inputs.get(ordinal) == newInput) {
            return parent;
        }
        inputs.set(ordinal, newInput);
        return parent.copy(parent.getTraitSet(), inputs);
    }

    /**
     * Creates a {@link org.apache.calcite.rel.logical.LogicalProject} that projects particular
     * fields of its input, according to a mapping.
     */
    public static RelNode createProject(RelNode child, Mappings.TargetMapping mapping) {
        return createProject(child, Mappings.asListNonNull(mapping.inverse()));
    }

    public static RelNode createProject(
            RelNode child,
            Mappings.TargetMapping mapping,
            RelFactories.ProjectFactory projectFactory) {
        return createProject(projectFactory, child, Mappings.asListNonNull(mapping.inverse()));
    }

    /**
     * Returns the relational table node for {@code tableName} if it occurs within a relational
     * expression {@code root} otherwise an empty option is returned.
     */
    public static @Nullable RelOptTable findTable(RelNode root, final String tableName) {
        try {
            RelShuttle visitor =
                    new RelHomogeneousShuttle() {
                        @Override
                        public RelNode visit(TableScan scan) {
                            final RelOptTable scanTable = scan.getTable();
                            final List<String> qualifiedName = scanTable.getQualifiedName();
                            if (qualifiedName.get(qualifiedName.size() - 1).equals(tableName)) {
                                throw new Util.FoundOne(scanTable);
                            }
                            return super.visit(scan);
                        }
                    };
            root.accept(visitor);
            return null;
        } catch (Util.FoundOne e) {
            Util.swallow(e, null);
            return (RelOptTable) e.getNode();
        }
    }

    /**
     * Returns whether relational expression {@code target} occurs within a relational expression
     * {@code ancestor}.
     */
    public static boolean contains(RelNode ancestor, final RelNode target) {
        if (ancestor == target) {
            // Short-cut common case.
            return true;
        }
        try {
            new RelVisitor() {
                @Override
                public void visit(RelNode node, int ordinal, @Nullable RelNode parent) {
                    if (node == target) {
                        throw Util.FoundOne.NULL;
                    }
                    super.visit(node, ordinal, parent);
                }
                // CHECKSTYLE: IGNORE 1
            }.go(ancestor);
            return false;
        } catch (Util.FoundOne e) {
            return true;
        }
    }

    /**
     * Within a relational expression {@code query}, replaces occurrences of {@code find} with
     * {@code replace}.
     */
    public static RelNode replace(RelNode query, RelNode find, RelNode replace) {
        if (find == replace) {
            // Short-cut common case.
            return query;
        }
        assert equalType("find", find, "replace", replace, Litmus.THROW);
        if (query == find) {
            // Short-cut another common case.
            return replace;
        }
        return replaceRecurse(query, find, replace);
    }

    /** Helper for {@link #replace}. */
    private static RelNode replaceRecurse(RelNode query, RelNode find, RelNode replace) {
        if (query == find) {
            return replace;
        }
        final List<RelNode> inputs = query.getInputs();
        if (!inputs.isEmpty()) {
            final List<RelNode> newInputs = new ArrayList<>();
            for (RelNode input : inputs) {
                newInputs.add(replaceRecurse(input, find, replace));
            }
            if (!newInputs.equals(inputs)) {
                return query.copy(query.getTraitSet(), newInputs);
            }
        }
        return query;
    }

    @Deprecated // to be removed before 2.0
    public static RelOptTable.ToRelContext getContext(RelOptCluster cluster) {
        return ViewExpanders.simpleContext(cluster);
    }

    /** Returns the number of {@link org.apache.calcite.rel.core.Join} nodes in a tree. */
    public static int countJoins(RelNode rootRel) {
        /** Visitor that counts join nodes. */
        class JoinCounter extends RelVisitor {
            int joinCount;

            @Override
            public void visit(
                    RelNode node,
                    int ordinal,
                    @org.checkerframework.checker.nullness.qual.Nullable RelNode parent) {
                if (node instanceof Join) {
                    ++joinCount;
                }
                super.visit(node, ordinal, parent);
            }

            int run(RelNode node) {
                go(node);
                return joinCount;
            }
        }

        return new JoinCounter().run(rootRel);
    }

    /** Permutes a record type according to a mapping. */
    public static RelDataType permute(
            RelDataTypeFactory typeFactory, RelDataType rowType, Mapping mapping) {
        return typeFactory.createStructType(Mappings.apply3(mapping, rowType.getFieldList()));
    }

    @Deprecated // to be removed before 2.0
    public static RelNode createProject(
            RelNode child, List<? extends RexNode> exprList, List<String> fieldNameList) {
        final RelBuilder relBuilder = RelFactories.LOGICAL_BUILDER.create(child.getCluster(), null);
        return relBuilder.push(child).project(exprList, fieldNameList, true).build();
    }

    @Deprecated // to be removed before 2.0
    public static RelNode createProject(
            RelNode child,
            List<Pair<RexNode, ? extends @Nullable String>> projectList,
            boolean optimize) {
        final RelBuilder relBuilder = RelFactories.LOGICAL_BUILDER.create(child.getCluster(), null);
        return relBuilder
                .push(child)
                .projectNamed(Pair.left(projectList), Pair.right(projectList), !optimize)
                .build();
    }

    /**
     * Creates a relational expression that projects the given fields of the input.
     *
     * <p>Optimizes if the fields are the identity projection.
     *
     * @param child Input relational expression
     * @param posList Source of each projected field
     * @return Relational expression that projects given fields
     */
    public static RelNode createProject(final RelNode child, final List<Integer> posList) {
        return createProject(RelFactories.DEFAULT_PROJECT_FACTORY, child, posList);
    }

    @Deprecated // to be removed before 2.0
    public static RelNode createProject(
            RelNode child,
            List<? extends RexNode> exprs,
            List<? extends @Nullable String> fieldNames,
            boolean optimize) {
        final RelBuilder relBuilder = RelFactories.LOGICAL_BUILDER.create(child.getCluster(), null);
        return relBuilder.push(child).projectNamed(exprs, fieldNames, !optimize).build();
    }

    // CHECKSTYLE: IGNORE 1
    /**
     * @deprecated Use {@link RelBuilder#projectNamed(Iterable, Iterable, boolean)}
     */
    @Deprecated // to be removed before 2.0
    public static RelNode createProject(
            RelNode child,
            List<? extends RexNode> exprs,
            List<? extends @Nullable String> fieldNames,
            boolean optimize,
            RelBuilder relBuilder) {
        return relBuilder.push(child).projectNamed(exprs, fieldNames, !optimize).build();
    }

    @Deprecated // to be removed before 2.0
    public static RelNode createRename(RelNode rel, List<? extends @Nullable String> fieldNames) {
        final List<RelDataTypeField> fields = rel.getRowType().getFieldList();
        assert fieldNames.size() == fields.size();
        final List<RexNode> refs =
                new AbstractList<RexNode>() {
                    @Override
                    public int size() {
                        return fields.size();
                    }

                    @Override
                    public RexNode get(int index) {
                        return RexInputRef.of(index, fields);
                    }
                };
        final RelBuilder relBuilder = RelFactories.LOGICAL_BUILDER.create(rel.getCluster(), null);
        return relBuilder.push(rel).projectNamed(refs, fieldNames, false).build();
    }

    /**
     * Creates a relational expression which permutes the output fields of a relational expression
     * according to a permutation.
     *
     * <p>Optimizations:
     *
     * <ul>
     *   <li>If the relational expression is a {@link org.apache.calcite.rel.logical.LogicalCalc} or
     *       {@link org.apache.calcite.rel.logical.LogicalProject} that is already acting as a
     *       permutation, combines the new permutation with the old;
     *   <li>If the permutation is the identity, returns the original relational expression.
     * </ul>
     *
     * <p>If a permutation is combined with its inverse, these optimizations would combine to remove
     * them both.
     *
     * @param rel Relational expression
     * @param permutation Permutation to apply to fields
     * @param fieldNames Field names; if null, or if a particular entry is null, the name of the
     *     permuted field is used
     * @return relational expression which permutes its input fields
     */
    public static RelNode permute(
            RelNode rel, Permutation permutation, @Nullable List<String> fieldNames) {
        if (permutation.isIdentity()) {
            return rel;
        }
        if (rel instanceof LogicalCalc) {
            LogicalCalc calc = (LogicalCalc) rel;
            Permutation permutation1 = calc.getProgram().getPermutation();
            if (permutation1 != null) {
                Permutation permutation2 = permutation.product(permutation1);
                return permute(rel, permutation2, null);
            }
        }
        if (rel instanceof LogicalProject) {
            Permutation permutation1 = ((LogicalProject) rel).getPermutation();
            if (permutation1 != null) {
                Permutation permutation2 = permutation.product(permutation1);
                return permute(rel, permutation2, null);
            }
        }
        final List<RelDataType> outputTypeList = new ArrayList<>();
        final List<String> outputNameList = new ArrayList<>();
        final List<RexNode> exprList = new ArrayList<>();
        final List<RexLocalRef> projectRefList = new ArrayList<>();
        final List<RelDataTypeField> fields = rel.getRowType().getFieldList();
        final RelOptCluster cluster = rel.getCluster();
        for (int i = 0; i < permutation.getTargetCount(); i++) {
            int target = permutation.getTarget(i);
            final RelDataTypeField targetField = fields.get(target);
            outputTypeList.add(targetField.getType());
            outputNameList.add(
                    ((fieldNames == null)
                                    || (fieldNames.size() <= i)
                                    || (fieldNames.get(i) == null))
                            ? targetField.getName()
                            : fieldNames.get(i));
            exprList.add(cluster.getRexBuilder().makeInputRef(fields.get(i).getType(), i));
            final int source = permutation.getSource(i);
            projectRefList.add(new RexLocalRef(source, fields.get(source).getType()));
        }
        final RelDataTypeFactory typeFactory = cluster.getTypeFactory();
        final RexProgram program =
                new RexProgram(
                        rel.getRowType(),
                        exprList,
                        projectRefList,
                        null,
                        typeFactory.createStructType(outputTypeList, outputNameList));
        return LogicalCalc.create(rel, program);
    }

    /**
     * Creates a relational expression that projects the given fields of the input.
     *
     * <p>Optimizes if the fields are the identity projection.
     *
     * @param factory ProjectFactory
     * @param child Input relational expression
     * @param posList Source of each projected field
     * @return Relational expression that projects given fields
     */
    public static RelNode createProject(
            final RelFactories.ProjectFactory factory,
            final RelNode child,
            final List<Integer> posList) {
        RelDataType rowType = child.getRowType();
        final List<String> fieldNames = rowType.getFieldNames();
        final RelBuilder relBuilder = RelBuilder.proto(factory).create(child.getCluster(), null);
        final List<RexNode> exprs =
                new AbstractList<RexNode>() {
                    @Override
                    public int size() {
                        return posList.size();
                    }

                    @Override
                    public RexNode get(int index) {
                        final int pos = posList.get(index);
                        return relBuilder.getRexBuilder().makeInputRef(child, pos);
                    }
                };
        final List<String> names = Util.select(fieldNames, posList);
        return relBuilder.push(child).projectNamed(exprs, names, false).build();
    }

    @Deprecated // to be removed before 2.0
    public static RelNode projectMapping(
            RelNode rel,
            Mapping mapping,
            @Nullable List<String> fieldNames,
            RelFactories.ProjectFactory projectFactory) {
        assert mapping.getMappingType().isSingleSource();
        assert mapping.getMappingType().isMandatorySource();
        if (mapping.isIdentity()) {
            return rel;
        }
        final List<String> outputNameList = new ArrayList<>();
        final List<RexNode> exprList = new ArrayList<>();
        final List<RelDataTypeField> fields = rel.getRowType().getFieldList();
        final RexBuilder rexBuilder = rel.getCluster().getRexBuilder();
        for (int i = 0; i < mapping.getTargetCount(); i++) {
            final int source = mapping.getSource(i);
            final RelDataTypeField sourceField = fields.get(source);
            outputNameList.add(
                    ((fieldNames == null)
                                    || (fieldNames.size() <= i)
                                    || (fieldNames.get(i) == null))
                            ? sourceField.getName()
                            : fieldNames.get(i));
            exprList.add(rexBuilder.makeInputRef(rel, source));
        }
        return projectFactory.createProject(
                rel, ImmutableList.of(), exprList, outputNameList, ImmutableSet.of());
    }

    /** Predicate for if a {@link Calc} does not contain windowed aggregates. */
    public static boolean notContainsWindowedAgg(Calc calc) {
        return !calc.containsOver();
    }

    /** Predicate for if a {@link Filter} does not contain windowed aggregates. */
    public static boolean notContainsWindowedAgg(Filter filter) {
        return !filter.containsOver();
    }

    /** Predicate for if a {@link Project} does not contain windowed aggregates. */
    public static boolean notContainsWindowedAgg(Project project) {
        return !project.containsOver();
    }

    /** Policies for handling two- and three-valued boolean logic. */
    public enum Logic {
        /** Three-valued boolean logic. */
        TRUE_FALSE_UNKNOWN,

        /** Nulls are not possible. */
        TRUE_FALSE,

        /**
         * Two-valued logic where UNKNOWN is treated as FALSE.
         *
         * <p>"x IS TRUE" produces the same result, and "WHERE x", "JOIN ... ON x" and "HAVING x"
         * have the same effect.
         */
        UNKNOWN_AS_FALSE,

        /**
         * Two-valued logic where UNKNOWN is treated as TRUE.
         *
         * <p>"x IS FALSE" produces the same result, as does "WHERE NOT x", etc.
         *
         * <p>In particular, this is the mode used by "WHERE k NOT IN q". If "k IN q" produces TRUE
         * or UNKNOWN, "NOT k IN q" produces FALSE or UNKNOWN and the row is eliminated; if "k IN q"
         * it returns FALSE, the row is retained by the WHERE clause.
         */
        UNKNOWN_AS_TRUE,

        /**
         * A semi-join will have been applied, so that only rows for which the value is TRUE will
         * have been returned.
         */
        TRUE,

        /**
         * An anti-semi-join will have been applied, so that only rows for which the value is FALSE
         * will have been returned.
         *
         * <p>Currently only used within {@link LogicVisitor}, to ensure that 'NOT (NOT EXISTS (q))'
         * behaves the same as 'EXISTS (q)')
         */
        FALSE;

        public Logic negate() {
            switch (this) {
                case UNKNOWN_AS_FALSE:
                case TRUE:
                    return UNKNOWN_AS_TRUE;
                case UNKNOWN_AS_TRUE:
                    return UNKNOWN_AS_FALSE;
                default:
                    return this;
            }
        }

        /**
         * Variant of {@link #negate()} to be used within {@link LogicVisitor}, where FALSE values
         * may exist.
         */
        public Logic negate2() {
            switch (this) {
                case FALSE:
                    return TRUE;
                case TRUE:
                    return FALSE;
                case UNKNOWN_AS_FALSE:
                    return UNKNOWN_AS_TRUE;
                case UNKNOWN_AS_TRUE:
                    return UNKNOWN_AS_FALSE;
                default:
                    return this;
            }
        }
    }

    /**
     * Pushes down expressions in "equal" join condition.
     *
     * <p>For example, given "emp JOIN dept ON emp.deptno + 1 = dept.deptno", adds a project above
     * "emp" that computes the expression "emp.deptno + 1". The resulting join condition is a simple
     * combination of AND, equals, and input fields, plus the remaining non-equal conditions.
     *
     * @param originalJoin Join whose condition is to be pushed down
     * @param relBuilder Factory to create project operator
     */
    public static RelNode pushDownJoinConditions(Join originalJoin, RelBuilder relBuilder) {
        RexNode joinCond = originalJoin.getCondition();
        final JoinRelType joinType = originalJoin.getJoinType();

        final List<RexNode> extraLeftExprs = new ArrayList<>();
        final List<RexNode> extraRightExprs = new ArrayList<>();
        final int leftCount = originalJoin.getLeft().getRowType().getFieldCount();
        final int rightCount = originalJoin.getRight().getRowType().getFieldCount();

        // You cannot push a 'get' because field names might change.
        //
        // Pushing sub-queries is OK in principle (if they don't reference both
        // sides of the join via correlating variables) but we'd rather not do it
        // yet.
        if (!containsGet(joinCond) && RexUtil.SubQueryFinder.find(joinCond) == null) {
            joinCond =
                    pushDownEqualJoinConditions(
                            joinCond,
                            leftCount,
                            rightCount,
                            extraLeftExprs,
                            extraRightExprs,
                            relBuilder.getRexBuilder());
        }

        final PairList<RexNode, @Nullable String> pairs = PairList.of();
        relBuilder.push(originalJoin.getLeft());
        if (!extraLeftExprs.isEmpty()) {
            final List<RelDataTypeField> fields = relBuilder.peek().getRowType().getFieldList();
            for (int i = 0, n = leftCount + extraLeftExprs.size(); i < n; i++) {
                if (i < leftCount) {
                    RelDataTypeField field = fields.get(i);
                    pairs.add(new RexInputRef(i, field.getType()), field.getName());
                } else {
                    pairs.add(extraLeftExprs.get(i - leftCount), null);
                }
            }
            relBuilder.project(pairs.leftList(), pairs.rightList());
            pairs.clear();
        }

        relBuilder.push(originalJoin.getRight());
        if (!extraRightExprs.isEmpty()) {
            final List<RelDataTypeField> fields = relBuilder.peek().getRowType().getFieldList();
            final int newLeftCount = leftCount + extraLeftExprs.size();
            for (int i = 0, n = rightCount + extraRightExprs.size(); i < n; i++) {
                if (i < rightCount) {
                    RelDataTypeField field = fields.get(i);
                    pairs.add(new RexInputRef(i, field.getType()), field.getName());
                } else {
                    pairs.add(
                            RexUtil.shift(extraRightExprs.get(i - rightCount), -newLeftCount),
                            null);
                }
            }
            relBuilder.project(pairs.leftList(), pairs.rightList());
            pairs.clear();
        }

        final RelNode right = relBuilder.build();
        final RelNode left = relBuilder.build();
        if (joinType == JoinRelType.ASOF || joinType == JoinRelType.LEFT_ASOF) {
            LogicalAsofJoin ljoin = (LogicalAsofJoin) originalJoin;
            RexNode match =
                    RexUtil.shift(ljoin.getMatchCondition(), leftCount, extraLeftExprs.size());
            RelNode copy = ljoin.copy(originalJoin.getTraitSet(), joinCond, match, left, right);
            relBuilder.push(copy);
        } else {
            relBuilder.push(
                    originalJoin.copy(
                            originalJoin.getTraitSet(),
                            joinCond,
                            left,
                            right,
                            joinType,
                            originalJoin.isSemiJoinDone()));
        }
        if (!extraLeftExprs.isEmpty() || !extraRightExprs.isEmpty()) {
            final int totalFields =
                    joinType.projectsRight()
                            ? leftCount
                                    + extraLeftExprs.size()
                                    + rightCount
                                    + extraRightExprs.size()
                            : leftCount + extraLeftExprs.size();
            final int[] mappingRanges =
                    joinType.projectsRight()
                            ? new int[] {
                                0,
                                0,
                                leftCount,
                                leftCount,
                                leftCount + extraLeftExprs.size(),
                                rightCount
                            }
                            : new int[] {0, 0, leftCount};
            Mappings.TargetMapping mapping =
                    Mappings.createShiftMapping(totalFields, mappingRanges);
            relBuilder.project(relBuilder.fields(mapping.inverse()));
        }
        return relBuilder.build();
    }

    @Deprecated // to be removed before 2.0
    public static RelNode pushDownJoinConditions(Join originalJoin) {
        return pushDownJoinConditions(originalJoin, RelFactories.LOGICAL_BUILDER);
    }

    @Deprecated // to be removed before 2.0
    public static RelNode pushDownJoinConditions(
            Join originalJoin, RelFactories.ProjectFactory projectFactory) {
        return pushDownJoinConditions(originalJoin, RelBuilder.proto(projectFactory));
    }

    private static RelNode pushDownJoinConditions(
            Join originalJoin, RelBuilderFactory relBuilderFactory) {
        return pushDownJoinConditions(
                originalJoin, relBuilderFactory.create(originalJoin.getCluster(), null));
    }

    private static boolean containsGet(RexNode node) {
        try {
            node.accept(
                    new RexVisitorImpl<Void>(true) {
                        @Override
                        public Void visitCall(RexCall call) {
                            if (call.getOperator() == RexBuilder.GET_OPERATOR) {
                                throw Util.FoundOne.NULL;
                            }
                            return super.visitCall(call);
                        }
                    });
            return false;
        } catch (Util.FoundOne e) {
            return true;
        }
    }

    /**
     * Pushes down parts of a join condition.
     *
     * <p>For example, given "emp JOIN dept ON emp.deptno + 1 = dept.deptno", adds a project above
     * "emp" that computes the expression "emp.deptno + 1". The resulting join condition is a simple
     * combination of AND, equals, and input fields.
     */
    private static RexNode pushDownEqualJoinConditions(
            RexNode condition,
            int leftCount,
            int rightCount,
            List<RexNode> extraLeftExprs,
            List<RexNode> extraRightExprs,
            RexBuilder builder) {
        // Normalize the condition first
        RexNode node =
                (condition instanceof RexCall)
                        ? collapseExpandedIsNotDistinctFromExpr((RexCall) condition, builder)
                        : condition;

        switch (node.getKind()) {
            case EQUALS:
            case IS_NOT_DISTINCT_FROM:
                final RexCall call0 = (RexCall) node;
                final RexNode leftRex = call0.getOperands().get(0);
                final RexNode rightRex = call0.getOperands().get(1);
                final ImmutableBitSet leftBits = RelOptUtil.InputFinder.bits(leftRex);
                final ImmutableBitSet rightBits = RelOptUtil.InputFinder.bits(rightRex);
                final int pivot = leftCount + extraLeftExprs.size();
                Side lside = Side.of(leftBits, pivot);
                Side rside = Side.of(rightBits, pivot);
                if (!lside.opposite(rside)) {
                    return call0;
                }
            // fall through
            case AND:
                final RexCall call = (RexCall) node;
                final List<RexNode> list = new ArrayList<>();
                List<RexNode> operands = Lists.newArrayList(call.getOperands());
                for (int i = 0; i < operands.size(); i++) {
                    RexNode operand = operands.get(i);
                    if (operand instanceof RexCall) {
                        operand = collapseExpandedIsNotDistinctFromExpr((RexCall) operand, builder);
                    }
                    if (node.getKind() == SqlKind.AND
                            && operand.getKind() != SqlKind.EQUALS
                            && operand.getKind() != SqlKind.IS_NOT_DISTINCT_FROM) {
                        // one of the join condition is neither EQ nor INDF
                        list.add(operand);
                    } else {
                        final int left2 = leftCount + extraLeftExprs.size();
                        final RexNode e =
                                pushDownEqualJoinConditions(
                                        operand,
                                        leftCount,
                                        rightCount,
                                        extraLeftExprs,
                                        extraRightExprs,
                                        builder);
                        if (!e.equals(operand)) {
                            final List<RexNode> remainingOperands = Util.skip(operands, i + 1);
                            final int left3 = leftCount + extraLeftExprs.size();
                            fix(remainingOperands, left2, left3);
                            fix(list, left2, left3);
                        }
                        list.add(e);
                    }
                }
                if (!list.equals(call.getOperands())) {
                    return call.clone(call.getType(), list);
                }
                return call;
            case OR:
            case INPUT_REF:
            case LITERAL:
            case NOT:
                return node;
            default:
                final ImmutableBitSet bits = RelOptUtil.InputFinder.bits(node);
                final int mid = leftCount + extraLeftExprs.size();
                switch (Side.of(bits, mid)) {
                    case LEFT:
                        fix(extraRightExprs, mid, mid + 1);
                        extraLeftExprs.add(node);
                        return new RexInputRef(mid, node.getType());
                    case RIGHT:
                        final int index2 = mid + rightCount + extraRightExprs.size();
                        extraRightExprs.add(node);
                        return new RexInputRef(index2, node.getType());
                    case BOTH:
                    case EMPTY:
                    default:
                        return node;
                }
        }
    }

    private static void fix(List<RexNode> operands, int before, int after) {
        if (before == after) {
            return;
        }
        operands.replaceAll(e -> RexUtil.shift(e, before, after - before));
    }

    /**
     * Determines whether any of the fields in a given relational expression may contain null
     * values, taking into account constraints on the field types and also deduced predicates.
     *
     * <p>The method is cautious: It may sometimes return {@code true} when the actual answer is
     * {@code false}. In particular, it does this when there is no executor, or the executor is not
     * a sub-class of {@link RexExecutorImpl}.
     */
    private static boolean containsNullableFields(RelNode r) {
        final RexBuilder rexBuilder = r.getCluster().getRexBuilder();
        final RelDataType rowType = r.getRowType();
        final List<RexNode> list = new ArrayList<>();
        final RelMetadataQuery mq = r.getCluster().getMetadataQuery();
        for (RelDataTypeField field : rowType.getFieldList()) {
            if (field.getType().isNullable()) {
                list.add(
                        rexBuilder.makeCall(
                                SqlStdOperatorTable.IS_NOT_NULL,
                                rexBuilder.makeInputRef(field.getType(), field.getIndex())));
            }
        }
        if (list.isEmpty()) {
            // All columns are declared NOT NULL.
            return false;
        }
        final RelOptPredicateList predicates = mq.getPulledUpPredicates(r);
        if (RelOptPredicateList.isEmpty(predicates)) {
            // We have no predicates, so cannot deduce that any of the fields
            // declared NULL are really NOT NULL.
            return true;
        }
        final RexExecutor executor = r.getCluster().getPlanner().getExecutor();
        if (!(executor instanceof RexExecutorImpl)) {
            // Cannot proceed without an executor.
            return true;
        }
        final RexImplicationChecker checker =
                new RexImplicationChecker(rexBuilder, executor, rowType);
        final RexNode first = RexUtil.composeConjunction(rexBuilder, predicates.pulledUpPredicates);
        final RexNode second = RexUtil.composeConjunction(rexBuilder, list);
        // Suppose we have EMP(empno INT NOT NULL, mgr INT),
        // and predicates [empno > 0, mgr > 0].
        // We make first: "empno > 0 AND mgr > 0"
        // and second: "mgr IS NOT NULL"
        // and ask whether first implies second.
        // It does, so we have no nullable columns.
        return !checker.implies(first, second);
    }

    // ~ Inner Classes ----------------------------------------------------------

    /**
     * A {@code RelShuttle} which propagates all the hints of relational expression to their
     * children nodes.
     *
     * <p>Given a plan:
     *
     * <blockquote>
     *
     * <pre>
     *            Filter (Hint1)
     *                |
     *               Join
     *              /    \
     *            Scan  Project (Hint2)
     *                     |
     *                    Scan2
     * </pre>
     *
     * </blockquote>
     *
     * <p>Every hint has a {@code inheritPath} (integers list) which records its propagate path,
     * number `0` represents the hint is propagated from the first(left) child, number `1`
     * represents the hint is propagated from the second(right) child, so the plan would have hints
     * path as follows (assumes each hint can be propagated to all child nodes):
     *
     * <ul>
     *   <li>Filter would have hints {Hint1[]}
     *   <li>Join would have hints {Hint1[0]}
     *   <li>Scan would have hints {Hint1[0, 0]}
     *   <li>Project would have hints {Hint1[0,1], Hint2[]}
     *   <li>Scan2 would have hints {[Hint1[0, 1, 0], Hint2[0]}
     * </ul>
     */
    private static class RelHintPropagateShuttle extends RelHomogeneousShuttle {
        /** Stack recording the hints and its current inheritPath. */
        private final Deque<Pair<List<RelHint>, Deque<Integer>>> inheritPaths = new ArrayDeque<>();

        /**
         * The hint strategies to decide if a hint should be attached to a relational expression.
         */
        private final HintStrategyTable hintStrategies;

        RelHintPropagateShuttle(HintStrategyTable hintStrategies) {
            this.hintStrategies = hintStrategies;
        }

        /** Visits a particular child of a parent. */
        @Override
        protected RelNode visitChild(RelNode parent, int i, RelNode child) {
            inheritPaths.forEach(inheritPath -> inheritPath.right.push(i));
            try {
                RelNode child2 = child.accept(this);
                if (child2 != child) {
                    final List<RelNode> newInputs = new ArrayList<>(parent.getInputs());
                    newInputs.set(i, child2);
                    return parent.copy(parent.getTraitSet(), newInputs);
                }
                return parent;
            } finally {
                inheritPaths.forEach(inheritPath -> inheritPath.right.pop());
            }
        }

        @Override
        public RelNode visit(RelNode other) {
            if (other instanceof Hintable) {
                return visitHintable(other);
            } else {
                return visitChildren(other);
            }
        }

        /**
         * Handle the {@link Hintable}s.
         *
         * <p>There are two cases to handle hints:
         *
         * <ul>
         *   <li>For TableScan: table scan is always a leaf node, attach the hints of the
         *       propagation path directly;
         *   <li>For other {@link Hintable}s: if the node has hints itself, that means, these hints
         *       are query hints that need to propagate to its children, so we do these things:
         *       <ol>
         *         <li>push the hints with empty inheritPath to the stack
         *         <li>visit the children nodes and propagate the hints
         *         <li>pop the hints pushed in step1
         *         <li>attach the hints of the propagation path
         *       </ol>
         *       if the node does not have hints, attach the hints of the propagation path directly.
         * </ul>
         *
         * @param node {@link Hintable} to handle
         * @return New copy of the {@code hintable} with propagated hints attached
         */
        private RelNode visitHintable(RelNode node) {
            final List<RelHint> topHints = ((Hintable) node).getHints();
            final boolean hasHints = !topHints.isEmpty();
            final boolean hasQueryHints = hasHints && !(node instanceof TableScan);
            if (hasQueryHints) {
                inheritPaths.push(Pair.of(topHints, new ArrayDeque<>()));
            }
            final RelNode node1 = visitChildren(node);
            if (hasQueryHints) {
                inheritPaths.pop();
            }
            return attachHints(node1);
        }

        private RelNode attachHints(RelNode original) {
            assert original instanceof Hintable;
            if (!inheritPaths.isEmpty()) {
                final List<RelHint> hints =
                        inheritPaths.stream()
                                .sorted(Comparator.comparingInt(o -> o.right.size()))
                                .map(path -> copyWithInheritPath(path.left, path.right))
                                .reduce(
                                        new ArrayList<>(),
                                        (acc, hints1) -> {
                                            acc.addAll(hints1);
                                            return acc;
                                        });
                final List<RelHint> filteredHints = hintStrategies.apply(hints, original);
                if (!filteredHints.isEmpty()) {
                    return ((Hintable) original).attachHints(filteredHints);
                }
            }
            return original;
        }

        private static List<RelHint> copyWithInheritPath(
                List<RelHint> hints, Deque<Integer> inheritPath) {
            // Copy the Dequeue in reverse order.
            final List<Integer> path = new ArrayList<>();
            final Iterator<Integer> iterator = inheritPath.descendingIterator();
            while (iterator.hasNext()) {
                path.add(iterator.next());
            }
            return hints.stream().map(hint -> hint.copy(path)).collect(Collectors.toList());
        }
    }

    /**
     * A {@code RelShuttle} which propagates the given hints to the sub-tree from the root node. It
     * stops the search of current path if the node already has hints or the whole propagation if
     * there is already a matched node.
     *
     * <p>Given a plan:
     *
     * <blockquote>
     *
     * <pre>
     *            Filter
     *                |
     *               Join
     *              /    \
     *            Scan  Project (Hint2)
     *                     |
     *                    Scan2
     * </pre>
     *
     * </blockquote>
     *
     * <p>The [Filter, Join, Scan] are the candidates(in sequence) to propagate, the whole
     * propagation ends if we append the given hints to a node successfully.
     */
    private static class SubTreeHintPropagateShuttle extends RelHomogeneousShuttle {
        /** Stack recording the appended inheritPath. */
        private final List<Integer> appendPath = new ArrayList<>();

        /**
         * The hint strategies to decide if a hint should be attached to a relational expression.
         */
        private final HintStrategyTable hintStrategies;

        /** Hints to propagate. */
        private final List<RelHint> hints;

        SubTreeHintPropagateShuttle(HintStrategyTable hintStrategies, List<RelHint> hints) {
            this.hintStrategies = hintStrategies;
            this.hints = hints;
        }

        /** Visits a particular child of a parent. */
        @Override
        protected RelNode visitChild(RelNode parent, int i, RelNode child) {
            appendPath.add(i);
            try {
                RelNode child2 = child.accept(this);
                if (child2 != child) {
                    final List<RelNode> newInputs = new ArrayList<>(parent.getInputs());
                    newInputs.set(i, child2);
                    return parent.copy(parent.getTraitSet(), newInputs);
                }
                return parent;
            } finally {
                // Remove the last element.
                appendPath.remove(appendPath.size() - 1);
            }
        }

        @Override
        public RelNode visit(RelNode other) {
            if (this.appendPath.size() > 3) {
                // Returns early if the visiting depth is bigger than 3
                return other;
            }
            if (other instanceof Hintable) {
                return visitHintable(other);
            } else {
                return visitChildren(other);
            }
        }

        /**
         * Handle the {@link Hintable}s.
         *
         * <p>Try to propagate the given hints to the node, the propagation finishes if:
         *
         * <ul>
         *   <li>This hintable already has hints, that means, the rel is definitely not created by a
         *       planner rule(or copied by the planner rule)
         *   <li>This hintable appended the hints successfully
         * </ul>
         *
         * @param node {@link Hintable} to handle
         * @return New copy of the {@code hintable} with propagated hints attached
         */
        private RelNode visitHintable(RelNode node) {
            final List<RelHint> topHints = ((Hintable) node).getHints();
            final boolean hasHints = !topHints.isEmpty();
            if (hasHints) {
                // This node is definitely not created by the planner, returns early.
                return node;
            }
            final RelNode node1 = attachHints(node);
            if (node1 != node) {
                return node1;
            }
            return visitChildren(node);
        }

        private RelNode attachHints(RelNode original) {
            assert original instanceof Hintable;
            final List<RelHint> hints =
                    this.hints.stream()
                            .map(hint -> copyWithAppendPath(hint, appendPath))
                            .collect(Collectors.toList());
            final List<RelHint> filteredHints = hintStrategies.apply(hints, original);
            if (!filteredHints.isEmpty()) {
                return ((Hintable) original).attachHints(filteredHints);
            }
            return original;
        }

        private static RelHint copyWithAppendPath(RelHint hint, List<Integer> appendPaths) {
            if (appendPaths.isEmpty()) {
                return hint;
            } else {
                List<Integer> newPath = new ArrayList<>(hint.inheritPath);
                newPath.addAll(appendPaths);
                return hint.copy(newPath);
            }
        }
    }

    /**
     * A {@code RelShuttle} which resets all the hints of a relational expression to what they are
     * originally like.
     *
     * <p>This would trigger a reverse transformation of what {@link RelHintPropagateShuttle} does.
     *
     * <p>Transformation rules:
     *
     * <ul>
     *   <li>Project: remove the hints that have non-empty inherit path (which means the hint was
     *       not originally declared from it);
     *   <li>Aggregate: remove the hints that have non-empty inherit path;
     *   <li>Join: remove all the hints;
     *   <li>TableScan: remove the hints that have non-empty inherit path.
     * </ul>
     */
    private static class ResetHintsShuttle extends RelHomogeneousShuttle {
        @Override
        public RelNode visit(RelNode node) {
            node = visitChildren(node);
            if (node instanceof Hintable) {
                node = resetHints((Hintable) node);
            }
            return node;
        }

        private static RelNode resetHints(Hintable hintable) {
            if (hintable.getHints().isEmpty()) {
                return (RelNode) hintable;
            }
            final List<RelHint> resetHints =
                    hintable.getHints().stream()
                            .filter(hint -> hint.inheritPath.isEmpty())
                            .collect(Collectors.toList());
            return hintable.withHints(resetHints);
        }
    }

    /** Visitor that finds all variables used but not stopped in an expression. */
    private static class VariableSetVisitor extends RelVisitor {
        final Set<CorrelationId> variables = new HashSet<>();

        // implement RelVisitor
        @Override
        public void visit(
                RelNode p,
                int ordinal,
                @org.checkerframework.checker.nullness.qual.Nullable RelNode parent) {
            super.visit(p, ordinal, parent);
            p.collectVariablesUsed(variables);

            // Important! Remove stopped variables AFTER we visit children
            // (which what super.visit() does)
            variables.removeAll(p.getVariablesSet());
        }
    }

    /** Visitor that finds all variables used in an expression. */
    public static class VariableUsedVisitor extends RexShuttle {
        public final Set<CorrelationId> variables = new LinkedHashSet<>();
        public final Multimap<CorrelationId, Integer> variableFields = LinkedHashMultimap.create();
        @NotOnlyInitialized private final @Nullable RelShuttle relShuttle;

        public VariableUsedVisitor(@UnknownInitialization @Nullable RelShuttle relShuttle) {
            this.relShuttle = relShuttle;
        }

        @Override
        public RexNode visitCorrelVariable(RexCorrelVariable p) {
            variables.add(p.id);
            variableFields.put(p.id, -1);
            return p;
        }

        @Override
        public RexNode visitFieldAccess(RexFieldAccess fieldAccess) {
            if (fieldAccess.getReferenceExpr() instanceof RexCorrelVariable) {
                final RexCorrelVariable v = (RexCorrelVariable) fieldAccess.getReferenceExpr();
                variableFields.put(v.id, fieldAccess.getField().getIndex());
            }
            return super.visitFieldAccess(fieldAccess);
        }

        @Override
        public RexNode visitSubQuery(RexSubQuery subQuery) {
            if (relShuttle != null) {
                subQuery.rel.accept(relShuttle); // look inside sub-queries
            }
            return super.visitSubQuery(subQuery);
        }
    }

    /** Shuttle that finds the set of inputs that are used. */
    public static class InputReferencedVisitor extends RexShuttle {
        public final NavigableSet<Integer> inputPosReferenced = new TreeSet<>();

        @Override
        public RexNode visitInputRef(RexInputRef inputRef) {
            inputPosReferenced.add(inputRef.getIndex());
            return inputRef;
        }
    }

    /** Converts types to descriptive strings. */
    public static class TypeDumper {
        private String indent;
        private final PrintWriter pw;

        TypeDumper(PrintWriter pw) {
            this.pw = pw;
            this.indent = "";
        }

        void accept(RelDataType type) {
            if (type.isStruct()) {
                final List<RelDataTypeField> fields = type.getFieldList();

                // RECORD (
                //   I INTEGER NOT NULL,
                //   J VARCHAR(240))
                pw.println("RECORD (");
                String prevIndent = indent;
                String extraIndent = "  ";
                this.indent = indent + extraIndent;
                acceptFields(fields);
                this.indent = prevIndent;
                pw.print(")");
                if (!type.isNullable()) {
                    pw.print(NON_NULLABLE_SUFFIX);
                }
            } else if (type instanceof MultisetSqlType) {
                // E.g. "INTEGER NOT NULL MULTISET NOT NULL"
                RelDataType componentType =
                        requireNonNull(
                                type.getComponentType(),
                                () -> "type.getComponentType() for " + type);
                accept(componentType);
                pw.print(" MULTISET");
                if (!type.isNullable()) {
                    pw.print(NON_NULLABLE_SUFFIX);
                }
            } else {
                // E.g. "INTEGER" E.g. "VARCHAR(240) CHARACTER SET "ISO-8859-1"
                // COLLATE "ISO-8859-1$en_US$primary" NOT NULL"
                pw.print(type.getFullTypeString());
            }
        }

        private void acceptFields(final List<RelDataTypeField> fields) {
            for (int i = 0; i < fields.size(); i++) {
                RelDataTypeField field = fields.get(i);
                if (i > 0) {
                    pw.println(",");
                }
                pw.print(indent);
                pw.print(field.getName());
                pw.print(" ");
                accept(field.getType());
            }
        }
    }

    /** Visitor which builds a bitmap of the inputs used by an expression. */
    public static class InputFinder extends RexVisitorImpl<Void> {
        private final ImmutableBitSet.Builder bitBuilder;
        private final @Nullable Set<RelDataTypeField> extraFields;

        private InputFinder(
                @Nullable Set<RelDataTypeField> extraFields, ImmutableBitSet.Builder bitBuilder) {
            super(true);
            this.bitBuilder = bitBuilder;
            this.extraFields = extraFields;
        }

        public InputFinder() {
            this(null);
        }

        public InputFinder(@Nullable Set<RelDataTypeField> extraFields) {
            this(extraFields, ImmutableBitSet.builder());
        }

        public InputFinder(
                @Nullable Set<RelDataTypeField> extraFields, ImmutableBitSet initialBits) {
            this(extraFields, initialBits.rebuild());
        }

        /** Returns an input finder that has analyzed a given expression. */
        public static InputFinder analyze(RexNode node) {
            final InputFinder inputFinder = new InputFinder();
            node.accept(inputFinder);
            return inputFinder;
        }

        /** Returns a bit set describing the inputs used by an expression. */
        public static ImmutableBitSet bits(RexNode node) {
            return analyze(node).build();
        }

        /**
         * Returns a bit set describing the inputs used by a collection of project expressions and
         * an optional condition.
         */
        public static ImmutableBitSet bits(List<RexNode> exprs, @Nullable RexNode expr) {
            final InputFinder inputFinder = new InputFinder();
            RexUtil.apply(inputFinder, exprs, expr);
            return inputFinder.build();
        }

        /**
         * Returns the bit set.
         *
         * <p>After calling this method, you cannot do any more visits or call this method again.
         */
        public ImmutableBitSet build() {
            return bitBuilder.build();
        }

        @Override
        public Void visitInputRef(RexInputRef inputRef) {
            bitBuilder.set(inputRef.getIndex());
            return null;
        }

        @Override
        public Void visitCall(RexCall call) {
            if (call.getOperator() == RexBuilder.GET_OPERATOR) {
                RexLiteral literal = (RexLiteral) call.getOperands().get(1);
                if (extraFields != null) {
                    requireNonNull(literal, () -> "first operand in " + call);
                    String value2 = (String) literal.getValue2();
                    requireNonNull(value2, () -> "value of the first operand in " + call);
                    extraFields.add(new RelDataTypeFieldImpl(value2, -1, call.getType()));
                }
            }
            return super.visitCall(call);
        }
    }

    /**
     * Walks an expression tree, converting the index of RexInputRefs based on some adjustment
     * factor.
     */
    public static class RexInputConverter extends RexShuttle {
        protected final RexBuilder rexBuilder;
        private final @Nullable List<RelDataTypeField> srcFields;
        protected final @Nullable List<RelDataTypeField> destFields;
        private final @Nullable List<RelDataTypeField> leftDestFields;
        private final @Nullable List<RelDataTypeField> rightDestFields;
        private final int nLeftDestFields;
        private final int[] adjustments;

        /**
         * Creates a RexInputConverter.
         *
         * @param rexBuilder builder for creating new RexInputRefs
         * @param srcFields fields where the RexInputRefs originated from; if null, a new
         *     RexInputRef is always created, referencing the input from destFields corresponding to
         *     its current index value
         * @param destFields fields that the new RexInputRefs will be referencing; if null, use the
         *     type information from the source field when creating the new RexInputRef
         * @param leftDestFields in the case where the destination is a join, these are the fields
         *     from the left join input
         * @param rightDestFields in the case where the destination is a join, these are the fields
         *     from the right join input
         * @param adjustments the amount to adjust each field by
         */
        private RexInputConverter(
                RexBuilder rexBuilder,
                @Nullable List<RelDataTypeField> srcFields,
                @Nullable List<RelDataTypeField> destFields,
                @Nullable List<RelDataTypeField> leftDestFields,
                @Nullable List<RelDataTypeField> rightDestFields,
                int[] adjustments) {
            this.rexBuilder = rexBuilder;
            this.srcFields = srcFields;
            this.destFields = destFields;
            this.adjustments = adjustments;
            this.leftDestFields = leftDestFields;
            this.rightDestFields = rightDestFields;
            if (leftDestFields == null) {
                nLeftDestFields = 0;
            } else {
                assert destFields == null;
                nLeftDestFields = leftDestFields.size();
            }
        }

        public RexInputConverter(
                RexBuilder rexBuilder,
                @Nullable List<RelDataTypeField> srcFields,
                @Nullable List<RelDataTypeField> leftDestFields,
                @Nullable List<RelDataTypeField> rightDestFields,
                int[] adjustments) {
            this(rexBuilder, srcFields, null, leftDestFields, rightDestFields, adjustments);
        }

        public RexInputConverter(
                RexBuilder rexBuilder,
                @Nullable List<RelDataTypeField> srcFields,
                @Nullable List<RelDataTypeField> destFields,
                int[] adjustments) {
            this(rexBuilder, srcFields, destFields, null, null, adjustments);
        }

        public RexInputConverter(
                RexBuilder rexBuilder,
                @Nullable List<RelDataTypeField> srcFields,
                int[] adjustments) {
            this(rexBuilder, srcFields, null, null, null, adjustments);
        }

        @Override
        public RexNode visitInputRef(RexInputRef var) {
            int srcIndex = var.getIndex();
            int destIndex = srcIndex + adjustments[srcIndex];

            RelDataType type;
            if (destFields != null) {
                type = destFields.get(destIndex).getType();
            } else if (leftDestFields != null) {
                if (destIndex < nLeftDestFields) {
                    type = leftDestFields.get(destIndex).getType();
                } else {
                    type =
                            requireNonNull(rightDestFields, "rightDestFields")
                                    .get(destIndex - nLeftDestFields)
                                    .getType();
                }
            } else {
                type = requireNonNull(srcFields, "srcFields").get(srcIndex).getType();
            }
            if ((adjustments[srcIndex] != 0)
                    || (srcFields == null)
                    || (type != srcFields.get(srcIndex).getType())) {
                return rexBuilder.makeInputRef(type, destIndex);
            } else {
                return var;
            }
        }
    }

    /** What kind of sub-query. */
    public enum SubQueryType {
        EXISTS,
        IN,
        SCALAR
    }

    /** Categorizes whether a bit set contains bits left and right of a line. */
    enum Side {
        LEFT,
        RIGHT,
        BOTH,
        EMPTY;

        static Side of(ImmutableBitSet bitSet, int middle) {
            final int firstBit = bitSet.nextSetBit(0);
            if (firstBit < 0) {
                return EMPTY;
            }
            if (firstBit >= middle) {
                return RIGHT;
            }
            if (bitSet.nextSetBit(middle) < 0) {
                return LEFT;
            }
            return BOTH;
        }

        public boolean opposite(Side side) {
            return (this == LEFT && side == RIGHT) || (this == RIGHT && side == LEFT);
        }
    }

    /**
     * Shuttle that finds correlation variables inside a given relational expression, including
     * those that are inside {@link RexSubQuery sub-queries}.
     */
    private static class CorrelationCollector extends RelHomogeneousShuttle {
        @SuppressWarnings("assignment.type.incompatible")
        private final VariableUsedVisitor vuv = new VariableUsedVisitor(this);

        @Override
        public RelNode visit(RelNode other) {
            other.collectVariablesUsed(vuv.variables);
            other.accept(vuv);
            RelNode result = super.visit(other);
            // Important! Remove stopped variables AFTER we visit
            // children. (which what super.visit() does)
            vuv.variables.removeAll(other.getVariablesSet());
            return result;
        }
    }

    /** Result of calling {@link org.apache.calcite.plan.RelOptUtil#createExistsPlan}. */
    public static class Exists {
        public final RelNode r;
        public final boolean indicator;
        public final boolean outerJoin;

        private Exists(RelNode r, boolean indicator, boolean outerJoin) {
            this.r = r;
            this.indicator = indicator;
            this.outerJoin = outerJoin;
        }
    }
}
