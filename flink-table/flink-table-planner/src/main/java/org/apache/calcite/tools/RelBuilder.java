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
package org.apache.calcite.tools;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMultiset;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Multiset;
import com.google.common.collect.Sets;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.linq4j.function.Experimental;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.ViewExpanders;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Intersect;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Match;
import org.apache.calcite.rel.core.Minus;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.RepeatUnion;
import org.apache.calcite.rel.core.Snapshot;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.Spool;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.TableSpool;
import org.apache.calcite.rel.core.Uncollect;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.hint.Hintable;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.metadata.RelColumnMapping;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCallBinding;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexExecutor;
import org.apache.calcite.rex.RexFieldCollation;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSimplify;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexWindowBound;
import org.apache.calcite.rex.RexWindowBounds;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.schema.TransientTable;
import org.apache.calcite.schema.impl.ListTransientTable;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.SqlWindow;
import org.apache.calcite.sql.fun.SqlCountAggFunction;
import org.apache.calcite.sql.fun.SqlLikeOperator;
import org.apache.calcite.sql.fun.SqlQuantifyOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.TableFunctionReturnTypeInference;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.util.Holder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.ImmutableNullableList;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.Optionality;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.Mappings;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.immutables.value.Value;

import java.math.BigDecimal;
import java.util.AbstractList;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.Deque;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static java.util.Objects.requireNonNull;
import static org.apache.calcite.linq4j.Nullness.castNonNull;
import static org.apache.calcite.sql.SqlKind.UNION;
import static org.apache.calcite.util.Static.RESOURCE;

/**
 * Copied from calcite to workaround CALCITE-4668
 *
 * <p>FLINK modifications are at lines
 *
 * <ol>
 *   <li>Should be removed after fix of FLINK-29804: Lines 2945 ~ 2948
 * </ol>
 */
@Value.Enclosing
public class RelBuilder {
    protected final RelOptCluster cluster;
    protected final @Nullable RelOptSchema relOptSchema;
    private final Deque<Frame> stack = new ArrayDeque<>();
    private RexSimplify simplifier;
    private final Config config;
    private final RelOptTable.ViewExpander viewExpander;
    private RelFactories.Struct struct;

    protected RelBuilder(
            @Nullable Context context, RelOptCluster cluster, @Nullable RelOptSchema relOptSchema) {
        this.cluster = cluster;
        this.relOptSchema = relOptSchema;
        if (context == null) {
            context = Contexts.EMPTY_CONTEXT;
        }
        this.config = getConfig(context);
        this.viewExpander = getViewExpander(cluster, context);
        this.struct = requireNonNull(RelFactories.Struct.fromContext(context));
        final RexExecutor executor =
                context.maybeUnwrap(RexExecutor.class)
                        .orElse(Util.first(cluster.getPlanner().getExecutor(), RexUtil.EXECUTOR));
        final RelOptPredicateList predicates = RelOptPredicateList.EMPTY;
        this.simplifier = new RexSimplify(cluster.getRexBuilder(), predicates, executor);
    }

    /**
     * Derives the view expander {@link org.apache.calcite.plan.RelOptTable.ViewExpander} to be used
     * for this RelBuilder.
     *
     * <p>The ViewExpander instance is used for expanding views in the default table scan factory
     * {@code RelFactories.TableScanFactoryImpl}. You can also define a new table scan factory in
     * the {@code struct} to override the whole table scan creation.
     *
     * <p>The default view expander does not support expanding views.
     */
    private static RelOptTable.ViewExpander getViewExpander(
            RelOptCluster cluster, Context context) {
        return context.maybeUnwrap(RelOptTable.ViewExpander.class)
                .orElseGet(() -> ViewExpanders.simpleContext(cluster));
    }

    /**
     * Derives the Config to be used for this RelBuilder.
     *
     * <p>Overrides {@link RelBuilder.Config#simplify} if {@link Hook#REL_BUILDER_SIMPLIFY} is set.
     */
    private static Config getConfig(Context context) {
        final Config config = context.maybeUnwrap(Config.class).orElse(Config.DEFAULT);
        boolean simplify = Hook.REL_BUILDER_SIMPLIFY.get(config.simplify());
        return config.withSimplify(simplify);
    }

    /** Creates a RelBuilder. */
    public static RelBuilder create(FrameworkConfig config) {
        return Frameworks.withPrepare(
                config,
                (cluster, relOptSchema, rootSchema, statement) ->
                        new RelBuilder(config.getContext(), cluster, relOptSchema));
    }

    /**
     * Creates a copy of this RelBuilder, with the same state as this, applying a transform to the
     * config.
     */
    public RelBuilder transform(UnaryOperator<Config> transform) {
        final Context context = Contexts.of(struct, transform.apply(config));
        return new RelBuilder(context, cluster, relOptSchema);
    }

    /**
     * Performs an action on this RelBuilder.
     *
     * <p>For example, consider the following code:
     *
     * <blockquote>
     *
     * <pre>
     *   RelNode filterAndRename(RelBuilder relBuilder, RelNode rel,
     *       RexNode condition, List&lt;String&gt; fieldNames) {
     *     relBuilder.push(rel)
     *         .filter(condition);
     *     if (fieldNames != null) {
     *       relBuilder.rename(fieldNames);
     *     }
     *     return relBuilder
     *         .build();</pre>
     *
     * </blockquote>
     *
     * <p>The pipeline is disrupted by the 'if'. The {@code let} method allows you to perform the
     * flow as a single pipeline:
     *
     * <blockquote>
     *
     * <pre>
     *   RelNode filterAndRename(RelBuilder relBuilder, RelNode rel,
     *       RexNode condition, List&lt;String&gt; fieldNames) {
     *     return relBuilder.push(rel)
     *         .filter(condition)
     *         .let(r -&gt; fieldNames == null ? r : r.rename(fieldNames))
     *         .build();</pre>
     *
     * </blockquote>
     *
     * <p>In pipelined cases such as this one, the lambda must return this RelBuilder. But {@code
     * let} return values of other types.
     */
    public <R> R let(Function<RelBuilder, R> consumer) {
        return consumer.apply(this);
    }

    /**
     * Converts this RelBuilder to a string. The string is the string representation of all of the
     * RelNodes on the stack.
     */
    @Override
    public String toString() {
        return stack.stream()
                .map(frame -> RelOptUtil.toString(frame.rel))
                .collect(Collectors.joining(""));
    }

    /** Returns the type factory. */
    public RelDataTypeFactory getTypeFactory() {
        return cluster.getTypeFactory();
    }

    /**
     * Returns new RelBuilder that adopts the convention provided. RelNode will be created with such
     * convention if corresponding factory is provided.
     */
    public RelBuilder adoptConvention(Convention convention) {
        this.struct = convention.getRelFactories();
        return this;
    }

    /** Returns the builder for {@link RexNode} expressions. */
    public RexBuilder getRexBuilder() {
        return cluster.getRexBuilder();
    }

    /**
     * Creates a {@link RelBuilderFactory}, a partially-created RelBuilder. Just add a {@link
     * RelOptCluster} and a {@link RelOptSchema}
     */
    public static RelBuilderFactory proto(final Context context) {
        return (cluster, schema) -> new RelBuilder(context, cluster, schema);
    }

    /** Creates a {@link RelBuilderFactory} that uses a given set of factories. */
    public static RelBuilderFactory proto(Object... factories) {
        return proto(Contexts.of(factories));
    }

    public RelOptCluster getCluster() {
        return cluster;
    }

    public @Nullable RelOptSchema getRelOptSchema() {
        return relOptSchema;
    }

    public RelFactories.TableScanFactory getScanFactory() {
        return struct.scanFactory;
    }

    // Methods for manipulating the stack

    /**
     * Adds a relational expression to be the input to the next relational expression constructed.
     *
     * <p>This method is usual when you want to weave in relational expressions that are not
     * supported by the builder. If, while creating such expressions, you need to use previously
     * built expressions as inputs, call {@link #build()} to pop those inputs.
     */
    public RelBuilder push(RelNode node) {
        stack.push(new Frame(node));
        return this;
    }

    /** Adds a rel node to the top of the stack while preserving the field names and aliases. */
    private void replaceTop(RelNode node) {
        final Frame frame = stack.pop();
        stack.push(new Frame(node, frame.fields));
    }

    /** Pushes a collection of relational expressions. */
    public RelBuilder pushAll(Iterable<? extends RelNode> nodes) {
        for (RelNode node : nodes) {
            push(node);
        }
        return this;
    }

    /** Returns the size of the stack. */
    public int size() {
        return stack.size();
    }

    /**
     * Returns the final relational expression.
     *
     * <p>Throws if the stack is empty.
     */
    public RelNode build() {
        return stack.pop().rel;
    }

    /** Returns the relational expression at the top of the stack, but does not remove it. */
    public RelNode peek() {
        return castNonNull(peek_()).rel;
    }

    private @Nullable Frame peek_() {
        return stack.peek();
    }

    /**
     * Returns the relational expression {@code n} positions from the top of the stack, but does not
     * remove it.
     */
    public RelNode peek(int n) {
        return peek_(n).rel;
    }

    private Frame peek_(int n) {
        if (n == 0) {
            // more efficient than starting an iterator
            return Objects.requireNonNull(stack.peek(), "stack.peek");
        }
        return Iterables.get(stack, n);
    }

    /**
     * Returns the relational expression {@code n} positions from the top of the stack, but does not
     * remove it.
     */
    public RelNode peek(int inputCount, int inputOrdinal) {
        return peek_(inputCount, inputOrdinal).rel;
    }

    private Frame peek_(int inputCount, int inputOrdinal) {
        return peek_(inputCount - 1 - inputOrdinal);
    }

    /**
     * Returns the number of fields in all inputs before (to the left of) the given input.
     *
     * @param inputCount Number of inputs
     * @param inputOrdinal Input ordinal
     */
    private int inputOffset(int inputCount, int inputOrdinal) {
        int offset = 0;
        for (int i = 0; i < inputOrdinal; i++) {
            offset += peek(inputCount, i).getRowType().getFieldCount();
        }
        return offset;
    }

    /** Evaluates an expression with a relational expression temporarily on the stack. */
    public <E> E with(RelNode r, Function<RelBuilder, E> fn) {
        try {
            push(r);
            return fn.apply(this);
        } finally {
            stack.pop();
        }
    }

    /** Performs an action with a temporary simplifier. */
    public <E> E withSimplifier(
            BiFunction<RelBuilder, RexSimplify, RexSimplify> simplifierTransform,
            Function<RelBuilder, E> fn) {
        final RexSimplify previousSimplifier = this.simplifier;
        try {
            this.simplifier = simplifierTransform.apply(this, previousSimplifier);
            return fn.apply(this);
        } finally {
            this.simplifier = previousSimplifier;
        }
    }

    /** Performs an action using predicates of the {@link #peek() current node} to simplify. */
    public <E> E withPredicates(RelMetadataQuery mq, Function<RelBuilder, E> fn) {
        final RelOptPredicateList predicates = mq.getPulledUpPredicates(peek());
        return withSimplifier((r, s) -> s.withPredicates(predicates), fn);
    }

    // Methods that return scalar expressions

    /** Creates a literal (constant expression). */
    public RexLiteral literal(@Nullable Object value) {
        final RexBuilder rexBuilder = cluster.getRexBuilder();
        if (value == null) {
            final RelDataType type = getTypeFactory().createSqlType(SqlTypeName.NULL);
            return rexBuilder.makeNullLiteral(type);
        } else if (value instanceof Boolean) {
            return rexBuilder.makeLiteral((Boolean) value);
        } else if (value instanceof BigDecimal) {
            return rexBuilder.makeExactLiteral((BigDecimal) value);
        } else if (value instanceof Float || value instanceof Double) {
            return rexBuilder.makeApproxLiteral(BigDecimal.valueOf(((Number) value).doubleValue()));
        } else if (value instanceof Number) {
            return rexBuilder.makeExactLiteral(BigDecimal.valueOf(((Number) value).longValue()));
        } else if (value instanceof String) {
            return rexBuilder.makeLiteral((String) value);
        } else if (value instanceof Enum) {
            return rexBuilder.makeLiteral(
                    value, getTypeFactory().createSqlType(SqlTypeName.SYMBOL));
        } else {
            throw new IllegalArgumentException(
                    "cannot convert " + value + " (" + value.getClass() + ") to a constant");
        }
    }

    /** Creates a correlation variable for the current input, and writes it into a Holder. */
    public RelBuilder variable(Holder<RexCorrelVariable> v) {
        v.set(
                (RexCorrelVariable)
                        getRexBuilder().makeCorrel(peek().getRowType(), cluster.createCorrel()));
        return this;
    }

    /**
     * Creates a reference to a field by name.
     *
     * <p>Equivalent to {@code field(1, 0, fieldName)}.
     *
     * @param fieldName Field name
     */
    public RexInputRef field(String fieldName) {
        return field(1, 0, fieldName);
    }

    /**
     * Creates a reference to a field of given input relational expression by name.
     *
     * @param inputCount Number of inputs
     * @param inputOrdinal Input ordinal
     * @param fieldName Field name
     */
    public RexInputRef field(int inputCount, int inputOrdinal, String fieldName) {
        final Frame frame = peek_(inputCount, inputOrdinal);
        final List<String> fieldNames = Pair.left(frame.fields());
        int i = fieldNames.indexOf(fieldName);
        if (i >= 0) {
            return field(inputCount, inputOrdinal, i);
        } else {
            throw new IllegalArgumentException(
                    "field [" + fieldName + "] not found; input fields are: " + fieldNames);
        }
    }

    /**
     * Creates a reference to an input field by ordinal.
     *
     * <p>Equivalent to {@code field(1, 0, ordinal)}.
     *
     * @param fieldOrdinal Field ordinal
     */
    public RexInputRef field(int fieldOrdinal) {
        return (RexInputRef) field(1, 0, fieldOrdinal, false);
    }

    /**
     * Creates a reference to a field of a given input relational expression by ordinal.
     *
     * @param inputCount Number of inputs
     * @param inputOrdinal Input ordinal
     * @param fieldOrdinal Field ordinal within input
     */
    public RexInputRef field(int inputCount, int inputOrdinal, int fieldOrdinal) {
        return (RexInputRef) field(inputCount, inputOrdinal, fieldOrdinal, false);
    }

    /**
     * As {@link #field(int, int, int)}, but if {@code alias} is true, the method may apply an alias
     * to make sure that the field has the same name as in the input frame. If no alias is applied
     * the expression is definitely a {@link RexInputRef}.
     */
    private RexNode field(int inputCount, int inputOrdinal, int fieldOrdinal, boolean alias) {
        final Frame frame = peek_(inputCount, inputOrdinal);
        final RelNode input = frame.rel;
        final RelDataType rowType = input.getRowType();
        if (fieldOrdinal < 0 || fieldOrdinal > rowType.getFieldCount()) {
            throw new IllegalArgumentException(
                    "field ordinal ["
                            + fieldOrdinal
                            + "] out of range; input fields are: "
                            + rowType.getFieldNames());
        }
        final RelDataTypeField field = rowType.getFieldList().get(fieldOrdinal);
        final int offset = inputOffset(inputCount, inputOrdinal);
        final RexInputRef ref =
                cluster.getRexBuilder().makeInputRef(field.getType(), offset + fieldOrdinal);
        final RelDataTypeField aliasField = frame.fields().get(fieldOrdinal);
        if (!alias || field.getName().equals(aliasField.getName())) {
            return ref;
        } else {
            return alias(ref, aliasField.getName());
        }
    }

    /**
     * Creates a reference to a field of the current record which originated in a relation with a
     * given alias.
     */
    public RexNode field(String alias, String fieldName) {
        return field(1, alias, fieldName);
    }

    /**
     * Creates a reference to a field which originated in a relation with the given alias. Searches
     * for the relation starting at the top of the stack.
     */
    public RexNode field(int inputCount, String alias, String fieldName) {
        requireNonNull(alias, "alias");
        requireNonNull(fieldName, "fieldName");
        final List<String> fields = new ArrayList<>();
        for (int inputOrdinal = 0; inputOrdinal < inputCount; ++inputOrdinal) {
            final Frame frame = peek_(inputOrdinal);
            for (Ord<Field> p : Ord.zip(frame.fields)) {
                // If alias and field name match, reference that field.
                if (p.e.left.contains(alias) && p.e.right.getName().equals(fieldName)) {
                    return field(inputCount, inputCount - 1 - inputOrdinal, p.i);
                }
                fields.add(
                        String.format(
                                Locale.ROOT,
                                "{aliases=%s,fieldName=%s}",
                                p.e.left,
                                p.e.right.getName()));
            }
        }
        throw new IllegalArgumentException(
                "{alias="
                        + alias
                        + ",fieldName="
                        + fieldName
                        + "} "
                        + "field not found; fields are: "
                        + fields);
    }

    /** Returns a reference to a given field of a record-valued expression. */
    public RexNode field(RexNode e, String name) {
        return getRexBuilder().makeFieldAccess(e, name, false);
    }

    /** Returns references to the fields of the top input. */
    public ImmutableList<RexNode> fields() {
        return fields(1, 0);
    }

    /** Returns references to the fields of a given input. */
    public ImmutableList<RexNode> fields(int inputCount, int inputOrdinal) {
        final RelNode input = peek(inputCount, inputOrdinal);
        final RelDataType rowType = input.getRowType();
        final ImmutableList.Builder<RexNode> nodes = ImmutableList.builder();
        for (int fieldOrdinal : Util.range(rowType.getFieldCount())) {
            nodes.add(field(inputCount, inputOrdinal, fieldOrdinal));
        }
        return nodes.build();
    }

    /** Returns references to fields for a given collation. */
    public ImmutableList<RexNode> fields(RelCollation collation) {
        final ImmutableList.Builder<RexNode> nodes = ImmutableList.builder();
        for (RelFieldCollation fieldCollation : collation.getFieldCollations()) {
            RexNode node = field(fieldCollation.getFieldIndex());
            switch (fieldCollation.direction) {
                case DESCENDING:
                    node = desc(node);
                    break;
                default:
                    break;
            }
            switch (fieldCollation.nullDirection) {
                case FIRST:
                    node = nullsFirst(node);
                    break;
                case LAST:
                    node = nullsLast(node);
                    break;
                default:
                    break;
            }
            nodes.add(node);
        }
        return nodes.build();
    }

    /** Returns references to fields for a given list of input ordinals. */
    public ImmutableList<RexNode> fields(List<? extends Number> ordinals) {
        final ImmutableList.Builder<RexNode> nodes = ImmutableList.builder();
        for (Number ordinal : ordinals) {
            RexNode node = field(1, 0, ordinal.intValue(), false);
            nodes.add(node);
        }
        return nodes.build();
    }

    /** Returns references to fields for a given bit set of input ordinals. */
    public ImmutableList<RexNode> fields(ImmutableBitSet ordinals) {
        return fields(ordinals.asList());
    }

    /** Returns references to fields identified by name. */
    public ImmutableList<RexNode> fields(Iterable<String> fieldNames) {
        final ImmutableList.Builder<RexNode> builder = ImmutableList.builder();
        for (String fieldName : fieldNames) {
            builder.add(field(fieldName));
        }
        return builder.build();
    }

    /** Returns references to fields identified by a mapping. */
    public ImmutableList<RexNode> fields(Mappings.TargetMapping mapping) {
        return fields(Mappings.asListNonNull(mapping));
    }

    /** Creates an access to a field by name. */
    public RexNode dot(RexNode node, String fieldName) {
        final RexBuilder builder = cluster.getRexBuilder();
        return builder.makeFieldAccess(node, fieldName, true);
    }

    /** Creates an access to a field by ordinal. */
    public RexNode dot(RexNode node, int fieldOrdinal) {
        final RexBuilder builder = cluster.getRexBuilder();
        return builder.makeFieldAccess(node, fieldOrdinal);
    }

    /** Creates a call to a scalar operator. */
    public RexNode call(SqlOperator operator, RexNode... operands) {
        return call(operator, ImmutableList.copyOf(operands));
    }

    /** Creates a call to a scalar operator. */
    private RexCall call(SqlOperator operator, List<RexNode> operandList) {
        switch (operator.getKind()) {
            case LIKE:
            case SIMILAR:
                final SqlLikeOperator likeOperator = (SqlLikeOperator) operator;
                if (likeOperator.isNegated()) {
                    final SqlOperator notLikeOperator = likeOperator.not();
                    return (RexCall) not(call(notLikeOperator, operandList));
                }
                break;
            case BETWEEN:
                assert operandList.size() == 3;
                return (RexCall)
                        between(operandList.get(0), operandList.get(1), operandList.get(2));
            default:
                break;
        }
        final RexBuilder builder = cluster.getRexBuilder();
        final RelDataType type = builder.deriveReturnType(operator, operandList);
        return (RexCall) builder.makeCall(type, operator, operandList);
    }

    /** Creates a call to a scalar operator. */
    public RexNode call(SqlOperator operator, Iterable<? extends RexNode> operands) {
        return call(operator, ImmutableList.copyOf(operands));
    }

    /**
     * Creates an IN predicate with a list of values.
     *
     * <p>For example,
     *
     * <pre>{@code
     * b.scan("Emp")
     *     .filter(b.in(b.field("deptno"), b.literal(10), b.literal(20)))
     * }</pre>
     *
     * is equivalent to SQL
     *
     * <pre>{@code
     * SELECT *
     * FROM Emp
     * WHERE deptno IN (10, 20)
     * }</pre>
     */
    public RexNode in(RexNode arg, RexNode... ranges) {
        return in(arg, ImmutableList.copyOf(ranges));
    }

    /**
     * Creates an IN predicate with a list of values.
     *
     * <p>For example,
     *
     * <pre>{@code
     * b.scan("Emps")
     *     .filter(
     *         b.in(b.field("deptno"),
     *             Arrays.asList(b.literal(10), b.literal(20))))
     * }</pre>
     *
     * <p>is equivalent to the SQL
     *
     * <pre>{@code
     * SELECT *
     * FROM Emps
     * WHERE deptno IN (10, 20)
     * }</pre>
     */
    public RexNode in(RexNode arg, Iterable<? extends RexNode> ranges) {
        return getRexBuilder().makeIn(arg, ImmutableList.copyOf(ranges));
    }

    /** Creates an IN predicate with a sub-query. */
    @Experimental
    public RexSubQuery in(RelNode rel, Iterable<? extends RexNode> nodes) {
        return RexSubQuery.in(rel, ImmutableList.copyOf(nodes));
    }

    /**
     * Creates an IN predicate with a sub-query.
     *
     * <p>For example,
     *
     * <pre>{@code
     * b.scan("Emps")
     *     .filter(
     *         b.in(b.field("deptno"),
     *             b2 -> b2.scan("Depts")
     *                 .filter(
     *                     b2.eq(b2.field("location"), b2.literal("Boston")))
     *                 .project(b.field("deptno"))
     *                 .build()))
     * }</pre>
     *
     * <p>is equivalent to the SQL
     *
     * <pre>{@code
     * SELECT *
     * FROM Emps
     * WHERE deptno IN (SELECT deptno FROM Dept WHERE location = 'Boston')
     * }</pre>
     */
    @Experimental
    public RexNode in(RexNode arg, Function<RelBuilder, RelNode> f) {
        final RelNode rel = f.apply(this);
        return RexSubQuery.in(rel, ImmutableList.of(arg));
    }

    /**
     * Creates a SOME (or ANY) predicate.
     *
     * <p>For example,
     *
     * <pre>{@code
     * b.scan("Emps")
     *     .filter(
     *         b.some(b.field("commission"),
     *             SqlStdOperatorTable.GREATER_THAN,
     *             b2 -> b2.scan("Emps")
     *                 .filter(
     *                     b2.eq(b2.field("job"), b2.literal("Manager")))
     *                 .project(b2.field("sal"))
     *                 .build()))
     * }</pre>
     *
     * <p>is equivalent to the SQL
     *
     * <pre>{@code
     * SELECT *
     * FROM Emps
     * WHERE commission > SOME (SELECT sal FROM Emps WHERE job = 'Manager')
     * }</pre>
     *
     * <p>or (since {@code SOME} and {@code ANY} are synonyms) the SQL
     *
     * <pre>{@code
     * SELECT *
     * FROM Emps
     * WHERE commission > ANY (SELECT sal FROM Emps WHERE job = 'Manager')
     * }</pre>
     */
    @Experimental
    public RexSubQuery some(RexNode node, SqlOperator op, Function<RelBuilder, RelNode> f) {
        return some_(node, op.kind, f);
    }

    private RexSubQuery some_(RexNode node, SqlKind kind, Function<RelBuilder, RelNode> f) {
        final RelNode rel = f.apply(this);
        final SqlQuantifyOperator quantifyOperator = SqlStdOperatorTable.some(kind);
        return RexSubQuery.some(rel, ImmutableList.of(node), quantifyOperator);
    }

    /**
     * Creates an ALL predicate.
     *
     * <p>For example,
     *
     * <pre>{@code
     * b.scan("Emps")
     *     .filter(
     *         b.all(b.field("commission"),
     *             SqlStdOperatorTable.GREATER_THAN,
     *             b2 -> b2.scan("Emps")
     *                 .filter(
     *                     b2.eq(b2.field("job"), b2.literal("Manager")))
     *                 .project(b2.field("sal"))
     *                 .build()))
     * }</pre>
     *
     * <p>is equivalent to the SQL
     *
     * <pre>{@code
     * SELECT *
     * FROM Emps
     * WHERE commission > ALL (SELECT sal FROM Emps WHERE job = 'Manager')
     * }</pre>
     *
     * <p>Calcite translates {@code ALL} predicates to {@code NOT SOME}. The following SQL is
     * equivalent to the previous:
     *
     * <pre>{@code
     * SELECT *
     * FROM Emps
     * WHERE NOT (commission <= SOME (SELECT sal FROM Emps WHERE job = 'Manager'))
     * }</pre>
     */
    @Experimental
    public RexNode all(RexNode node, SqlOperator op, Function<RelBuilder, RelNode> f) {
        return not(some_(node, op.kind.negateNullSafe(), f));
    }

    /**
     * Creates an EXISTS predicate.
     *
     * <p>For example,
     *
     * <pre>{@code
     * b.scan("Depts")
     *     .filter(
     *         b.exists(b2 ->
     *             b2.scan("Emps")
     *                 .filter(
     *                     b2.eq(b2.field("job"), b2.literal("Manager")))
     *                 .build()))
     * }</pre>
     *
     * <p>is equivalent to the SQL
     *
     * <pre>{@code
     * SELECT *
     * FROM Depts
     * WHERE EXISTS (SELECT 1 FROM Emps WHERE job = 'Manager')
     * }</pre>
     */
    @Experimental
    public RexSubQuery exists(Function<RelBuilder, RelNode> f) {
        final RelNode rel = f.apply(this);
        return RexSubQuery.exists(rel);
    }

    /**
     * Creates a UNIQUE predicate.
     *
     * <p>For example,
     *
     * <pre>{@code
     * b.scan("Depts")
     *     .filter(
     *         b.exists(b2 ->
     *             b2.scan("Emps")
     *                 .filter(
     *                     b2.eq(b2.field("job"), b2.literal("Manager")))
     *                 .project(b2.field("deptno")
     *                 .build()))
     * }</pre>
     *
     * <p>is equivalent to the SQL
     *
     * <pre>{@code
     * SELECT *
     * FROM Depts
     * WHERE UNIQUE (SELECT deptno FROM Emps WHERE job = 'Manager')
     * }</pre>
     */
    @Experimental
    public RexSubQuery unique(Function<RelBuilder, RelNode> f) {
        final RelNode rel = f.apply(this);
        return RexSubQuery.unique(rel);
    }

    /**
     * Creates a scalar sub-query.
     *
     * <p>For example,
     *
     * <pre>{@code
     * b.scan("Depts")
     *     .project(
     *         b.field("deptno")
     *         b.scalarQuery(b2 ->
     *             b2.scan("Emps")
     *                 .aggregate(
     *                     b2.eq(b2.field("job"), b2.literal("Manager")))
     *                 .build()))
     * }</pre>
     *
     * <p>is equivalent to the SQL
     *
     * <pre>{@code
     * SELECT deptno, (SELECT MAX(sal) FROM Emps)
     * FROM Depts
     * }</pre>
     */
    @Experimental
    public RexSubQuery scalarQuery(Function<RelBuilder, RelNode> f) {
        return RexSubQuery.scalar(f.apply(this));
    }

    /**
     * Creates an ARRAY sub-query.
     *
     * <p>For example,
     *
     * <pre>{@code
     * b.scan("Depts")
     *     .project(
     *         b.field("deptno")
     *         b.arrayQuery(b2 ->
     *             b2.scan("Emps")
     *                 .build()))
     * }</pre>
     *
     * <p>is equivalent to the SQL
     *
     * <pre>{@code
     * SELECT deptno, ARRAY (SELECT * FROM Emps)
     * FROM Depts
     * }</pre>
     */
    @Experimental
    public RexSubQuery arrayQuery(Function<RelBuilder, RelNode> f) {
        return RexSubQuery.array(f.apply(this));
    }

    /**
     * Creates a MULTISET sub-query.
     *
     * <p>For example,
     *
     * <pre>{@code
     * b.scan("Depts")
     *     .project(
     *         b.field("deptno")
     *         b.multisetQuery(b2 ->
     *             b2.scan("Emps")
     *                 .build()))
     * }</pre>
     *
     * <p>is equivalent to the SQL
     *
     * <pre>{@code
     * SELECT deptno, MULTISET (SELECT * FROM Emps)
     * FROM Depts
     * }</pre>
     */
    @Experimental
    public RexSubQuery multisetQuery(Function<RelBuilder, RelNode> f) {
        return RexSubQuery.multiset(f.apply(this));
    }

    /**
     * Creates a MAP sub-query.
     *
     * <p>For example,
     *
     * <pre>{@code
     * b.scan("Depts")
     *     .project(
     *         b.field("deptno")
     *         b.multisetQuery(b2 ->
     *             b2.scan("Emps")
     *                 .project(b2.field("empno"), b2.field("job"))
     *                 .build()))
     * }</pre>
     *
     * <p>is equivalent to the SQL
     *
     * <pre>{@code
     * SELECT deptno, MAP (SELECT empno, job FROM Emps)
     * FROM Depts
     * }</pre>
     */
    @Experimental
    public RexSubQuery mapQuery(Function<RelBuilder, RelNode> f) {
        return RexSubQuery.map(f.apply(this));
    }

    /** Creates an AND. */
    public RexNode and(RexNode... operands) {
        return and(ImmutableList.copyOf(operands));
    }

    /**
     * Creates an AND.
     *
     * <p>Simplifies the expression a little: {@code e AND TRUE} becomes {@code e}; {@code e AND e2
     * AND NOT e} becomes {@code e2}.
     */
    public RexNode and(Iterable<? extends RexNode> operands) {
        return RexUtil.composeConjunction(getRexBuilder(), operands);
    }

    /** Creates an OR. */
    public RexNode or(RexNode... operands) {
        return or(ImmutableList.copyOf(operands));
    }

    /** Creates an OR. */
    public RexNode or(Iterable<? extends RexNode> operands) {
        return RexUtil.composeDisjunction(cluster.getRexBuilder(), operands);
    }

    /** Creates a NOT. */
    public RexNode not(RexNode operand) {
        return call(SqlStdOperatorTable.NOT, operand);
    }

    /** Creates an {@code =}. */
    public RexNode equals(RexNode operand0, RexNode operand1) {
        return call(SqlStdOperatorTable.EQUALS, operand0, operand1);
    }

    /** Creates a {@code >}. */
    public RexNode greaterThan(RexNode operand0, RexNode operand1) {
        return call(SqlStdOperatorTable.GREATER_THAN, operand0, operand1);
    }

    /** Creates a {@code >=}. */
    public RexNode greaterThanOrEqual(RexNode operand0, RexNode operand1) {
        return call(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, operand0, operand1);
    }

    /** Creates a {@code <}. */
    public RexNode lessThan(RexNode operand0, RexNode operand1) {
        return call(SqlStdOperatorTable.LESS_THAN, operand0, operand1);
    }

    /** Creates a {@code <=}. */
    public RexNode lessThanOrEqual(RexNode operand0, RexNode operand1) {
        return call(SqlStdOperatorTable.LESS_THAN_OR_EQUAL, operand0, operand1);
    }

    /** Creates a {@code <>}. */
    public RexNode notEquals(RexNode operand0, RexNode operand1) {
        return call(SqlStdOperatorTable.NOT_EQUALS, operand0, operand1);
    }

    /**
     * Creates an expression equivalent to "{@code o0 IS NOT DISTINCT FROM o1}". It is also
     * equivalent to "{@code o0 = o1 OR (o0 IS NULL AND o1 IS NULL)}".
     */
    public RexNode isNotDistinctFrom(RexNode operand0, RexNode operand1) {
        return RelOptUtil.isDistinctFrom(getRexBuilder(), operand0, operand1, true);
    }

    /**
     * Creates an expression equivalent to {@code o0 IS DISTINCT FROM o1}. It is also equivalent to
     * "{@code NOT (o0 = o1 OR (o0 IS NULL AND o1 IS NULL))}.
     */
    public RexNode isDistinctFrom(RexNode operand0, RexNode operand1) {
        return RelOptUtil.isDistinctFrom(getRexBuilder(), operand0, operand1, false);
    }

    /** Creates a {@code BETWEEN}. */
    public RexNode between(RexNode arg, RexNode lower, RexNode upper) {
        return getRexBuilder().makeBetween(arg, lower, upper);
    }

    /** Creates ab {@code IS NULL}. */
    public RexNode isNull(RexNode operand) {
        return call(SqlStdOperatorTable.IS_NULL, operand);
    }

    /** Creates an {@code IS NOT NULL}. */
    public RexNode isNotNull(RexNode operand) {
        return call(SqlStdOperatorTable.IS_NOT_NULL, operand);
    }

    /** Creates an expression that casts an expression to a given type. */
    public RexNode cast(RexNode expr, SqlTypeName typeName) {
        final RelDataType type = cluster.getTypeFactory().createSqlType(typeName);
        return cluster.getRexBuilder().makeCast(type, expr);
    }

    /**
     * Creates an expression that casts an expression to a type with a given name and precision or
     * length.
     */
    public RexNode cast(RexNode expr, SqlTypeName typeName, int precision) {
        final RelDataType type = cluster.getTypeFactory().createSqlType(typeName, precision);
        return cluster.getRexBuilder().makeCast(type, expr);
    }

    /**
     * Creates an expression that casts an expression to a type with a given name, precision and
     * scale.
     */
    public RexNode cast(RexNode expr, SqlTypeName typeName, int precision, int scale) {
        final RelDataType type = cluster.getTypeFactory().createSqlType(typeName, precision, scale);
        return cluster.getRexBuilder().makeCast(type, expr);
    }

    /**
     * Returns an expression wrapped in an alias.
     *
     * <p>This method is idempotent: If the expression is already wrapped in the correct alias, does
     * nothing; if wrapped in an incorrect alias, removes the incorrect alias and applies the
     * correct alias.
     *
     * @see #project
     */
    public RexNode alias(RexNode expr, @Nullable String alias) {
        final RexNode aliasLiteral = literal(alias);
        switch (expr.getKind()) {
            case AS:
                final RexCall call = (RexCall) expr;
                if (call.operands.get(1).equals(aliasLiteral)) {
                    // current alias is correct
                    return expr;
                }
                expr = call.operands.get(0);
                // strip current (incorrect) alias, and fall through
            default:
                return call(SqlStdOperatorTable.AS, expr, aliasLiteral);
        }
    }

    /** Converts a sort expression to descending. */
    public RexNode desc(RexNode node) {
        return call(SqlStdOperatorTable.DESC, node);
    }

    /** Converts a sort expression to nulls last. */
    public RexNode nullsLast(RexNode node) {
        return call(SqlStdOperatorTable.NULLS_LAST, node);
    }

    /** Converts a sort expression to nulls first. */
    public RexNode nullsFirst(RexNode node) {
        return call(SqlStdOperatorTable.NULLS_FIRST, node);
    }

    // Methods that create window bounds

    /**
     * Creates an {@code UNBOUNDED PRECEDING} window bound, for use in methods such as {@link
     * OverCall#rowsFrom(RexWindowBound)} and {@link OverCall#rangeBetween(RexWindowBound,
     * RexWindowBound)}.
     */
    public RexWindowBound unboundedPreceding() {
        return RexWindowBounds.UNBOUNDED_PRECEDING;
    }

    /**
     * Creates a {@code bound PRECEDING} window bound, for use in methods such as {@link
     * OverCall#rowsFrom(RexWindowBound)} and {@link OverCall#rangeBetween(RexWindowBound,
     * RexWindowBound)}.
     */
    public RexWindowBound preceding(RexNode bound) {
        return RexWindowBounds.preceding(bound);
    }

    /**
     * Creates a {@code CURRENT ROW} window bound, for use in methods such as {@link
     * OverCall#rowsFrom(RexWindowBound)} and {@link OverCall#rangeBetween(RexWindowBound,
     * RexWindowBound)}.
     */
    public RexWindowBound currentRow() {
        return RexWindowBounds.CURRENT_ROW;
    }

    /**
     * Creates a {@code bound FOLLOWING} window bound, for use in methods such as {@link
     * OverCall#rowsFrom(RexWindowBound)} and {@link OverCall#rangeBetween(RexWindowBound,
     * RexWindowBound)}.
     */
    public RexWindowBound following(RexNode bound) {
        return RexWindowBounds.following(bound);
    }

    /**
     * Creates an {@code UNBOUNDED FOLLOWING} window bound, for use in methods such as {@link
     * OverCall#rowsFrom(RexWindowBound)} and {@link OverCall#rangeBetween(RexWindowBound,
     * RexWindowBound)}.
     */
    public RexWindowBound unboundedFollowing() {
        return RexWindowBounds.UNBOUNDED_FOLLOWING;
    }

    // Methods that create group keys and aggregate calls

    /** Creates an empty group key. */
    public GroupKey groupKey() {
        return groupKey(ImmutableList.of());
    }

    /** Creates a group key. */
    public GroupKey groupKey(RexNode... nodes) {
        return groupKey(ImmutableList.copyOf(nodes));
    }

    /** Creates a group key. */
    public GroupKey groupKey(Iterable<? extends RexNode> nodes) {
        return new GroupKeyImpl(ImmutableList.copyOf(nodes), null, null);
    }

    /** Creates a group key with grouping sets. */
    public GroupKey groupKey(
            Iterable<? extends RexNode> nodes,
            Iterable<? extends Iterable<? extends RexNode>> nodeLists) {
        return groupKey_(nodes, nodeLists);
    }

    // CHECKSTYLE: IGNORE 1
    /**
     * @deprecated Now that indicator is deprecated, use {@link #groupKey(Iterable, Iterable)},
     *     which has the same behavior as calling this method with {@code indicator = false}.
     */
    @Deprecated // to be removed before 2.0
    public GroupKey groupKey(
            Iterable<? extends RexNode> nodes,
            boolean indicator,
            Iterable<? extends Iterable<? extends RexNode>> nodeLists) {
        Aggregate.checkIndicator(indicator);
        return groupKey_(nodes, nodeLists);
    }

    private static GroupKey groupKey_(
            Iterable<? extends RexNode> nodes,
            Iterable<? extends Iterable<? extends RexNode>> nodeLists) {
        final ImmutableList.Builder<ImmutableList<RexNode>> builder = ImmutableList.builder();
        for (Iterable<? extends RexNode> nodeList : nodeLists) {
            builder.add(ImmutableList.copyOf(nodeList));
        }
        return new GroupKeyImpl(ImmutableList.copyOf(nodes), builder.build(), null);
    }

    /** Creates a group key of fields identified by ordinal. */
    public GroupKey groupKey(int... fieldOrdinals) {
        return groupKey(fields(ImmutableIntList.of(fieldOrdinals)));
    }

    /** Creates a group key of fields identified by name. */
    public GroupKey groupKey(String... fieldNames) {
        return groupKey(fields(ImmutableList.copyOf(fieldNames)));
    }

    /**
     * Creates a group key, identified by field positions in the underlying relational expression.
     *
     * <p>This method of creating a group key does not allow you to group on new expressions, only
     * column projections, but is efficient, especially when you are coming from an existing {@link
     * Aggregate}.
     */
    public GroupKey groupKey(ImmutableBitSet groupSet) {
        return groupKey_(groupSet, ImmutableList.of(groupSet));
    }

    /**
     * Creates a group key with grouping sets, both identified by field positions in the underlying
     * relational expression.
     *
     * <p>This method of creating a group key does not allow you to group on new expressions, only
     * column projections, but is efficient, especially when you are coming from an existing {@link
     * Aggregate}.
     *
     * <p>It is possible for {@code groupSet} to be strict superset of all {@code groupSets}. For
     * example, in the pseudo SQL
     *
     * <pre>{@code
     * GROUP BY 0, 1, 2
     * GROUPING SETS ((0, 1), 0)
     * }</pre>
     *
     * <p>column 2 does not appear in either grouping set. This is not valid SQL. We can approximate
     * in actual SQL by adding an extra grouping set and filtering out using {@code HAVING}, as
     * follows:
     *
     * <pre>{@code
     * GROUP BY GROUPING SETS ((0, 1, 2), (0, 1), 0)
     * HAVING GROUPING_ID(0, 1, 2) <> 0
     * }</pre>
     */
    public GroupKey groupKey(
            ImmutableBitSet groupSet, Iterable<? extends ImmutableBitSet> groupSets) {
        return groupKey_(groupSet, ImmutableList.copyOf(groupSets));
    }

    // CHECKSTYLE: IGNORE 1
    /** @deprecated Use {@link #groupKey(ImmutableBitSet, Iterable)}. */
    @Deprecated // to be removed before 2.0
    public GroupKey groupKey(
            ImmutableBitSet groupSet,
            boolean indicator,
            @Nullable ImmutableList<ImmutableBitSet> groupSets) {
        Aggregate.checkIndicator(indicator);
        return groupKey_(
                groupSet,
                groupSets == null ? ImmutableList.of(groupSet) : ImmutableList.copyOf(groupSets));
    }

    private GroupKey groupKey_(ImmutableBitSet groupSet, ImmutableList<ImmutableBitSet> groupSets) {
        if (groupSet.length() > peek().getRowType().getFieldCount()) {
            throw new IllegalArgumentException("out of bounds: " + groupSet);
        }
        requireNonNull(groupSets, "groupSets");
        final ImmutableList<RexNode> nodes = fields(groupSet);
        return groupKey_(nodes, Util.transform(groupSets, this::fields));
    }

    @Deprecated // to be removed before 2.0
    public AggCall aggregateCall(
            SqlAggFunction aggFunction,
            boolean distinct,
            RexNode filter,
            @Nullable String alias,
            RexNode... operands) {
        return aggregateCall(
                aggFunction,
                distinct,
                false,
                false,
                filter,
                null,
                ImmutableList.of(),
                alias,
                ImmutableList.copyOf(operands));
    }

    @Deprecated // to be removed before 2.0
    public AggCall aggregateCall(
            SqlAggFunction aggFunction,
            boolean distinct,
            boolean approximate,
            RexNode filter,
            @Nullable String alias,
            RexNode... operands) {
        return aggregateCall(
                aggFunction,
                distinct,
                approximate,
                false,
                filter,
                null,
                ImmutableList.of(),
                alias,
                ImmutableList.copyOf(operands));
    }

    @Deprecated // to be removed before 2.0
    public AggCall aggregateCall(
            SqlAggFunction aggFunction,
            boolean distinct,
            RexNode filter,
            @Nullable String alias,
            Iterable<? extends RexNode> operands) {
        return aggregateCall(
                aggFunction,
                distinct,
                false,
                false,
                filter,
                null,
                ImmutableList.of(),
                alias,
                ImmutableList.copyOf(operands));
    }

    @Deprecated // to be removed before 2.0
    public AggCall aggregateCall(
            SqlAggFunction aggFunction,
            boolean distinct,
            boolean approximate,
            RexNode filter,
            @Nullable String alias,
            Iterable<? extends RexNode> operands) {
        return aggregateCall(
                aggFunction,
                distinct,
                approximate,
                false,
                filter,
                null,
                ImmutableList.of(),
                alias,
                ImmutableList.copyOf(operands));
    }

    /**
     * Creates a call to an aggregate function.
     *
     * <p>To add other operands, apply {@link AggCall#distinct()}, {@link
     * AggCall#approximate(boolean)}, {@link AggCall#filter(RexNode...)}, {@link AggCall#sort},
     * {@link AggCall#as} to the result.
     */
    public AggCall aggregateCall(SqlAggFunction aggFunction, Iterable<? extends RexNode> operands) {
        return aggregateCall(
                aggFunction,
                false,
                false,
                false,
                null,
                null,
                ImmutableList.of(),
                null,
                ImmutableList.copyOf(operands));
    }

    /**
     * Creates a call to an aggregate function.
     *
     * <p>To add other operands, apply {@link AggCall#distinct()}, {@link
     * AggCall#approximate(boolean)}, {@link AggCall#filter(RexNode...)}, {@link AggCall#sort},
     * {@link AggCall#as} to the result.
     */
    public AggCall aggregateCall(SqlAggFunction aggFunction, RexNode... operands) {
        return aggregateCall(
                aggFunction,
                false,
                false,
                false,
                null,
                null,
                ImmutableList.of(),
                null,
                ImmutableList.copyOf(operands));
    }

    /** Creates a call to an aggregate function as a copy of an {@link AggregateCall}. */
    public AggCall aggregateCall(AggregateCall a) {
        return aggregateCall(
                a.getAggregation(),
                a.isDistinct(),
                a.isApproximate(),
                a.ignoreNulls(),
                a.filterArg < 0 ? null : field(a.filterArg),
                a.distinctKeys == null ? null : fields(a.distinctKeys),
                fields(a.collation),
                a.name,
                fields(a.getArgList()));
    }

    /**
     * Creates a call to an aggregate function as a copy of an {@link AggregateCall}, applying a
     * mapping.
     */
    public AggCall aggregateCall(AggregateCall a, Mapping mapping) {
        return aggregateCall(
                a.getAggregation(),
                a.isDistinct(),
                a.isApproximate(),
                a.ignoreNulls(),
                a.filterArg < 0 ? null : field(Mappings.apply(mapping, a.filterArg)),
                a.distinctKeys == null ? null : fields(Mappings.apply(mapping, a.distinctKeys)),
                fields(RexUtil.apply(mapping, a.collation)),
                a.name,
                fields(Mappings.apply2(mapping, a.getArgList())));
    }

    /** Creates a call to an aggregate function with all applicable operands. */
    protected AggCall aggregateCall(
            SqlAggFunction aggFunction,
            boolean distinct,
            boolean approximate,
            boolean ignoreNulls,
            @Nullable RexNode filter,
            @Nullable ImmutableList<RexNode> distinctKeys,
            ImmutableList<RexNode> orderKeys,
            @Nullable String alias,
            ImmutableList<RexNode> operands) {
        return new AggCallImpl(
                aggFunction,
                distinct,
                approximate,
                ignoreNulls,
                filter,
                alias,
                operands,
                distinctKeys,
                orderKeys);
    }

    /** Creates a call to the {@code COUNT} aggregate function. */
    public AggCall count(RexNode... operands) {
        return count(false, null, operands);
    }

    /** Creates a call to the {@code COUNT} aggregate function. */
    public AggCall count(Iterable<? extends RexNode> operands) {
        return count(false, null, operands);
    }

    /**
     * Creates a call to the {@code COUNT} aggregate function, optionally distinct and with an
     * alias.
     */
    public AggCall count(boolean distinct, @Nullable String alias, RexNode... operands) {
        return aggregateCall(
                SqlStdOperatorTable.COUNT,
                distinct,
                false,
                false,
                null,
                null,
                ImmutableList.of(),
                alias,
                ImmutableList.copyOf(operands));
    }

    /**
     * Creates a call to the {@code COUNT} aggregate function, optionally distinct and with an
     * alias.
     */
    public AggCall count(
            boolean distinct, @Nullable String alias, Iterable<? extends RexNode> operands) {
        return aggregateCall(
                SqlStdOperatorTable.COUNT,
                distinct,
                false,
                false,
                null,
                null,
                ImmutableList.of(),
                alias,
                ImmutableList.copyOf(operands));
    }

    /** Creates a call to the {@code COUNT(*)} aggregate function. */
    public AggCall countStar(@Nullable String alias) {
        return count(false, alias);
    }

    /** Creates a call to the {@code SUM} aggregate function. */
    public AggCall sum(RexNode operand) {
        return sum(false, null, operand);
    }

    /**
     * Creates a call to the {@code SUM} aggregate function, optionally distinct and with an alias.
     */
    public AggCall sum(boolean distinct, @Nullable String alias, RexNode operand) {
        return aggregateCall(
                SqlStdOperatorTable.SUM,
                distinct,
                false,
                false,
                null,
                null,
                ImmutableList.of(),
                alias,
                ImmutableList.of(operand));
    }

    /** Creates a call to the {@code AVG} aggregate function. */
    public AggCall avg(RexNode operand) {
        return avg(false, null, operand);
    }

    /**
     * Creates a call to the {@code AVG} aggregate function, optionally distinct and with an alias.
     */
    public AggCall avg(boolean distinct, @Nullable String alias, RexNode operand) {
        return aggregateCall(
                SqlStdOperatorTable.AVG,
                distinct,
                false,
                false,
                null,
                null,
                ImmutableList.of(),
                alias,
                ImmutableList.of(operand));
    }

    /** Creates a call to the {@code MIN} aggregate function. */
    public AggCall min(RexNode operand) {
        return min(null, operand);
    }

    /** Creates a call to the {@code MIN} aggregate function, optionally with an alias. */
    public AggCall min(@Nullable String alias, RexNode operand) {
        return aggregateCall(
                SqlStdOperatorTable.MIN,
                false,
                false,
                false,
                null,
                null,
                ImmutableList.of(),
                alias,
                ImmutableList.of(operand));
    }

    /** Creates a call to the {@code MAX} aggregate function, optionally with an alias. */
    public AggCall max(RexNode operand) {
        return max(null, operand);
    }

    /** Creates a call to the {@code MAX} aggregate function. */
    public AggCall max(@Nullable String alias, RexNode operand) {
        return aggregateCall(
                SqlStdOperatorTable.MAX,
                false,
                false,
                false,
                null,
                null,
                ImmutableList.of(),
                alias,
                ImmutableList.of(operand));
    }

    // Methods for patterns

    /**
     * Creates a reference to a given field of the pattern.
     *
     * @param alpha the pattern name
     * @param type Type of field
     * @param i Ordinal of field
     * @return Reference to field of pattern
     */
    public RexNode patternField(String alpha, RelDataType type, int i) {
        return getRexBuilder().makePatternFieldRef(alpha, type, i);
    }

    /** Creates a call that concatenates patterns; for use in {@link #match}. */
    public RexNode patternConcat(Iterable<? extends RexNode> nodes) {
        final ImmutableList<RexNode> list = ImmutableList.copyOf(nodes);
        if (list.size() > 2) {
            // Convert into binary calls
            return patternConcat(patternConcat(Util.skipLast(list)), Util.last(list));
        }
        final RelDataType t = getTypeFactory().createSqlType(SqlTypeName.NULL);
        return getRexBuilder().makeCall(t, SqlStdOperatorTable.PATTERN_CONCAT, list);
    }

    /** Creates a call that concatenates patterns; for use in {@link #match}. */
    public RexNode patternConcat(RexNode... nodes) {
        return patternConcat(ImmutableList.copyOf(nodes));
    }

    /** Creates a call that creates alternate patterns; for use in {@link #match}. */
    public RexNode patternAlter(Iterable<? extends RexNode> nodes) {
        final RelDataType t = getTypeFactory().createSqlType(SqlTypeName.NULL);
        return getRexBuilder()
                .makeCall(t, SqlStdOperatorTable.PATTERN_ALTER, ImmutableList.copyOf(nodes));
    }

    /** Creates a call that creates alternate patterns; for use in {@link #match}. */
    public RexNode patternAlter(RexNode... nodes) {
        return patternAlter(ImmutableList.copyOf(nodes));
    }

    /** Creates a call that creates quantify patterns; for use in {@link #match}. */
    public RexNode patternQuantify(Iterable<? extends RexNode> nodes) {
        final RelDataType t = getTypeFactory().createSqlType(SqlTypeName.NULL);
        return getRexBuilder()
                .makeCall(t, SqlStdOperatorTable.PATTERN_QUANTIFIER, ImmutableList.copyOf(nodes));
    }

    /** Creates a call that creates quantify patterns; for use in {@link #match}. */
    public RexNode patternQuantify(RexNode... nodes) {
        return patternQuantify(ImmutableList.copyOf(nodes));
    }

    /** Creates a call that creates permute patterns; for use in {@link #match}. */
    public RexNode patternPermute(Iterable<? extends RexNode> nodes) {
        final RelDataType t = getTypeFactory().createSqlType(SqlTypeName.NULL);
        return getRexBuilder()
                .makeCall(t, SqlStdOperatorTable.PATTERN_PERMUTE, ImmutableList.copyOf(nodes));
    }

    /** Creates a call that creates permute patterns; for use in {@link #match}. */
    public RexNode patternPermute(RexNode... nodes) {
        return patternPermute(ImmutableList.copyOf(nodes));
    }

    /** Creates a call that creates an exclude pattern; for use in {@link #match}. */
    public RexNode patternExclude(RexNode node) {
        final RelDataType t = getTypeFactory().createSqlType(SqlTypeName.NULL);
        return getRexBuilder()
                .makeCall(t, SqlStdOperatorTable.PATTERN_EXCLUDE, ImmutableList.of(node));
    }

    // Methods that create relational expressions

    /**
     * Creates a {@link TableScan} of the table with a given name.
     *
     * <p>Throws if the table does not exist.
     *
     * <p>Returns this builder.
     *
     * @param tableNames Name of table (can optionally be qualified)
     */
    public RelBuilder scan(Iterable<String> tableNames) {
        final List<String> names = ImmutableList.copyOf(tableNames);
        requireNonNull(relOptSchema, "relOptSchema");
        final RelOptTable relOptTable = relOptSchema.getTableForMember(names);
        if (relOptTable == null) {
            throw RESOURCE.tableNotFound(String.join(".", names)).ex();
        }
        final RelNode scan =
                struct.scanFactory.createScan(
                        ViewExpanders.toRelContext(viewExpander, cluster), relOptTable);
        push(scan);
        rename(relOptTable.getRowType().getFieldNames());

        // When the node is not a TableScan but from expansion,
        // we need to explicitly add the alias.
        if (!(scan instanceof TableScan)) {
            as(Util.last(ImmutableList.copyOf(tableNames)));
        }
        return this;
    }

    /**
     * Creates a {@link TableScan} of the table with a given name.
     *
     * <p>Throws if the table does not exist.
     *
     * <p>Returns this builder.
     *
     * @param tableNames Name of table (can optionally be qualified)
     */
    public RelBuilder scan(String... tableNames) {
        return scan(ImmutableList.copyOf(tableNames));
    }

    /**
     * Creates a {@link Snapshot} of a given snapshot period.
     *
     * <p>Returns this builder.
     *
     * @param period Name of table (can optionally be qualified)
     */
    public RelBuilder snapshot(RexNode period) {
        final Frame frame = stack.pop();
        final RelNode snapshot = struct.snapshotFactory.createSnapshot(frame.rel, period);
        stack.push(new Frame(snapshot, frame.fields));
        return this;
    }

    /**
     * Gets column mappings of the operator.
     *
     * @param op operator instance
     * @return column mappings associated with this function
     */
    private static @Nullable Set<RelColumnMapping> getColumnMappings(SqlOperator op) {
        SqlReturnTypeInference inference = op.getReturnTypeInference();
        if (inference instanceof TableFunctionReturnTypeInference) {
            return ((TableFunctionReturnTypeInference) inference).getColumnMappings();
        } else {
            return null;
        }
    }

    /**
     * Creates a RexCall to the {@code CURSOR} function by ordinal.
     *
     * @param inputCount Number of inputs
     * @param ordinal The reference to the relational input
     * @return RexCall to CURSOR function
     */
    public RexNode cursor(int inputCount, int ordinal) {
        if (inputCount <= ordinal || ordinal < 0) {
            throw new IllegalArgumentException("bad input count or ordinal");
        }
        // Refer to the "ordinal"th input as if it were a field
        // (because that's how things are laid out inside a TableFunctionScan)
        final RelNode input = peek(inputCount, ordinal);
        return call(
                SqlStdOperatorTable.CURSOR,
                getRexBuilder().makeInputRef(input.getRowType(), ordinal));
    }

    /** Creates a {@link TableFunctionScan}. */
    public RelBuilder functionScan(SqlOperator operator, int inputCount, RexNode... operands) {
        return functionScan(operator, inputCount, ImmutableList.copyOf(operands));
    }

    /** Creates a {@link TableFunctionScan}. */
    public RelBuilder functionScan(
            SqlOperator operator, int inputCount, Iterable<? extends RexNode> operands) {
        if (inputCount < 0 || inputCount > stack.size()) {
            throw new IllegalArgumentException("bad input count");
        }

        // Gets inputs.
        final List<RelNode> inputs = new ArrayList<>();
        for (int i = 0; i < inputCount; i++) {
            inputs.add(0, build());
        }

        final RexCall call = call(operator, ImmutableList.copyOf(operands));
        final RelNode functionScan =
                struct.tableFunctionScanFactory.createTableFunctionScan(
                        cluster, inputs, call, null, getColumnMappings(operator));
        push(functionScan);
        return this;
    }

    /**
     * Creates a {@link Filter} of an array of predicates.
     *
     * <p>The predicates are combined using AND, and optimized in a similar way to the {@link #and}
     * method. If the result is TRUE no filter is created.
     */
    public RelBuilder filter(RexNode... predicates) {
        return filter(ImmutableSet.of(), ImmutableList.copyOf(predicates));
    }

    /**
     * Creates a {@link Filter} of a list of predicates.
     *
     * <p>The predicates are combined using AND, and optimized in a similar way to the {@link #and}
     * method. If the result is TRUE no filter is created.
     */
    public RelBuilder filter(Iterable<? extends RexNode> predicates) {
        return filter(ImmutableSet.of(), predicates);
    }

    /**
     * Creates a {@link Filter} of a list of correlation variables and an array of predicates.
     *
     * <p>The predicates are combined using AND, and optimized in a similar way to the {@link #and}
     * method. If the result is TRUE no filter is created.
     */
    public RelBuilder filter(Iterable<CorrelationId> variablesSet, RexNode... predicates) {
        return filter(variablesSet, ImmutableList.copyOf(predicates));
    }

    /**
     * Creates a {@link Filter} of a list of correlation variables and a list of predicates.
     *
     * <p>The predicates are combined using AND, and optimized in a similar way to the {@link #and}
     * method. If simplification is on and the result is TRUE, no filter is created.
     */
    public RelBuilder filter(
            Iterable<CorrelationId> variablesSet, Iterable<? extends RexNode> predicates) {
        final RexNode conjunctionPredicates;
        if (config.simplify()) {
            conjunctionPredicates = simplifier.simplifyFilterPredicates(predicates);
        } else {
            conjunctionPredicates = RexUtil.composeConjunction(simplifier.rexBuilder, predicates);
        }

        if (conjunctionPredicates == null || conjunctionPredicates.isAlwaysFalse()) {
            return empty();
        }
        if (conjunctionPredicates.isAlwaysTrue()) {
            return this;
        }

        final Frame frame = stack.pop();
        final RelNode filter =
                struct.filterFactory.createFilter(
                        frame.rel, conjunctionPredicates, ImmutableSet.copyOf(variablesSet));
        stack.push(new Frame(filter, frame.fields));
        return this;
    }

    /** Creates a {@link Project} of the given expressions. */
    public RelBuilder project(RexNode... nodes) {
        return project(ImmutableList.copyOf(nodes));
    }

    /**
     * Creates a {@link Project} of the given list of expressions.
     *
     * <p>Infers names as would {@link #project(Iterable, Iterable)} if all suggested names were
     * null.
     *
     * @param nodes Expressions
     */
    public RelBuilder project(Iterable<? extends RexNode> nodes) {
        return project(nodes, ImmutableList.of());
    }

    /**
     * Creates a {@link Project} of the given list of expressions and field names.
     *
     * @param nodes Expressions
     * @param fieldNames field names for expressions
     */
    public RelBuilder project(
            Iterable<? extends RexNode> nodes, Iterable<? extends @Nullable String> fieldNames) {
        return project(nodes, fieldNames, false);
    }

    /**
     * Creates a {@link Project} of the given list of expressions, using the given names.
     *
     * <p>Names are deduced as follows:
     *
     * <ul>
     *   <li>If the length of {@code fieldNames} is greater than the index of the current entry in
     *       {@code nodes}, and the entry in {@code fieldNames} is not null, uses it; otherwise
     *   <li>If an expression projects an input field, or is a cast an input field, uses the input
     *       field name; otherwise
     *   <li>If an expression is a call to {@link SqlStdOperatorTable#AS} (see {@link #alias}),
     *       removes the call but uses the intended alias.
     * </ul>
     *
     * <p>After the field names have been inferred, makes the field names unique by appending
     * numeric suffixes.
     *
     * @param nodes Expressions
     * @param fieldNames Suggested field names
     * @param force create project even if it is identity
     */
    public RelBuilder project(
            Iterable<? extends RexNode> nodes,
            Iterable<? extends @Nullable String> fieldNames,
            boolean force) {
        return project_(nodes, fieldNames, ImmutableList.of(), force);
    }

    /** Creates a {@link Project} of all original fields, plus the given expressions. */
    public RelBuilder projectPlus(RexNode... nodes) {
        return projectPlus(ImmutableList.copyOf(nodes));
    }

    /** Creates a {@link Project} of all original fields, plus the given list of expressions. */
    public RelBuilder projectPlus(Iterable<RexNode> nodes) {
        return project(Iterables.concat(fields(), nodes));
    }

    /**
     * Creates a {@link Project} of all original fields, except the given expressions.
     *
     * @throws IllegalArgumentException if the given expressions contain duplicates or there is an
     *     expression that does not match an existing field
     */
    public RelBuilder projectExcept(RexNode... expressions) {
        return projectExcept(ImmutableList.copyOf(expressions));
    }

    /**
     * Creates a {@link Project} of all original fields, except the given list of expressions.
     *
     * @throws IllegalArgumentException if the given expressions contain duplicates or there is an
     *     expression that does not match an existing field
     */
    public RelBuilder projectExcept(Iterable<RexNode> expressions) {
        List<RexNode> allExpressions = new ArrayList<>(fields());
        Set<RexNode> excludeExpressions = new HashSet<>();
        for (RexNode excludeExp : expressions) {
            if (!excludeExpressions.add(excludeExp)) {
                throw new IllegalArgumentException(
                        "Input list contains duplicates. Expression "
                                + excludeExp
                                + " exists multiple times.");
            }
            if (!allExpressions.remove(excludeExp)) {
                throw new IllegalArgumentException(
                        "Expression " + excludeExp.toString() + " not found.");
            }
        }
        return this.project(allExpressions);
    }

    /**
     * Creates a {@link Project} of the given list of expressions, using the given names.
     *
     * <p>Names are deduced as follows:
     *
     * <ul>
     *   <li>If the length of {@code fieldNames} is greater than the index of the current entry in
     *       {@code nodes}, and the entry in {@code fieldNames} is not null, uses it; otherwise
     *   <li>If an expression projects an input field, or is a cast an input field, uses the input
     *       field name; otherwise
     *   <li>If an expression is a call to {@link SqlStdOperatorTable#AS} (see {@link #alias}),
     *       removes the call but uses the intended alias.
     * </ul>
     *
     * <p>After the field names have been inferred, makes the field names unique by appending
     * numeric suffixes.
     *
     * @param nodes Expressions
     * @param fieldNames Suggested field names
     * @param hints Hints
     * @param force create project even if it is identity
     */
    private RelBuilder project_(
            Iterable<? extends RexNode> nodes,
            Iterable<? extends @Nullable String> fieldNames,
            Iterable<RelHint> hints,
            boolean force) {
        final Frame frame = requireNonNull(peek_(), "frame stack is empty");
        final RelDataType inputRowType = frame.rel.getRowType();
        final List<RexNode> nodeList = Lists.newArrayList(nodes);

        // Perform a quick check for identity. We'll do a deeper check
        // later when we've derived column names.
        if (!force && Iterables.isEmpty(fieldNames) && RexUtil.isIdentity(nodeList, inputRowType)) {
            return this;
        }

        final List<@Nullable String> fieldNameList = Lists.newArrayList(fieldNames);
        while (fieldNameList.size() < nodeList.size()) {
            fieldNameList.add(null);
        }

        bloat:
        if (frame.rel instanceof Project && config.bloat() >= 0) {
            final Project project = (Project) frame.rel;
            // Populate field names. If the upper expression is an input ref and does
            // not have a recommended name, use the name of the underlying field.
            for (int i = 0; i < fieldNameList.size(); i++) {
                if (fieldNameList.get(i) == null) {
                    final RexNode node = nodeList.get(i);
                    if (node instanceof RexInputRef) {
                        final RexInputRef ref = (RexInputRef) node;
                        fieldNameList.set(
                                i, project.getRowType().getFieldNames().get(ref.getIndex()));
                    }
                }
            }
            final List<RexNode> newNodes =
                    RelOptUtil.pushPastProjectUnlessBloat(nodeList, project, config.bloat());
            if (newNodes == null) {
                // The merged expression is more complex than the input expressions.
                // Do not merge.
                break bloat;
            }

            // Carefully build a list of fields, so that table aliases from the input
            // can be seen for fields that are based on a RexInputRef.
            final Frame frame1 = stack.pop();
            final List<Field> fields = new ArrayList<>();
            for (RelDataTypeField f : project.getInput().getRowType().getFieldList()) {
                fields.add(new Field(ImmutableSet.of(), f));
            }
            for (Pair<RexNode, Field> pair : Pair.zip(project.getProjects(), frame1.fields)) {
                switch (pair.left.getKind()) {
                    case INPUT_REF:
                        final int i = ((RexInputRef) pair.left).getIndex();
                        final Field field = fields.get(i);
                        final ImmutableSet<String> aliases = pair.right.left;
                        fields.set(i, new Field(aliases, field.right));
                        break;
                    default:
                        break;
                }
            }
            stack.push(new Frame(project.getInput(), ImmutableList.copyOf(fields)));
            final ImmutableSet.Builder<RelHint> mergedHints = ImmutableSet.builder();
            mergedHints.addAll(project.getHints());
            mergedHints.addAll(hints);
            return project_(newNodes, fieldNameList, mergedHints.build(), force);
        }

        // Simplify expressions.
        if (config.simplify()) {
            for (int i = 0; i < nodeList.size(); i++) {
                nodeList.set(i, simplifier.simplifyPreservingType(nodeList.get(i)));
            }
        }

        // Replace null names with generated aliases.
        for (int i = 0; i < fieldNameList.size(); i++) {
            if (fieldNameList.get(i) == null) {
                fieldNameList.set(i, inferAlias(nodeList, nodeList.get(i), i));
            }
        }

        final ImmutableList.Builder<Field> fields = ImmutableList.builder();
        final Set<String> uniqueNameList =
                getTypeFactory().getTypeSystem().isSchemaCaseSensitive()
                        ? new HashSet<>()
                        : new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        // calculate final names and build field list
        for (int i = 0; i < fieldNameList.size(); ++i) {
            final RexNode node = nodeList.get(i);
            String name = fieldNameList.get(i);
            String originalName = name;
            Field field;
            if (name == null || uniqueNameList.contains(name)) {
                int j = 0;
                if (name == null) {
                    j = i;
                }
                do {
                    name = SqlValidatorUtil.F_SUGGESTER.apply(originalName, j, j++);
                } while (uniqueNameList.contains(name));
                fieldNameList.set(i, name);
            }
            RelDataTypeField fieldType = new RelDataTypeFieldImpl(name, i, node.getType());
            switch (node.getKind()) {
                case INPUT_REF:
                    // preserve rel aliases for INPUT_REF fields
                    final int index = ((RexInputRef) node).getIndex();
                    field = new Field(frame.fields.get(index).left, fieldType);
                    break;
                default:
                    field = new Field(ImmutableSet.of(), fieldType);
                    break;
            }
            uniqueNameList.add(name);
            fields.add(field);
        }
        if (!force && RexUtil.isIdentity(nodeList, inputRowType)) {
            if (fieldNameList.equals(inputRowType.getFieldNames())) {
                // Do not create an identity project if it does not rename any fields
                return this;
            } else {
                // create "virtual" row type for project only rename fields
                stack.pop();
                // Ignore the hints.
                stack.push(new Frame(frame.rel, fields.build()));
            }
            return this;
        }

        // If the expressions are all literals, and the input is a Values with N
        // rows, replace with a Values with same tuple N times.
        if (config.simplifyValues()
                && frame.rel instanceof Values
                && nodeList.stream().allMatch(e -> e instanceof RexLiteral)) {
            final Values values = (Values) build();
            final RelDataTypeFactory.Builder typeBuilder = getTypeFactory().builder();
            Pair.forEach(
                    fieldNameList,
                    nodeList,
                    (name, expr) -> typeBuilder.add(requireNonNull(name, "name"), expr.getType()));
            @SuppressWarnings({"unchecked", "rawtypes"})
            final List<RexLiteral> tuple = (List<RexLiteral>) (List) nodeList;
            return values(Collections.nCopies(values.tuples.size(), tuple), typeBuilder.build());
        }

        final RelNode project =
                struct.projectFactory.createProject(
                        frame.rel,
                        ImmutableList.copyOf(hints),
                        ImmutableList.copyOf(nodeList),
                        fieldNameList);
        stack.pop();
        stack.push(new Frame(project, fields.build()));
        return this;
    }

    /**
     * Creates a {@link Project} of the given expressions and field names, and optionally
     * optimizing.
     *
     * <p>If {@code fieldNames} is null, or if a particular entry in {@code fieldNames} is null,
     * derives field names from the input expressions.
     *
     * <p>If {@code force} is false, and the input is a {@code Project}, and the expressions make
     * the trivial projection ($0, $1, ...), modifies the input.
     *
     * @param nodes Expressions
     * @param fieldNames Suggested field names, or null to generate
     * @param force Whether to create a renaming Project if the projections are trivial
     */
    public RelBuilder projectNamed(
            Iterable<? extends RexNode> nodes,
            @Nullable Iterable<? extends @Nullable String> fieldNames,
            boolean force) {
        @SuppressWarnings("unchecked")
        final List<? extends RexNode> nodeList =
                nodes instanceof List ? (List) nodes : ImmutableList.copyOf(nodes);
        final List<@Nullable String> fieldNameList =
                fieldNames == null
                        ? null
                        : fieldNames instanceof List
                                ? (List<@Nullable String>) fieldNames
                                : ImmutableNullableList.copyOf(fieldNames);
        final RelNode input = peek();
        final RelDataType rowType =
                RexUtil.createStructType(
                        cluster.getTypeFactory(),
                        nodeList,
                        fieldNameList,
                        SqlValidatorUtil.F_SUGGESTER);
        if (!force && RexUtil.isIdentity(nodeList, input.getRowType())) {
            if (input instanceof Project && fieldNames != null) {
                // Rename columns of child projection if desired field names are given.
                final Frame frame = stack.pop();
                final Project childProject = (Project) frame.rel;
                final Project newInput =
                        childProject.copy(
                                childProject.getTraitSet(),
                                childProject.getInput(),
                                childProject.getProjects(),
                                rowType);
                stack.push(new Frame(newInput.attachHints(childProject.getHints()), frame.fields));
            }
            if (input instanceof Values && fieldNameList != null) {
                // Rename columns of child values if desired field names are given.
                final Frame frame = stack.pop();
                final Values values = (Values) frame.rel;
                final RelDataTypeFactory.Builder typeBuilder = getTypeFactory().builder();
                Pair.forEach(
                        fieldNameList,
                        rowType.getFieldList(),
                        (name, field) ->
                                typeBuilder.add(requireNonNull(name, "name"), field.getType()));
                final RelDataType newRowType = typeBuilder.build();
                final RelNode newValues =
                        struct.valuesFactory.createValues(cluster, newRowType, values.tuples);
                stack.push(new Frame(newValues, frame.fields));
            }
        } else {
            project(nodeList, rowType.getFieldNames(), force);
        }
        return this;
    }

    /**
     * Creates an {@link Uncollect} with given item aliases.
     *
     * @param itemAliases Operand item aliases, never null
     * @param withOrdinality If {@code withOrdinality}, the output contains an extra {@code
     *     ORDINALITY} column
     */
    public RelBuilder uncollect(List<String> itemAliases, boolean withOrdinality) {
        Frame frame = stack.pop();
        stack.push(
                new Frame(
                        new Uncollect(
                                cluster,
                                cluster.traitSetOf(Convention.NONE),
                                frame.rel,
                                withOrdinality,
                                requireNonNull(itemAliases, "itemAliases"))));
        return this;
    }

    /**
     * Ensures that the field names match those given.
     *
     * <p>If all fields have the same name, adds nothing; if any fields do not have the same name,
     * adds a {@link Project}.
     *
     * <p>Note that the names can be short-lived. Other {@code RelBuilder} operations make no
     * guarantees about the field names of the rows they produce.
     *
     * @param fieldNames List of desired field names; may contain null values or have fewer fields
     *     than the current row type
     */
    public RelBuilder rename(List<? extends @Nullable String> fieldNames) {
        final List<String> oldFieldNames = peek().getRowType().getFieldNames();
        Preconditions.checkArgument(
                fieldNames.size() <= oldFieldNames.size(), "More names than fields");
        final List<String> newFieldNames = new ArrayList<>(oldFieldNames);
        for (int i = 0; i < fieldNames.size(); i++) {
            final String s = fieldNames.get(i);
            if (s != null) {
                newFieldNames.set(i, s);
            }
        }
        if (oldFieldNames.equals(newFieldNames)) {
            return this;
        }
        if (peek() instanceof Values) {
            // Special treatment for VALUES. Re-build it rather than add a project.
            final Values v = (Values) build();
            final RelDataTypeFactory.Builder b = getTypeFactory().builder();
            for (Pair<String, RelDataTypeField> p :
                    Pair.zip(newFieldNames, v.getRowType().getFieldList())) {
                b.add(p.left, p.right.getType());
            }
            return values(v.tuples, b.build());
        }

        return project(fields(), newFieldNames, true);
    }

    /**
     * Infers the alias of an expression.
     *
     * <p>If the expression was created by {@link #alias}, replaces the expression in the project
     * list.
     */
    private @Nullable String inferAlias(List<RexNode> exprList, RexNode expr, int i) {
        switch (expr.getKind()) {
            case INPUT_REF:
                final RexInputRef ref = (RexInputRef) expr;
                return requireNonNull(stack.peek(), "empty frame stack")
                        .fields
                        .get(ref.getIndex())
                        .getValue()
                        .getName();
            case CAST:
                return inferAlias(exprList, ((RexCall) expr).getOperands().get(0), -1);
            case AS:
                final RexCall call = (RexCall) expr;
                if (i >= 0) {
                    exprList.set(i, call.getOperands().get(0));
                }
                NlsString value = (NlsString) ((RexLiteral) call.getOperands().get(1)).getValue();
                return castNonNull(value).getValue();
            default:
                return null;
        }
    }

    /** Creates an {@link Aggregate} that makes the relational expression distinct on all fields. */
    public RelBuilder distinct() {
        return aggregate(groupKey(fields()));
    }

    /** Creates an {@link Aggregate} with an array of calls. */
    public RelBuilder aggregate(GroupKey groupKey, AggCall... aggCalls) {
        return aggregate(groupKey, ImmutableList.copyOf(aggCalls));
    }

    /** Creates an {@link Aggregate} with an array of {@link AggregateCall}s. */
    public RelBuilder aggregate(GroupKey groupKey, List<AggregateCall> aggregateCalls) {
        return aggregate(
                groupKey,
                aggregateCalls.stream()
                        .map(
                                aggregateCall ->
                                        new AggCallImpl2(
                                                aggregateCall,
                                                aggregateCall.getArgList().stream()
                                                        .map(this::field)
                                                        .collect(Util.toImmutableList())))
                        .collect(Collectors.toList()));
    }

    /** Creates an {@link Aggregate} with multiple calls. */
    public RelBuilder aggregate(GroupKey groupKey, Iterable<AggCall> aggCalls) {
        final Registrar registrar = new Registrar(fields(), peek().getRowType().getFieldNames());
        final GroupKeyImpl groupKey_ = (GroupKeyImpl) groupKey;
        ImmutableBitSet groupSet =
                ImmutableBitSet.of(registrar.registerExpressions(groupKey_.nodes));
        label:
        if (Iterables.isEmpty(aggCalls)) {
            final RelMetadataQuery mq = peek().getCluster().getMetadataQuery();
            if (groupSet.isEmpty()) {
                final Double minRowCount = mq.getMinRowCount(peek());
                if (minRowCount == null || minRowCount < 1D) {
                    // We can't remove "GROUP BY ()" if there's a chance the rel could be
                    // empty.
                    break label;
                }
            }
            if (registrar.extraNodes.size() == fields().size()) {
                final Boolean unique = mq.areColumnsUnique(peek(), groupSet);
                if (unique != null && unique && !config.aggregateUnique() && groupKey_.isSimple()) {
                    // Rel is already unique.
                    return project(fields(groupSet));
                }
            }
            final Double maxRowCount = mq.getMaxRowCount(peek());
            if (maxRowCount != null
                    && maxRowCount <= 1D
                    && !config.aggregateUnique()
                    && groupKey_.isSimple()) {
                // If there is at most one row, rel is already unique.
                return project(fields(groupSet));
            }
        }

        ImmutableList<ImmutableBitSet> groupSets;
        if (groupKey_.nodeLists != null) {
            final int sizeBefore = registrar.extraNodes.size();
            final List<ImmutableBitSet> groupSetList = new ArrayList<>();
            for (ImmutableList<RexNode> nodeList : groupKey_.nodeLists) {
                final ImmutableBitSet groupSet2 =
                        ImmutableBitSet.of(registrar.registerExpressions(nodeList));
                if (!groupSet.contains(groupSet2)) {
                    throw new IllegalArgumentException(
                            "group set element " + nodeList + " must be a subset of group key");
                }
                groupSetList.add(groupSet2);
            }
            final ImmutableSortedMultiset<ImmutableBitSet> groupSetMultiset =
                    ImmutableSortedMultiset.copyOf(ImmutableBitSet.COMPARATOR, groupSetList);
            if (Iterables.any(aggCalls, RelBuilder::isGroupId)
                    || !ImmutableBitSet.ORDERING.isStrictlyOrdered(groupSetMultiset)) {
                return rewriteAggregateWithDuplicateGroupSets(
                        groupSet, groupSetMultiset, ImmutableList.copyOf(aggCalls));
            }
            groupSets = ImmutableList.copyOf(groupSetMultiset.elementSet());
            if (registrar.extraNodes.size() > sizeBefore) {
                throw new IllegalArgumentException(
                        "group sets contained expressions "
                                + "not in group key: "
                                + Util.skip(registrar.extraNodes, sizeBefore));
            }
        } else {
            groupSets = ImmutableList.of(groupSet);
        }

        for (AggCall aggCall : aggCalls) {
            ((AggCallPlus) aggCall).register(registrar);
        }
        project(registrar.extraNodes);
        rename(registrar.names);
        final Frame frame = stack.pop();
        RelNode r = frame.rel;
        final List<AggregateCall> aggregateCalls = new ArrayList<>();
        for (AggCall aggCall : aggCalls) {
            aggregateCalls.add(((AggCallPlus) aggCall).aggregateCall(registrar, groupSet, r));
        }

        assert ImmutableBitSet.ORDERING.isStrictlyOrdered(groupSets) : groupSets;
        for (ImmutableBitSet set : groupSets) {
            assert groupSet.contains(set);
        }

        List<Field> inFields = frame.fields;
        if (config.pruneInputOfAggregate() && r instanceof Project) {
            final Set<Integer> fieldsUsed = RelOptUtil.getAllFields2(groupSet, aggregateCalls);
            // Some parts of the system can't handle rows with zero fields, so
            // pretend that one field is used.
            if (fieldsUsed.isEmpty()) {
                r = ((Project) r).getInput();
            } else if (fieldsUsed.size() < r.getRowType().getFieldCount()) {
                // Some fields are computed but not used. Prune them.
                final Map<Integer, Integer> map = new HashMap<>();
                for (int source : fieldsUsed) {
                    map.put(source, map.size());
                }

                groupSet = groupSet.permute(map);
                groupSets =
                        ImmutableBitSet.ORDERING.immutableSortedCopy(
                                ImmutableBitSet.permute(groupSets, map));

                final Mappings.TargetMapping targetMapping =
                        Mappings.target(map, r.getRowType().getFieldCount(), fieldsUsed.size());
                final List<AggregateCall> oldAggregateCalls = new ArrayList<>(aggregateCalls);
                aggregateCalls.clear();
                for (AggregateCall aggregateCall : oldAggregateCalls) {
                    aggregateCalls.add(aggregateCall.transform(targetMapping));
                }
                inFields = Mappings.permute(inFields, targetMapping.inverse());

                final Project project = (Project) r;
                final List<RexNode> newProjects = new ArrayList<>();
                final RelDataTypeFactory.Builder builder = cluster.getTypeFactory().builder();
                for (int i : fieldsUsed) {
                    newProjects.add(project.getProjects().get(i));
                    builder.add(project.getRowType().getFieldList().get(i));
                }
                r =
                        project.copy(
                                cluster.traitSet(),
                                project.getInput(),
                                newProjects,
                                builder.build());
            }
        }

        if (!config.dedupAggregateCalls() || Util.isDistinct(aggregateCalls)) {
            return aggregate_(
                    groupSet, groupSets, r, aggregateCalls, registrar.extraNodes, inFields);
        }

        // There are duplicate aggregate calls. Rebuild the list to eliminate
        // duplicates, then add a Project.
        final Set<AggregateCall> callSet = new HashSet<>();
        final List<Pair<Integer, @Nullable String>> projects = new ArrayList<>();
        Util.range(groupSet.cardinality()).forEach(i -> projects.add(Pair.of(i, null)));
        final List<AggregateCall> distinctAggregateCalls = new ArrayList<>();
        for (AggregateCall aggregateCall : aggregateCalls) {
            final int i;
            if (callSet.add(aggregateCall)) {
                i = distinctAggregateCalls.size();
                distinctAggregateCalls.add(aggregateCall);
            } else {
                i = distinctAggregateCalls.indexOf(aggregateCall);
                assert i >= 0;
            }
            projects.add(Pair.of(groupSet.cardinality() + i, aggregateCall.name));
        }
        aggregate_(groupSet, groupSets, r, distinctAggregateCalls, registrar.extraNodes, inFields);
        final List<RexNode> fields =
                projects.stream()
                        .map(p -> p.right == null ? field(p.left) : alias(field(p.left), p.right))
                        .collect(Collectors.toList());
        return project(fields);
    }

    /**
     * Finishes the implementation of {@link #aggregate} by creating an {@link Aggregate} and
     * pushing it onto the stack.
     */
    private RelBuilder aggregate_(
            ImmutableBitSet groupSet,
            ImmutableList<ImmutableBitSet> groupSets,
            RelNode input,
            List<AggregateCall> aggregateCalls,
            List<RexNode> extraNodes,
            List<Field> inFields) {
        final RelNode aggregate =
                struct.aggregateFactory.createAggregate(
                        input, ImmutableList.of(), groupSet, groupSets, aggregateCalls);

        // build field list
        final ImmutableList.Builder<Field> fields = ImmutableList.builder();
        final List<RelDataTypeField> aggregateFields = aggregate.getRowType().getFieldList();
        int i = 0;
        // first, group fields
        for (Integer groupField : groupSet.asList()) {
            RexNode node = extraNodes.get(groupField);
            final SqlKind kind = node.getKind();
            switch (kind) {
                case INPUT_REF:
                    fields.add(inFields.get(((RexInputRef) node).getIndex()));
                    break;
                default:
                    String name = aggregateFields.get(i).getName();
                    RelDataTypeField fieldType = new RelDataTypeFieldImpl(name, i, node.getType());
                    fields.add(new Field(ImmutableSet.of(), fieldType));
                    break;
            }
            i++;
        }
        // second, aggregate fields. retain `i' as field index
        for (int j = 0; j < aggregateCalls.size(); ++j) {
            final AggregateCall call = aggregateCalls.get(j);
            final RelDataTypeField fieldType =
                    new RelDataTypeFieldImpl(
                            aggregateFields.get(i + j).getName(), i + j, call.getType());
            fields.add(new Field(ImmutableSet.of(), fieldType));
        }
        stack.push(new Frame(aggregate, fields.build()));
        return this;
    }

    /**
     * The {@code GROUP_ID()} function is used to distinguish duplicate groups. However, as
     * Aggregate normalizes group sets to canonical form (i.e., flatten, sorting, redundancy
     * removal), this information is lost in RelNode. Therefore, it is impossible to implement the
     * function in runtime.
     *
     * <p>To fill this gap, an aggregation query that contains duplicate group sets is rewritten
     * into a Union of Aggregate operators whose group sets are distinct. The number of inputs to
     * the Union is equal to the maximum number of duplicates. In the {@code N}th input to the
     * Union, calls to the {@code GROUP_ID} aggregate function are replaced by the integer literal
     * {@code N}.
     *
     * <p>This method also handles the case where group sets are distinct but there is a call to
     * {@code GROUP_ID}. That call is replaced by the integer literal {@code 0}.
     *
     * <p>Also see the discussion in <a
     * href="https://issues.apache.org/jira/browse/CALCITE-1824">[CALCITE-1824] GROUP_ID returns
     * wrong result</a> and <a
     * href="https://issues.apache.org/jira/browse/CALCITE-4748">[CALCITE-4748] If there are
     * duplicate GROUPING SETS, Calcite should return duplicate rows</a>.
     */
    private RelBuilder rewriteAggregateWithDuplicateGroupSets(
            ImmutableBitSet groupSet,
            ImmutableSortedMultiset<ImmutableBitSet> groupSets,
            List<AggCall> aggregateCalls) {
        final List<String> fieldNamesIfNoRewrite =
                Aggregate.deriveRowType(
                                getTypeFactory(),
                                peek().getRowType(),
                                false,
                                groupSet,
                                groupSets.asList(),
                                aggregateCalls.stream()
                                        .map(c -> ((AggCallPlus) c).aggregateCall())
                                        .collect(Util.toImmutableList()))
                        .getFieldNames();

        // If n duplicates exist for a particular grouping, the {@code GROUP_ID()}
        // function produces values in the range 0 to n-1. For each value,
        // we need to figure out the corresponding group sets.
        //
        // For example, "... GROUPING SETS (a, a, b, c, c, c, c)"
        // (i) The max value of the GROUP_ID() function returns is 3
        // (ii) GROUPING SETS (a, b, c) produces value 0,
        //      GROUPING SETS (a, c) produces value 1,
        //      GROUPING SETS (c) produces value 2
        //      GROUPING SETS (c) produces value 3
        final Map<Integer, Set<ImmutableBitSet>> groupIdToGroupSets = new HashMap<>();
        int maxGroupId = 0;
        for (Multiset.Entry<ImmutableBitSet> entry : groupSets.entrySet()) {
            int groupId = entry.getCount() - 1;
            if (groupId > maxGroupId) {
                maxGroupId = groupId;
            }
            for (int i = 0; i <= groupId; i++) {
                groupIdToGroupSets
                        .computeIfAbsent(i, k -> Sets.newTreeSet(ImmutableBitSet.COMPARATOR))
                        .add(entry.getElement());
            }
        }

        // AggregateCall list without GROUP_ID function
        final List<AggCall> aggregateCallsWithoutGroupId = new ArrayList<>(aggregateCalls);
        aggregateCallsWithoutGroupId.removeIf(RelBuilder::isGroupId);

        // For each group id value, we first construct an Aggregate without
        // GROUP_ID() function call, and then create a Project node on top of it.
        // The Project adds literal value for group id in right position.
        final Frame frame = stack.pop();
        for (int groupId = 0; groupId <= maxGroupId; groupId++) {
            // Create the Aggregate node without GROUP_ID() call
            stack.push(frame);
            aggregate(
                    groupKey(groupSet, castNonNull(groupIdToGroupSets.get(groupId))),
                    aggregateCallsWithoutGroupId);

            final List<RexNode> selectList = new ArrayList<>();
            final int groupExprLength = groupSet.cardinality();
            // Project fields in group by expressions
            for (int i = 0; i < groupExprLength; i++) {
                selectList.add(field(i));
            }
            // Project fields in aggregate calls
            int groupIdCount = 0;
            for (int i = 0; i < aggregateCalls.size(); i++) {
                if (isGroupId(aggregateCalls.get(i))) {
                    selectList.add(
                            getRexBuilder()
                                    .makeExactLiteral(
                                            BigDecimal.valueOf(groupId),
                                            getTypeFactory().createSqlType(SqlTypeName.BIGINT)));
                    groupIdCount++;
                } else {
                    selectList.add(field(groupExprLength + i - groupIdCount));
                }
            }
            project(selectList, fieldNamesIfNoRewrite);
        }

        return union(true, maxGroupId + 1);
    }

    private static boolean isGroupId(AggCall c) {
        return ((AggCallPlus) c).op().kind == SqlKind.GROUP_ID;
    }

    private RelBuilder setOp(boolean all, SqlKind kind, int n) {
        List<RelNode> inputs = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            inputs.add(0, build());
        }
        switch (kind) {
            case UNION:
            case INTERSECT:
            case EXCEPT:
                if (n < 1) {
                    throw new IllegalArgumentException("bad INTERSECT/UNION/EXCEPT input count");
                }
                break;
            default:
                throw new AssertionError("bad setOp " + kind);
        }

        if (n == 1) {
            return push(inputs.get(0));
        }

        if (config.simplifyValues()
                && kind == UNION
                && inputs.stream().allMatch(r -> r instanceof Values)) {
            List<RelDataType> inputTypes = Util.transform(inputs, RelNode::getRowType);
            RelDataType rowType = getTypeFactory().leastRestrictive(inputTypes);
            requireNonNull(rowType, () -> "leastRestrictive(" + inputTypes + ")");
            final List<List<RexLiteral>> tuples = new ArrayList<>();
            for (RelNode input : inputs) {
                tuples.addAll(((Values) input).tuples);
            }
            final List<List<RexLiteral>> tuples2 = all ? tuples : Util.distinctList(tuples);
            return values(tuples2, rowType);
        }

        return push(struct.setOpFactory.createSetOp(kind, inputs, all));
    }

    /**
     * Creates a {@link Union} of the two most recent relational expressions on the stack.
     *
     * @param all Whether to create UNION ALL
     */
    public RelBuilder union(boolean all) {
        return union(all, 2);
    }

    /**
     * Creates a {@link Union} of the {@code n} most recent relational expressions on the stack.
     *
     * @param all Whether to create UNION ALL
     * @param n Number of inputs to the UNION operator
     */
    public RelBuilder union(boolean all, int n) {
        return setOp(all, UNION, n);
    }

    /**
     * Creates an {@link Intersect} of the two most recent relational expressions on the stack.
     *
     * @param all Whether to create INTERSECT ALL
     */
    public RelBuilder intersect(boolean all) {
        return intersect(all, 2);
    }

    /**
     * Creates an {@link Intersect} of the {@code n} most recent relational expressions on the
     * stack.
     *
     * @param all Whether to create INTERSECT ALL
     * @param n Number of inputs to the INTERSECT operator
     */
    public RelBuilder intersect(boolean all, int n) {
        return setOp(all, SqlKind.INTERSECT, n);
    }

    /**
     * Creates a {@link Minus} of the two most recent relational expressions on the stack.
     *
     * @param all Whether to create EXCEPT ALL
     */
    public RelBuilder minus(boolean all) {
        return minus(all, 2);
    }

    /**
     * Creates a {@link Minus} of the {@code n} most recent relational expressions on the stack.
     *
     * @param all Whether to create EXCEPT ALL
     */
    public RelBuilder minus(boolean all, int n) {
        return setOp(all, SqlKind.EXCEPT, n);
    }

    /**
     * Creates a {@link TableScan} on a {@link TransientTable} with the given name, using as type
     * the top of the stack's type.
     *
     * @param tableName table name
     */
    @Experimental
    public RelBuilder transientScan(String tableName) {
        return this.transientScan(tableName, this.peek().getRowType());
    }

    /**
     * Creates a {@link TableScan} on a {@link TransientTable} with the given name and type.
     *
     * @param tableName table name
     * @param rowType row type of the table
     */
    @Experimental
    public RelBuilder transientScan(String tableName, RelDataType rowType) {
        TransientTable transientTable = new ListTransientTable(tableName, rowType);
        requireNonNull(relOptSchema, "relOptSchema");
        RelOptTable relOptTable =
                RelOptTableImpl.create(
                        relOptSchema, rowType, transientTable, ImmutableList.of(tableName));
        RelNode scan =
                struct.scanFactory.createScan(
                        ViewExpanders.toRelContext(viewExpander, cluster), relOptTable);
        push(scan);
        rename(rowType.getFieldNames());
        return this;
    }

    /**
     * Creates a {@link TableSpool} for the most recent relational expression.
     *
     * @param readType Spool's read type (as described in {@link Spool.Type})
     * @param writeType Spool's write type (as described in {@link Spool.Type})
     * @param table Table to write into
     */
    private RelBuilder tableSpool(Spool.Type readType, Spool.Type writeType, RelOptTable table) {
        RelNode spool = struct.spoolFactory.createTableSpool(peek(), readType, writeType, table);
        replaceTop(spool);
        return this;
    }

    /**
     * Creates a {@link RepeatUnion} associated to a {@link TransientTable} without a maximum number
     * of iterations, i.e. repeatUnion(tableName, all, -1).
     *
     * @param tableName name of the {@link TransientTable} associated to the {@link RepeatUnion}
     * @param all whether duplicates will be considered or not
     */
    @Experimental
    public RelBuilder repeatUnion(String tableName, boolean all) {
        return repeatUnion(tableName, all, -1);
    }

    /**
     * Creates a {@link RepeatUnion} associated to a {@link TransientTable} of the two most recent
     * relational expressions on the stack.
     *
     * <p>Warning: if these relational expressions are not correctly defined, this operation might
     * lead to an infinite loop.
     *
     * <p>The generated {@link RepeatUnion} operates as follows:
     *
     * <ul>
     *   <li>Evaluate its left term once, propagating the results into the {@link TransientTable};
     *   <li>Evaluate its right term (which may contain a {@link TableScan} on the {@link
     *       TransientTable}) over and over until it produces no more results (or until an optional
     *       maximum number of iterations is reached). On each iteration, the results are propagated
     *       into the {@link TransientTable}, overwriting the results from the previous one.
     * </ul>
     *
     * @param tableName Name of the {@link TransientTable} associated to the {@link RepeatUnion}
     * @param all Whether duplicates are considered
     * @param iterationLimit Maximum number of iterations; negative value means no limit
     */
    @Experimental
    public RelBuilder repeatUnion(String tableName, boolean all, int iterationLimit) {
        RelOptTableFinder finder = new RelOptTableFinder(tableName);
        for (int i = 0; i < stack.size(); i++) { // search scan(tableName) in the stack
            peek(i).accept(finder);
            if (finder.relOptTable != null) { // found
                break;
            }
        }
        if (finder.relOptTable == null) {
            throw RESOURCE.tableNotFound(tableName).ex();
        }

        RelNode iterative =
                tableSpool(Spool.Type.LAZY, Spool.Type.LAZY, finder.relOptTable).build();
        RelNode seed = tableSpool(Spool.Type.LAZY, Spool.Type.LAZY, finder.relOptTable).build();
        RelNode repeatUnion =
                struct.repeatUnionFactory.createRepeatUnion(
                        seed, iterative, all, iterationLimit, finder.relOptTable);
        return push(repeatUnion);
    }

    /** Auxiliary class to find a certain RelOptTable based on its name. */
    private static final class RelOptTableFinder extends RelHomogeneousShuttle {
        private @MonotonicNonNull RelOptTable relOptTable = null;
        private final String tableName;

        private RelOptTableFinder(String tableName) {
            this.tableName = tableName;
        }

        @Override
        public RelNode visit(TableScan scan) {
            final RelOptTable scanTable = scan.getTable();
            final List<String> qualifiedName = scanTable.getQualifiedName();
            if (qualifiedName.get(qualifiedName.size() - 1).equals(tableName)) {
                relOptTable = scanTable;
            }
            return super.visit(scan);
        }
    }

    /** Creates a {@link Join} with an array of conditions. */
    public RelBuilder join(JoinRelType joinType, RexNode condition0, RexNode... conditions) {
        return join(joinType, Lists.asList(condition0, conditions));
    }

    /** Creates a {@link Join} with multiple conditions. */
    public RelBuilder join(JoinRelType joinType, Iterable<? extends RexNode> conditions) {
        return join(joinType, and(conditions), ImmutableSet.of());
    }

    /** Creates a {@link Join} with one condition. */
    public RelBuilder join(JoinRelType joinType, RexNode condition) {
        return join(joinType, condition, ImmutableSet.of());
    }

    /** Creates a {@link Join} with correlating variables. */
    public RelBuilder join(
            JoinRelType joinType, RexNode condition, Set<CorrelationId> variablesSet) {
        Frame right = stack.pop();
        final Frame left = stack.pop();
        final RelNode join;
        // FLINK BEGIN MODIFICATION
        // keep behavior of Calcite 1.27.0
        final boolean correlate = variablesSet.size() == 1;
        // FLINK END MODIFICATION
        RexNode postCondition = literal(true);
        if (config.simplify()) {
            // Normalize expanded versions IS NOT DISTINCT FROM so that simplifier does not
            // transform the expression to something unrecognizable
            if (condition instanceof RexCall) {
                condition =
                        RelOptUtil.collapseExpandedIsNotDistinctFromExpr(
                                (RexCall) condition, getRexBuilder());
            }
            condition = simplifier.simplifyUnknownAsFalse(condition);
        }
        if (correlate) {
            final CorrelationId id = Iterables.getOnlyElement(variablesSet);
            // Correlate does not have an ON clause.
            switch (joinType) {
                case LEFT:
                case SEMI:
                case ANTI:
                    // For a LEFT/SEMI/ANTI, predicate must be evaluated first.
                    stack.push(right);
                    filter(condition.accept(new Shifter(left.rel, id, right.rel)));
                    right = stack.pop();
                    break;
                case INNER:
                    // For INNER, we can defer.
                    postCondition = condition;
                    break;
                default:
                    throw new IllegalArgumentException(
                            "Correlated " + joinType + " join is not supported");
            }
            final ImmutableBitSet requiredColumns = RelOptUtil.correlationColumns(id, right.rel);
            join =
                    struct.correlateFactory.createCorrelate(
                            left.rel, right.rel, ImmutableList.of(), id, requiredColumns, joinType);
        } else {
            RelNode join0 =
                    struct.joinFactory.createJoin(
                            left.rel,
                            right.rel,
                            ImmutableList.of(),
                            condition,
                            variablesSet,
                            joinType,
                            false);

            if (join0 instanceof Join && config.pushJoinCondition()) {
                join = RelOptUtil.pushDownJoinConditions((Join) join0, this);
            } else {
                join = join0;
            }
        }
        final ImmutableList.Builder<Field> fields = ImmutableList.builder();
        fields.addAll(left.fields);
        fields.addAll(right.fields);
        stack.push(new Frame(join, fields.build()));
        filter(postCondition);
        return this;
    }

    /**
     * Creates a {@link Correlate} with a {@link CorrelationId} and an array of fields that are used
     * by correlation.
     */
    public RelBuilder correlate(
            JoinRelType joinType, CorrelationId correlationId, RexNode... requiredFields) {
        return correlate(joinType, correlationId, ImmutableList.copyOf(requiredFields));
    }

    /**
     * Creates a {@link Correlate} with a {@link CorrelationId} and a list of fields that are used
     * by correlation.
     */
    public RelBuilder correlate(
            JoinRelType joinType,
            CorrelationId correlationId,
            Iterable<? extends RexNode> requiredFields) {
        Frame right = stack.pop();

        final Registrar registrar = new Registrar(fields(), peek().getRowType().getFieldNames());

        List<Integer> requiredOrdinals =
                registrar.registerExpressions(ImmutableList.copyOf(requiredFields));

        project(registrar.extraNodes);
        rename(registrar.names);
        Frame left = stack.pop();

        final RelNode correlate =
                struct.correlateFactory.createCorrelate(
                        left.rel,
                        right.rel,
                        ImmutableList.of(),
                        correlationId,
                        ImmutableBitSet.of(requiredOrdinals),
                        joinType);

        final ImmutableList.Builder<Field> fields = ImmutableList.builder();
        fields.addAll(left.fields);
        fields.addAll(right.fields);
        stack.push(new Frame(correlate, fields.build()));

        return this;
    }

    /**
     * Creates a {@link Join} using USING syntax.
     *
     * <p>For each of the field names, both left and right inputs must have a field of that name.
     * Constructs a join condition that the left and right fields are equal.
     *
     * @param joinType Join type
     * @param fieldNames Field names
     */
    public RelBuilder join(JoinRelType joinType, String... fieldNames) {
        final List<RexNode> conditions = new ArrayList<>();
        for (String fieldName : fieldNames) {
            conditions.add(equals(field(2, 0, fieldName), field(2, 1, fieldName)));
        }
        return join(joinType, conditions);
    }

    /**
     * Creates a {@link Join} with {@link JoinRelType#SEMI}.
     *
     * <p>A semi-join is a form of join that combines two relational expressions according to some
     * condition, and outputs only rows from the left input for which at least one row from the
     * right input matches. It only outputs columns from the left input, and ignores duplicates on
     * the right.
     *
     * <p>For example, {@code EMP semi-join DEPT} finds all {@code EMP} records that do not have a
     * corresponding {@code DEPT} record, similar to the following SQL:
     *
     * <blockquote>
     *
     * <pre>
     * SELECT * FROM EMP
     * WHERE EXISTS (SELECT 1 FROM DEPT
     *     WHERE DEPT.DEPTNO = EMP.DEPTNO)</pre>
     *
     * </blockquote>
     */
    public RelBuilder semiJoin(Iterable<? extends RexNode> conditions) {
        final Frame right = stack.pop();
        final RelNode semiJoin =
                struct.joinFactory.createJoin(
                        peek(),
                        right.rel,
                        ImmutableList.of(),
                        and(conditions),
                        ImmutableSet.of(),
                        JoinRelType.SEMI,
                        false);
        replaceTop(semiJoin);
        return this;
    }

    /**
     * Creates a {@link Join} with {@link JoinRelType#SEMI}.
     *
     * @see #semiJoin(Iterable)
     */
    public RelBuilder semiJoin(RexNode... conditions) {
        return semiJoin(ImmutableList.copyOf(conditions));
    }

    /**
     * Creates an anti-join.
     *
     * <p>An anti-join is a form of join that combines two relational expressions according to some
     * condition, but outputs only rows from the left input for which no rows from the right input
     * match.
     *
     * <p>For example, {@code EMP anti-join DEPT} finds all {@code EMP} records that do not have a
     * corresponding {@code DEPT} record, similar to the following SQL:
     *
     * <blockquote>
     *
     * <pre>
     * SELECT * FROM EMP
     * WHERE NOT EXISTS (SELECT 1 FROM DEPT
     *     WHERE DEPT.DEPTNO = EMP.DEPTNO)</pre>
     *
     * </blockquote>
     */
    public RelBuilder antiJoin(Iterable<? extends RexNode> conditions) {
        final Frame right = stack.pop();
        final RelNode antiJoin =
                struct.joinFactory.createJoin(
                        peek(),
                        right.rel,
                        ImmutableList.of(),
                        and(conditions),
                        ImmutableSet.of(),
                        JoinRelType.ANTI,
                        false);
        replaceTop(antiJoin);
        return this;
    }

    /**
     * Creates an anti-join.
     *
     * @see #antiJoin(Iterable)
     */
    public RelBuilder antiJoin(RexNode... conditions) {
        return antiJoin(ImmutableList.copyOf(conditions));
    }

    /** Assigns a table alias to the top entry on the stack. */
    public RelBuilder as(final String alias) {
        final Frame pair = stack.pop();
        List<Field> newFields = Util.transform(pair.fields, field -> field.addAlias(alias));
        stack.push(new Frame(pair.rel, ImmutableList.copyOf(newFields)));
        return this;
    }

    /**
     * Creates a {@link Values}.
     *
     * <p>The {@code values} array must have the same number of entries as {@code fieldNames}, or an
     * integer multiple if you wish to create multiple rows.
     *
     * <p>If there are zero rows, or if all values of a any column are null, this method cannot
     * deduce the type of columns. For these cases, call {@link #values(Iterable, RelDataType)}.
     *
     * @param fieldNames Field names
     * @param values Values
     */
    public RelBuilder values(@Nullable String[] fieldNames, @Nullable Object... values) {
        if (fieldNames == null
                || fieldNames.length == 0
                || values.length % fieldNames.length != 0
                || values.length < fieldNames.length) {
            throw new IllegalArgumentException(
                    "Value count must be a positive multiple of field count");
        }
        final int rowCount = values.length / fieldNames.length;
        for (Ord<@Nullable String> fieldName : Ord.zip(fieldNames)) {
            if (allNull(values, fieldName.i, fieldNames.length)) {
                throw new IllegalArgumentException(
                        "All values of field '"
                                + fieldName.e
                                + "' (field index "
                                + fieldName.i
                                + ")"
                                + " are null; cannot deduce type");
            }
        }
        final ImmutableList<ImmutableList<RexLiteral>> tupleList =
                tupleList(fieldNames.length, values);
        assert tupleList.size() == rowCount;
        final List<String> fieldNameList =
                Util.transformIndexed(
                        Arrays.asList(fieldNames),
                        (name, i) -> name != null ? name : SqlUtil.deriveAliasFromOrdinal(i));
        return values(tupleList, fieldNameList);
    }

    private RelBuilder values(List<? extends List<RexLiteral>> tupleList, List<String> fieldNames) {
        final RelDataTypeFactory typeFactory = cluster.getTypeFactory();
        final RelDataTypeFactory.Builder builder = typeFactory.builder();
        Ord.forEach(
                fieldNames,
                (fieldName, i) -> {
                    final RelDataType type =
                            typeFactory.leastRestrictive(
                                    new AbstractList<RelDataType>() {
                                        @Override
                                        public RelDataType get(int index) {
                                            return tupleList.get(index).get(i).getType();
                                        }

                                        @Override
                                        public int size() {
                                            return tupleList.size();
                                        }
                                    });
                    assert type != null : "can't infer type for field " + i + ", " + fieldName;
                    builder.add(fieldName, type);
                });
        final RelDataType rowType = builder.build();
        return values(tupleList, rowType);
    }

    private ImmutableList<ImmutableList<RexLiteral>> tupleList(
            int columnCount, @Nullable Object[] values) {
        final ImmutableList.Builder<ImmutableList<RexLiteral>> listBuilder =
                ImmutableList.builder();
        final List<RexLiteral> valueList = new ArrayList<>();
        for (int i = 0; i < values.length; i++) {
            Object value = values[i];
            valueList.add(literal(value));
            if ((i + 1) % columnCount == 0) {
                listBuilder.add(ImmutableList.copyOf(valueList));
                valueList.clear();
            }
        }
        return listBuilder.build();
    }

    /** Returns whether all values for a given column are null. */
    private static boolean allNull(@Nullable Object[] values, int column, int columnCount) {
        for (int i = column; i < values.length; i += columnCount) {
            if (values[i] != null) {
                return false;
            }
        }
        return true;
    }

    /**
     * Creates a relational expression that reads from an input and throws all of the rows away.
     *
     * <p>Note that this method always pops one relational expression from the stack. {@code
     * values}, in contrast, does not pop any relational expressions, and always produces a leaf.
     *
     * <p>The default implementation creates a {@link Values} with the same specified row type and
     * aliases as the input, and ignores the input entirely. But schema-on-query systems such as
     * Drill might override this method to create a relation expression that retains the input, just
     * to read its schema.
     */
    public RelBuilder empty() {
        final Frame frame = stack.pop();
        final RelNode values =
                struct.valuesFactory.createValues(
                        cluster, frame.rel.getRowType(), ImmutableList.of());
        stack.push(new Frame(values, frame.fields));
        return this;
    }

    /**
     * Creates a {@link Values} with a specified row type.
     *
     * <p>This method can handle cases that {@link #values(String[], Object...)} cannot, such as all
     * values of a column being null, or there being zero rows.
     *
     * @param rowType Row type
     * @param columnValues Values
     */
    public RelBuilder values(RelDataType rowType, Object... columnValues) {
        final ImmutableList<ImmutableList<RexLiteral>> tupleList =
                tupleList(rowType.getFieldCount(), columnValues);
        RelNode values =
                struct.valuesFactory.createValues(
                        cluster, rowType, ImmutableList.copyOf(tupleList));
        push(values);
        return this;
    }

    /**
     * Creates a {@link Values} with a specified row type.
     *
     * <p>This method can handle cases that {@link #values(String[], Object...)} cannot, such as all
     * values of a column being null, or there being zero rows.
     *
     * @param tupleList Tuple list
     * @param rowType Row type
     */
    public RelBuilder values(Iterable<? extends List<RexLiteral>> tupleList, RelDataType rowType) {
        RelNode values = struct.valuesFactory.createValues(cluster, rowType, copy(tupleList));
        push(values);
        return this;
    }

    /**
     * Creates a {@link Values} with a specified row type and zero rows.
     *
     * @param rowType Row type
     */
    public RelBuilder values(RelDataType rowType) {
        return values(ImmutableList.<ImmutableList<RexLiteral>>of(), rowType);
    }

    /**
     * Converts an iterable of lists into an immutable list of immutable lists with the same
     * contents. Returns the same object if possible.
     */
    private static <E> ImmutableList<ImmutableList<E>> copy(Iterable<? extends List<E>> tupleList) {
        final ImmutableList.Builder<ImmutableList<E>> builder = ImmutableList.builder();
        int changeCount = 0;
        for (List<E> literals : tupleList) {
            final ImmutableList<E> literals2 = ImmutableList.copyOf(literals);
            builder.add(literals2);
            if (literals != literals2) {
                ++changeCount;
            }
        }
        if (changeCount == 0 && tupleList instanceof ImmutableList) {
            // don't make a copy if we don't have to
            //noinspection unchecked
            return (ImmutableList<ImmutableList<E>>) tupleList;
        }
        return builder.build();
    }

    /** Creates a limit without a sort. */
    public RelBuilder limit(int offset, int fetch) {
        return sortLimit(offset, fetch, ImmutableList.of());
    }

    /** Creates an Exchange by distribution. */
    public RelBuilder exchange(RelDistribution distribution) {
        RelNode exchange = struct.exchangeFactory.createExchange(peek(), distribution);
        replaceTop(exchange);
        return this;
    }

    /** Creates a SortExchange by distribution and collation. */
    public RelBuilder sortExchange(RelDistribution distribution, RelCollation collation) {
        RelNode exchange =
                struct.sortExchangeFactory.createSortExchange(peek(), distribution, collation);
        replaceTop(exchange);
        return this;
    }

    /**
     * Creates a {@link Sort} by field ordinals.
     *
     * <p>Negative fields mean descending: -1 means field(0) descending, -2 means field(1)
     * descending, etc.
     */
    public RelBuilder sort(int... fields) {
        final ImmutableList.Builder<RexNode> builder = ImmutableList.builder();
        for (int field : fields) {
            builder.add(field < 0 ? desc(field(-field - 1)) : field(field));
        }
        return sortLimit(-1, -1, builder.build());
    }

    /** Creates a {@link Sort} by expressions. */
    public RelBuilder sort(RexNode... nodes) {
        return sortLimit(-1, -1, ImmutableList.copyOf(nodes));
    }

    /** Creates a {@link Sort} by expressions. */
    public RelBuilder sort(Iterable<? extends RexNode> nodes) {
        return sortLimit(-1, -1, nodes);
    }

    /** Creates a {@link Sort} by expressions, with limit and offset. */
    public RelBuilder sortLimit(int offset, int fetch, RexNode... nodes) {
        return sortLimit(offset, fetch, ImmutableList.copyOf(nodes));
    }

    /** Creates a {@link Sort} by specifying collations. */
    public RelBuilder sort(RelCollation collation) {
        final RelNode sort = struct.sortFactory.createSort(peek(), collation, null, null);
        replaceTop(sort);
        return this;
    }

    /**
     * Creates a {@link Sort} by a list of expressions, with limit and offset.
     *
     * @param offset Number of rows to skip; non-positive means don't skip any
     * @param fetch Maximum number of rows to fetch; negative means no limit
     * @param nodes Sort expressions
     */
    public RelBuilder sortLimit(int offset, int fetch, Iterable<? extends RexNode> nodes) {
        final @Nullable RexNode offsetNode = offset <= 0 ? null : literal(offset);
        final @Nullable RexNode fetchNode = fetch < 0 ? null : literal(fetch);
        return sortLimit(offsetNode, fetchNode, nodes);
    }

    /**
     * Creates a {@link Sort} by a list of expressions, with limitNode and offsetNode.
     *
     * @param offsetNode RexLiteral means number of rows to skip is deterministic, RexDynamicParam
     *     means number of rows to skip is dynamic.
     * @param fetchNode RexLiteral means maximum number of rows to fetch is deterministic,
     *     RexDynamicParam mean maximum number is dynamic.
     * @param nodes Sort expressions
     */
    public RelBuilder sortLimit(
            @Nullable RexNode offsetNode,
            @Nullable RexNode fetchNode,
            Iterable<? extends RexNode> nodes) {
        if (offsetNode != null) {
            if (!(offsetNode instanceof RexLiteral || offsetNode instanceof RexDynamicParam)) {
                throw new IllegalArgumentException(
                        "OFFSET node must be RexLiteral or RexDynamicParam");
            }
        }
        if (fetchNode != null) {
            if (!(fetchNode instanceof RexLiteral || fetchNode instanceof RexDynamicParam)) {
                throw new IllegalArgumentException(
                        "FETCH node must be RexLiteral or RexDynamicParam");
            }
        }

        final Registrar registrar = new Registrar(fields(), ImmutableList.of());
        final List<RelFieldCollation> fieldCollations = registrar.registerFieldCollations(nodes);
        final int fetch = fetchNode instanceof RexLiteral ? RexLiteral.intValue(fetchNode) : -1;
        if (offsetNode == null && fetch == 0 && config.simplifyLimit()) {
            return empty();
        }
        if (offsetNode == null && fetchNode == null && fieldCollations.isEmpty()) {
            return this; // sort is trivial
        }

        if (fieldCollations.isEmpty()) {
            assert registrar.addedFieldCount() == 0;
            RelNode top = peek();
            if (top instanceof Sort) {
                final Sort sort2 = (Sort) top;
                if (sort2.offset == null && sort2.fetch == null) {
                    replaceTop(sort2.getInput());
                    final RelNode sort =
                            struct.sortFactory.createSort(
                                    peek(), sort2.collation, offsetNode, fetchNode);
                    replaceTop(sort);
                    return this;
                }
            }
            if (top instanceof Project) {
                final Project project = (Project) top;
                if (project.getInput() instanceof Sort) {
                    final Sort sort2 = (Sort) project.getInput();
                    if (sort2.offset == null && sort2.fetch == null) {
                        final RelNode sort =
                                struct.sortFactory.createSort(
                                        sort2.getInput(), sort2.collation, offsetNode, fetchNode);
                        replaceTop(
                                struct.projectFactory.createProject(
                                        sort,
                                        project.getHints(),
                                        project.getProjects(),
                                        Pair.right(project.getNamedProjects())));
                        return this;
                    }
                }
            }
        }
        if (registrar.addedFieldCount() > 0) {
            project(registrar.extraNodes);
        }
        final RelNode sort =
                struct.sortFactory.createSort(
                        peek(), RelCollations.of(fieldCollations), offsetNode, fetchNode);
        replaceTop(sort);
        if (registrar.addedFieldCount() > 0) {
            project(registrar.originalExtraNodes);
        }
        return this;
    }

    private static RelFieldCollation collation(
            RexNode node,
            RelFieldCollation.Direction direction,
            RelFieldCollation.NullDirection nullDirection,
            List<RexNode> extraNodes) {
        switch (node.getKind()) {
            case INPUT_REF:
                return new RelFieldCollation(
                        ((RexInputRef) node).getIndex(),
                        direction,
                        Util.first(nullDirection, direction.defaultNullDirection()));
            case DESCENDING:
                return collation(
                        ((RexCall) node).getOperands().get(0),
                        RelFieldCollation.Direction.DESCENDING,
                        nullDirection,
                        extraNodes);
            case NULLS_FIRST:
                return collation(
                        ((RexCall) node).getOperands().get(0),
                        direction,
                        RelFieldCollation.NullDirection.FIRST,
                        extraNodes);
            case NULLS_LAST:
                return collation(
                        ((RexCall) node).getOperands().get(0),
                        direction,
                        RelFieldCollation.NullDirection.LAST,
                        extraNodes);
            default:
                final int fieldIndex = extraNodes.size();
                extraNodes.add(node);
                return new RelFieldCollation(
                        fieldIndex,
                        direction,
                        Util.first(nullDirection, direction.defaultNullDirection()));
        }
    }

    private static RexFieldCollation rexCollation(
            RexNode node,
            RelFieldCollation.Direction direction,
            RelFieldCollation.NullDirection nullDirection) {
        switch (node.getKind()) {
            case DESCENDING:
                return rexCollation(
                        ((RexCall) node).operands.get(0),
                        RelFieldCollation.Direction.DESCENDING,
                        nullDirection);
            case NULLS_LAST:
                return rexCollation(
                        ((RexCall) node).operands.get(0),
                        direction,
                        RelFieldCollation.NullDirection.LAST);
            case NULLS_FIRST:
                return rexCollation(
                        ((RexCall) node).operands.get(0),
                        direction,
                        RelFieldCollation.NullDirection.FIRST);
            default:
                final Set<SqlKind> flags = EnumSet.noneOf(SqlKind.class);
                if (direction == RelFieldCollation.Direction.DESCENDING) {
                    flags.add(SqlKind.DESCENDING);
                }
                if (nullDirection == RelFieldCollation.NullDirection.FIRST) {
                    flags.add(SqlKind.NULLS_FIRST);
                }
                if (nullDirection == RelFieldCollation.NullDirection.LAST) {
                    flags.add(SqlKind.NULLS_LAST);
                }
                return new RexFieldCollation(node, flags);
        }
    }

    /**
     * Creates a projection that converts the current relational expression's output to a desired
     * row type.
     *
     * <p>The desired row type and the row type to be converted must have the same number of fields.
     *
     * @param castRowType row type after cast
     * @param rename if true, use field names from castRowType; if false, preserve field names from
     *     rel
     */
    public RelBuilder convert(RelDataType castRowType, boolean rename) {
        final RelNode r = build();
        final RelNode r2 = RelOptUtil.createCastRel(r, castRowType, rename, struct.projectFactory);
        push(r2);
        return this;
    }

    public RelBuilder permute(Mapping mapping) {
        assert mapping.getMappingType().isSingleSource();
        assert mapping.getMappingType().isMandatorySource();
        if (mapping.isIdentity()) {
            return this;
        }
        final List<RexNode> exprList = new ArrayList<>();
        for (int i = 0; i < mapping.getTargetCount(); i++) {
            exprList.add(field(mapping.getSource(i)));
        }
        return project(exprList);
    }

    /** Creates a {@link Match}. */
    public RelBuilder match(
            RexNode pattern,
            boolean strictStart,
            boolean strictEnd,
            Map<String, RexNode> patternDefinitions,
            Iterable<? extends RexNode> measureList,
            RexNode after,
            Map<String, ? extends SortedSet<String>> subsets,
            boolean allRows,
            Iterable<? extends RexNode> partitionKeys,
            Iterable<? extends RexNode> orderKeys,
            RexNode interval) {
        final Registrar registrar = new Registrar(fields(), peek().getRowType().getFieldNames());
        final List<RelFieldCollation> fieldCollations =
                registrar.registerFieldCollations(orderKeys);

        final ImmutableBitSet partitionBitSet =
                ImmutableBitSet.of(registrar.registerExpressions(partitionKeys));

        final RelDataTypeFactory.Builder typeBuilder = cluster.getTypeFactory().builder();
        for (RexNode partitionKey : partitionKeys) {
            typeBuilder.add(partitionKey.toString(), partitionKey.getType());
        }
        if (allRows) {
            for (RexNode orderKey : orderKeys) {
                if (!typeBuilder.nameExists(orderKey.toString())) {
                    typeBuilder.add(orderKey.toString(), orderKey.getType());
                }
            }

            final RelDataType inputRowType = peek().getRowType();
            for (RelDataTypeField fs : inputRowType.getFieldList()) {
                if (!typeBuilder.nameExists(fs.getName())) {
                    typeBuilder.add(fs);
                }
            }
        }

        final ImmutableMap.Builder<String, RexNode> measures = ImmutableMap.builder();
        for (RexNode measure : measureList) {
            List<RexNode> operands = ((RexCall) measure).getOperands();
            String alias = operands.get(1).toString();
            typeBuilder.add(alias, operands.get(0).getType());
            measures.put(alias, operands.get(0));
        }

        final RelNode match =
                struct.matchFactory.createMatch(
                        peek(),
                        pattern,
                        typeBuilder.build(),
                        strictStart,
                        strictEnd,
                        patternDefinitions,
                        measures.build(),
                        after,
                        subsets,
                        allRows,
                        partitionBitSet,
                        RelCollations.of(fieldCollations),
                        interval);
        stack.push(new Frame(match));
        return this;
    }

    /**
     * Creates a Pivot.
     *
     * <p>To achieve the same effect as the SQL
     *
     * <blockquote>
     *
     * <pre>{@code
     * SELECT *
     * FROM (SELECT mgr, deptno, job, sal FROM emp)
     * PIVOT (SUM(sal) AS ss, COUNT(*) AS c
     *     FOR (job, deptno)
     *     IN (('CLERK', 10) AS c10, ('MANAGER', 20) AS m20))
     * }</pre>
     *
     * </blockquote>
     *
     * <p>use the builder as follows:
     *
     * <blockquote>
     *
     * <pre>{@code
     * RelBuilder b;
     * b.scan("EMP");
     * final RelBuilder.GroupKey groupKey = b.groupKey("MGR");
     * final List<RelBuilder.AggCall> aggCalls =
     *     Arrays.asList(b.sum(b.field("SAL")).as("SS"),
     *         b.count().as("C"));
     * final List<RexNode> axes =
     *     Arrays.asList(b.field("JOB"),
     *         b.field("DEPTNO"));
     * final ImmutableMap.Builder<String, List<RexNode>> valueMap =
     *     ImmutableMap.builder();
     * valueMap.put("C10",
     *     Arrays.asList(b.literal("CLERK"), b.literal(10)));
     * valueMap.put("M20",
     *     Arrays.asList(b.literal("MANAGER"), b.literal(20)));
     * b.pivot(groupKey, aggCalls, axes, valueMap.build().entrySet());
     * }</pre>
     *
     * </blockquote>
     *
     * <p>Note that the SQL uses a sub-query to project away columns (e.g. {@code HIREDATE}) that it
     * does not reference, so that they do not appear in the {@code GROUP BY}. You do not need to do
     * that in this API, because the {@code groupKey} parameter specifies the keys.
     *
     * <p>Pivot is implemented by desugaring. The above example becomes the following:
     *
     * <blockquote>
     *
     * <pre>{@code
     * SELECT mgr,
     *     SUM(sal) FILTER (WHERE job = 'CLERK' AND deptno = 10) AS c10_ss,
     *     COUNT(*) FILTER (WHERE job = 'CLERK' AND deptno = 10) AS c10_c,
     *     SUM(sal) FILTER (WHERE job = 'MANAGER' AND deptno = 20) AS m20_ss,
     *      COUNT(*) FILTER (WHERE job = 'MANAGER' AND deptno = 20) AS m20_c
     * FROM emp
     * GROUP BY mgr
     * }</pre>
     *
     * </blockquote>
     *
     * @param groupKey Key columns
     * @param aggCalls Aggregate expressions to compute for each value
     * @param axes Columns to pivot
     * @param values Values to pivot, and the alias for each column group
     * @return this RelBuilder
     */
    public RelBuilder pivot(
            GroupKey groupKey,
            Iterable<? extends AggCall> aggCalls,
            Iterable<? extends RexNode> axes,
            Iterable<? extends Map.Entry<String, ? extends Iterable<? extends RexNode>>> values) {
        final List<RexNode> axisList = ImmutableList.copyOf(axes);
        final List<AggCall> multipliedAggCalls = new ArrayList<>();
        Pair.forEach(
                values,
                (alias, expressions) -> {
                    final List<RexNode> expressionList = ImmutableList.copyOf(expressions);
                    if (expressionList.size() != axisList.size()) {
                        throw new IllegalArgumentException(
                                "value count must match axis count ["
                                        + expressionList
                                        + "], ["
                                        + axisList
                                        + "]");
                    }
                    aggCalls.forEach(
                            aggCall -> {
                                final String alias2 = alias + "_" + ((AggCallPlus) aggCall).alias();
                                final List<RexNode> filters = new ArrayList<>();
                                Pair.forEach(
                                        axisList,
                                        expressionList,
                                        (axis, expression) ->
                                                filters.add(equals(axis, expression)));
                                multipliedAggCalls.add(aggCall.filter(and(filters)).as(alias2));
                            });
                });
        return aggregate(groupKey, multipliedAggCalls);
    }

    /**
     * Creates an Unpivot.
     *
     * <p>To achieve the same effect as the SQL
     *
     * <blockquote>
     *
     * <pre>{@code
     * SELECT *
     * FROM (SELECT deptno, job, sal, comm FROM emp)
     *   UNPIVOT INCLUDE NULLS (remuneration
     *     FOR remuneration_type IN (comm AS 'commission',
     *                               sal AS 'salary'))
     * }</pre>
     *
     * </blockquote>
     *
     * <p>use the builder as follows:
     *
     * <blockquote>
     *
     * <pre>{@code
     * RelBuilder b;
     * b.scan("EMP");
     * final List<String> measureNames = Arrays.asList("REMUNERATION");
     * final List<String> axisNames = Arrays.asList("REMUNERATION_TYPE");
     * final Map<List<RexLiteral>, List<RexNode>> axisMap =
     *     ImmutableMap.<List<RexLiteral>, List<RexNode>>builder()
     *         .put(Arrays.asList(b.literal("commission")),
     *             Arrays.asList(b.field("COMM")))
     *         .put(Arrays.asList(b.literal("salary")),
     *             Arrays.asList(b.field("SAL")))
     *         .build();
     * b.unpivot(false, measureNames, axisNames, axisMap);
     * }</pre>
     *
     * </blockquote>
     *
     * <p>The query generates two columns: {@code remuneration_type} (an axis column) and {@code
     * remuneration} (a measure column). Axis columns contain values to indicate the source of the
     * row (in this case, {@code 'salary'} if the row came from the {@code sal} column, and {@code
     * 'commission'} if the row came from the {@code comm} column).
     *
     * @param includeNulls Whether to include NULL values in the output
     * @param measureNames Names of columns to be generated to hold pivoted measures
     * @param axisNames Names of columns to be generated to hold qualifying values
     * @param axisMap Mapping from the columns that hold measures to the values that the axis
     *     columns will hold in the generated rows
     * @return This RelBuilder
     */
    public RelBuilder unpivot(
            boolean includeNulls,
            Iterable<String> measureNames,
            Iterable<String> axisNames,
            Iterable<
                            ? extends
                                    Map.Entry<
                                            ? extends List<? extends RexLiteral>,
                                            ? extends List<? extends RexNode>>>
                    axisMap) {
        // Make immutable copies of all arguments.
        final List<String> measureNameList = ImmutableList.copyOf(measureNames);
        final List<String> axisNameList = ImmutableList.copyOf(axisNames);
        final List<Pair<List<RexLiteral>, List<RexNode>>> map =
                StreamSupport.stream(axisMap.spliterator(), false)
                        .map(
                                pair ->
                                        Pair.<List<RexLiteral>, List<RexNode>>of(
                                                ImmutableList.<RexLiteral>copyOf(pair.getKey()),
                                                ImmutableList.<RexNode>copyOf(pair.getValue())))
                        .collect(Util.toImmutableList());

        // Check that counts match.
        Pair.forEach(
                map,
                (valueList, inputMeasureList) -> {
                    if (inputMeasureList.size() != measureNameList.size()) {
                        throw new IllegalArgumentException(
                                "Number of measures ("
                                        + inputMeasureList.size()
                                        + ") must match number of measure names ("
                                        + measureNameList.size()
                                        + ")");
                    }
                    if (valueList.size() != axisNameList.size()) {
                        throw new IllegalArgumentException(
                                "Number of axis values ("
                                        + valueList.size()
                                        + ") match match number of axis names ("
                                        + axisNameList.size()
                                        + ")");
                    }
                });

        final RelDataType leftRowType = peek().getRowType();
        final BitSet usedFields = new BitSet();
        Pair.forEach(
                map,
                (aliases, nodes) ->
                        nodes.forEach(
                                node -> {
                                    if (node instanceof RexInputRef) {
                                        usedFields.set(((RexInputRef) node).getIndex());
                                    }
                                }));

        // Create "VALUES (('commission'), ('salary')) AS t (remuneration_type)"
        values(ImmutableList.copyOf(Pair.left(map)), axisNameList);

        join(JoinRelType.INNER);

        final ImmutableBitSet unusedFields =
                ImmutableBitSet.range(leftRowType.getFieldCount())
                        .except(ImmutableBitSet.fromBitSet(usedFields));
        final List<RexNode> projects = new ArrayList<>(fields(unusedFields));
        Ord.forEach(
                axisNameList,
                (dimensionName, d) ->
                        projects.add(alias(field(leftRowType.getFieldCount() + d), dimensionName)));

        final List<RexNode> conditions = new ArrayList<>();
        Ord.forEach(
                measureNameList,
                (measureName, m) -> {
                    final List<RexNode> caseOperands = new ArrayList<>();
                    Pair.forEach(
                            map,
                            (literals, nodes) -> {
                                Ord.forEach(
                                        literals,
                                        (literal, d) ->
                                                conditions.add(
                                                        equals(
                                                                field(
                                                                        leftRowType.getFieldCount()
                                                                                + d),
                                                                literal)));
                                caseOperands.add(and(conditions));
                                conditions.clear();
                                caseOperands.add(nodes.get(m));
                            });
                    caseOperands.add(literal(null));
                    projects.add(alias(call(SqlStdOperatorTable.CASE, caseOperands), measureName));
                });
        project(projects);

        if (!includeNulls) {
            // Add 'WHERE m1 IS NOT NULL OR m2 IS NOT NULL'
            final BitSet notNullFields = new BitSet();
            Ord.forEach(
                    measureNameList,
                    (measureName, m) -> {
                        final int f = unusedFields.cardinality() + axisNameList.size() + m;
                        conditions.add(isNotNull(field(f)));
                        notNullFields.set(f);
                    });
            filter(or(conditions));
            if (measureNameList.size() == 1) {
                // If there is one field, EXCLUDE NULLS will have converted it to NOT
                // NULL.
                final RelDataTypeFactory.Builder builder = getTypeFactory().builder();
                peek().getRowType()
                        .getFieldList()
                        .forEach(
                                field -> {
                                    final RelDataType type = field.getType();
                                    builder.add(
                                            field.getName(),
                                            notNullFields.get(field.getIndex())
                                                    ? getTypeFactory()
                                                            .createTypeWithNullability(type, false)
                                                    : type);
                                });
                convert(builder.build(), false);
            }
            conditions.clear();
        }

        return this;
    }

    /**
     * Attaches an array of hints to the stack top relational expression.
     *
     * <p>The redundant hints would be eliminated.
     *
     * @param hints Hints
     * @throws AssertionError if the top relational expression does not implement {@link
     *     org.apache.calcite.rel.hint.Hintable}
     */
    public RelBuilder hints(RelHint... hints) {
        return hints(ImmutableList.copyOf(hints));
    }

    /**
     * Attaches multiple hints to the stack top relational expression.
     *
     * <p>The redundant hints would be eliminated.
     *
     * @param hints Hints
     * @throws AssertionError if the top relational expression does not implement {@link
     *     org.apache.calcite.rel.hint.Hintable}
     */
    public RelBuilder hints(Iterable<RelHint> hints) {
        requireNonNull(hints, "hints");
        final List<RelHint> relHintList =
                hints instanceof List ? (List<RelHint>) hints : Lists.newArrayList(hints);
        if (relHintList.isEmpty()) {
            return this;
        }
        final Frame frame = peek_();
        assert frame != null : "There is no relational expression to attach the hints";
        assert frame.rel instanceof Hintable : "The top relational expression is not a Hintable";
        Hintable hintable = (Hintable) frame.rel;
        replaceTop(hintable.attachHints(relHintList));
        return this;
    }

    /**
     * Clears the stack.
     *
     * <p>The builder's state is now the same as when it was created.
     */
    public void clear() {
        stack.clear();
    }

    /**
     * Information necessary to create a call to an aggregate function.
     *
     * @see RelBuilder#aggregateCall
     */
    public interface AggCall {
        /** Returns a copy of this AggCall that applies a filter before aggregating values. */
        AggCall filter(@Nullable RexNode condition);

        /**
         * Returns a copy of this AggCall that sorts its input values by {@code orderKeys} before
         * aggregating, as in SQL's {@code WITHIN GROUP} clause.
         */
        AggCall sort(Iterable<RexNode> orderKeys);

        /**
         * Returns a copy of this AggCall that sorts its input values by {@code orderKeys} before
         * aggregating, as in SQL's {@code WITHIN GROUP} clause.
         */
        default AggCall sort(RexNode... orderKeys) {
            return sort(ImmutableList.copyOf(orderKeys));
        }

        /**
         * Returns a copy of this AggCall that makes its input values unique by {@code distinctKeys}
         * before aggregating, as in SQL's {@code WITHIN DISTINCT} clause.
         */
        AggCall unique(@Nullable Iterable<RexNode> distinctKeys);

        /**
         * Returns a copy of this AggCall that makes its input values unique by {@code distinctKeys}
         * before aggregating, as in SQL's {@code WITHIN DISTINCT} clause.
         */
        default AggCall unique(RexNode... distinctKeys) {
            return unique(ImmutableList.copyOf(distinctKeys));
        }

        /**
         * Returns a copy of this AggCall that may return approximate results if {@code approximate}
         * is true.
         */
        AggCall approximate(boolean approximate);

        /** Returns a copy of this AggCall that ignores nulls. */
        AggCall ignoreNulls(boolean ignoreNulls);

        /** Returns a copy of this AggCall with a given alias. */
        AggCall as(@Nullable String alias);

        /** Returns a copy of this AggCall that is optionally distinct. */
        AggCall distinct(boolean distinct);

        /** Returns a copy of this AggCall that is distinct. */
        default AggCall distinct() {
            return distinct(true);
        }

        /** Converts this aggregate call to a windowed aggregate call. */
        OverCall over();
    }

    /** Internal methods shared by all implementations of {@link AggCall}. */
    private interface AggCallPlus extends AggCall {
        /** Returns the aggregate function. */
        SqlAggFunction op();

        /** Returns the alias. */
        @Nullable
        String alias();

        /**
         * Returns an {@link AggregateCall} that is approximately equivalent to this {@code AggCall}
         * and is good for certain things, such as deriving field names.
         */
        AggregateCall aggregateCall();

        /** Converts this {@code AggCall} to a good {@link AggregateCall}. */
        AggregateCall aggregateCall(Registrar registrar, ImmutableBitSet groupSet, RelNode r);

        /** Registers expressions in operands and filters. */
        void register(Registrar registrar);
    }

    /**
     * Information necessary to create the GROUP BY clause of an Aggregate.
     *
     * @see RelBuilder#groupKey
     */
    public interface GroupKey {
        /**
         * Assigns an alias to this group key.
         *
         * <p>Used to assign field names in the {@code group} operation.
         */
        GroupKey alias(@Nullable String alias);

        /** Returns the number of columns in the group key. */
        int groupKeyCount();
    }

    /** Implementation of {@link RelBuilder.GroupKey}. */
    static class GroupKeyImpl implements GroupKey {
        final ImmutableList<RexNode> nodes;
        final @Nullable ImmutableList<ImmutableList<RexNode>> nodeLists;
        final @Nullable String alias;

        GroupKeyImpl(
                ImmutableList<RexNode> nodes,
                @Nullable ImmutableList<ImmutableList<RexNode>> nodeLists,
                @Nullable String alias) {
            this.nodes = requireNonNull(nodes, "nodes");
            this.nodeLists = nodeLists;
            this.alias = alias;
        }

        @Override
        public String toString() {
            return alias == null ? nodes.toString() : nodes + " as " + alias;
        }

        @Override
        public int groupKeyCount() {
            return nodes.size();
        }

        @Override
        public GroupKey alias(@Nullable String alias) {
            return Objects.equals(this.alias, alias)
                    ? this
                    : new GroupKeyImpl(nodes, nodeLists, alias);
        }

        boolean isSimple() {
            return nodeLists == null || nodeLists.size() == 1;
        }
    }

    /**
     * Checks for {@link CorrelationId}, then validates the id is not used on left, and finally
     * checks if id is actually used on right.
     *
     * @return true if a correlate id is present and used
     * @throws IllegalArgumentException if the {@link CorrelationId} is used by left side or if the
     *     a {@link CorrelationId} is present and the {@link JoinRelType} is FULL or RIGHT.
     */
    private static boolean checkIfCorrelated(
            Set<CorrelationId> variablesSet,
            JoinRelType joinType,
            RelNode leftNode,
            RelNode rightRel) {
        if (variablesSet.size() != 1) {
            return false;
        }
        CorrelationId id = Iterables.getOnlyElement(variablesSet);
        if (!RelOptUtil.notContainsCorrelation(leftNode, id, Litmus.IGNORE)) {
            throw new IllegalArgumentException(
                    "variable " + id + " must not be used by left input to correlation");
        }
        switch (joinType) {
            case RIGHT:
            case FULL:
                throw new IllegalArgumentException(
                        "Correlated " + joinType + " join is not supported");
            default:
                return !RelOptUtil.correlationColumns(
                                Iterables.getOnlyElement(variablesSet), rightRel)
                        .isEmpty();
        }
    }

    /** Implementation of {@link AggCall}. */
    private class AggCallImpl implements AggCallPlus {
        private final SqlAggFunction aggFunction;
        private final boolean distinct;
        private final boolean approximate;
        private final boolean ignoreNulls;
        private final @Nullable RexNode filter;
        private final @Nullable String alias;
        private final ImmutableList<RexNode> operands; // may be empty
        private final @Nullable ImmutableList<RexNode> distinctKeys; // may be empty or null
        private final ImmutableList<RexNode> orderKeys; // may be empty

        AggCallImpl(
                SqlAggFunction aggFunction,
                boolean distinct,
                boolean approximate,
                boolean ignoreNulls,
                @Nullable RexNode filter,
                @Nullable String alias,
                ImmutableList<RexNode> operands,
                @Nullable ImmutableList<RexNode> distinctKeys,
                ImmutableList<RexNode> orderKeys) {
            this.aggFunction = requireNonNull(aggFunction, "aggFunction");
            // If the aggregate function ignores DISTINCT,
            // make the DISTINCT flag FALSE.
            this.distinct = distinct && aggFunction.getDistinctOptionality() != Optionality.IGNORED;
            this.approximate = approximate;
            this.ignoreNulls = ignoreNulls;
            this.alias = alias;
            this.operands = requireNonNull(operands, "operands");
            this.distinctKeys = distinctKeys;
            this.orderKeys = requireNonNull(orderKeys, "orderKeys");
            if (filter != null) {
                if (filter.getType().getSqlTypeName() != SqlTypeName.BOOLEAN) {
                    throw RESOURCE.filterMustBeBoolean().ex();
                }
                if (filter.getType().isNullable()) {
                    filter = call(SqlStdOperatorTable.IS_TRUE, filter);
                }
            }
            this.filter = filter;
        }

        @Override
        public String toString() {
            final StringBuilder b = new StringBuilder();
            b.append(aggFunction.getName()).append('(');
            if (distinct) {
                b.append("DISTINCT ");
            }
            if (operands.size() > 0) {
                b.append(operands.get(0));
                for (int i = 1; i < operands.size(); i++) {
                    b.append(", ");
                    b.append(operands.get(i));
                }
            }
            b.append(')');
            if (filter != null) {
                b.append(" FILTER (WHERE ").append(filter).append(')');
            }
            if (distinctKeys != null) {
                b.append(" WITHIN DISTINCT (").append(distinctKeys).append(')');
            }
            return b.toString();
        }

        @Override
        public SqlAggFunction op() {
            return aggFunction;
        }

        @Override
        public @Nullable String alias() {
            return alias;
        }

        @Override
        public AggregateCall aggregateCall() {
            // Use dummy values for collation and type. This method only promises to
            // return a call that is "approximately equivalent ... and is good for
            // deriving field names", so dummy values are good enough.
            final RelCollation collation = RelCollations.EMPTY;
            final RelDataType type = getTypeFactory().createSqlType(SqlTypeName.BOOLEAN);
            return AggregateCall.create(
                    aggFunction,
                    distinct,
                    approximate,
                    ignoreNulls,
                    ImmutableList.of(),
                    -1,
                    null,
                    collation,
                    type,
                    alias);
        }

        @Override
        public AggregateCall aggregateCall(
                Registrar registrar, ImmutableBitSet groupSet, RelNode r) {
            List<Integer> args = registrar.registerExpressions(this.operands);
            final int filterArg =
                    this.filter == null ? -1 : registrar.registerExpression(this.filter);
            if (this.distinct && !this.aggFunction.isQuantifierAllowed()) {
                throw new IllegalArgumentException("DISTINCT not allowed");
            }
            if (this.filter != null && !this.aggFunction.allowsFilter()) {
                throw new IllegalArgumentException("FILTER not allowed");
            }
            final @Nullable ImmutableBitSet distinctKeys =
                    this.distinctKeys == null
                            ? null
                            : ImmutableBitSet.of(registrar.registerExpressions(this.distinctKeys));
            final RelCollation collation =
                    RelCollations.of(
                            this.orderKeys.stream()
                                    .map(
                                            orderKey ->
                                                    collation(
                                                            orderKey,
                                                            RelFieldCollation.Direction.ASCENDING,
                                                            null,
                                                            Collections.emptyList()))
                                    .collect(Collectors.toList()));
            if (aggFunction instanceof SqlCountAggFunction && !distinct) {
                args = args.stream().filter(r::fieldIsNullable).collect(Util.toImmutableList());
            }
            return AggregateCall.create(
                    aggFunction,
                    distinct,
                    approximate,
                    ignoreNulls,
                    args,
                    filterArg,
                    distinctKeys,
                    collation,
                    groupSet.cardinality(),
                    r,
                    null,
                    alias);
        }

        @Override
        public void register(Registrar registrar) {
            registrar.registerExpressions(operands);
            if (filter != null) {
                registrar.registerExpression(filter);
            }
            if (distinctKeys != null) {
                registrar.registerExpressions(distinctKeys);
            }
            registrar.registerExpressions(orderKeys);
        }

        @Override
        public OverCall over() {
            return new OverCallImpl(aggFunction, distinct, operands, ignoreNulls, alias);
        }

        @Override
        public AggCall sort(Iterable<RexNode> orderKeys) {
            final ImmutableList<RexNode> orderKeyList = ImmutableList.copyOf(orderKeys);
            return orderKeyList.equals(this.orderKeys)
                    ? this
                    : new AggCallImpl(
                            aggFunction,
                            distinct,
                            approximate,
                            ignoreNulls,
                            filter,
                            alias,
                            operands,
                            distinctKeys,
                            orderKeyList);
        }

        @Override
        public AggCall sort(RexNode... orderKeys) {
            return sort(ImmutableList.copyOf(orderKeys));
        }

        @Override
        public AggCall unique(@Nullable Iterable<RexNode> distinctKeys) {
            final @Nullable ImmutableList<RexNode> distinctKeyList =
                    distinctKeys == null ? null : ImmutableList.copyOf(distinctKeys);
            return Objects.equals(distinctKeyList, this.distinctKeys)
                    ? this
                    : new AggCallImpl(
                            aggFunction,
                            distinct,
                            approximate,
                            ignoreNulls,
                            filter,
                            alias,
                            operands,
                            distinctKeyList,
                            orderKeys);
        }

        @Override
        public AggCall approximate(boolean approximate) {
            return approximate == this.approximate
                    ? this
                    : new AggCallImpl(
                            aggFunction,
                            distinct,
                            approximate,
                            ignoreNulls,
                            filter,
                            alias,
                            operands,
                            distinctKeys,
                            orderKeys);
        }

        @Override
        public AggCall filter(@Nullable RexNode condition) {
            return Objects.equals(condition, this.filter)
                    ? this
                    : new AggCallImpl(
                            aggFunction,
                            distinct,
                            approximate,
                            ignoreNulls,
                            condition,
                            alias,
                            operands,
                            distinctKeys,
                            orderKeys);
        }

        @Override
        public AggCall as(@Nullable String alias) {
            return Objects.equals(alias, this.alias)
                    ? this
                    : new AggCallImpl(
                            aggFunction,
                            distinct,
                            approximate,
                            ignoreNulls,
                            filter,
                            alias,
                            operands,
                            distinctKeys,
                            orderKeys);
        }

        @Override
        public AggCall distinct(boolean distinct) {
            return distinct == this.distinct
                    ? this
                    : new AggCallImpl(
                            aggFunction,
                            distinct,
                            approximate,
                            ignoreNulls,
                            filter,
                            alias,
                            operands,
                            distinctKeys,
                            orderKeys);
        }

        @Override
        public AggCall ignoreNulls(boolean ignoreNulls) {
            return ignoreNulls == this.ignoreNulls
                    ? this
                    : new AggCallImpl(
                            aggFunction,
                            distinct,
                            approximate,
                            ignoreNulls,
                            filter,
                            alias,
                            operands,
                            distinctKeys,
                            orderKeys);
        }
    }

    /** Implementation of {@link AggCall} that wraps an {@link AggregateCall}. */
    private class AggCallImpl2 implements AggCallPlus {
        private final AggregateCall aggregateCall;
        private final ImmutableList<RexNode> operands;

        AggCallImpl2(AggregateCall aggregateCall, ImmutableList<RexNode> operands) {
            this.aggregateCall = requireNonNull(aggregateCall, "aggregateCall");
            this.operands = requireNonNull(operands, "operands");
        }

        @Override
        public OverCall over() {
            return new OverCallImpl(
                    aggregateCall.getAggregation(),
                    aggregateCall.isDistinct(),
                    operands,
                    aggregateCall.ignoreNulls(),
                    aggregateCall.name);
        }

        @Override
        public String toString() {
            return aggregateCall.toString();
        }

        @Override
        public SqlAggFunction op() {
            return aggregateCall.getAggregation();
        }

        @Override
        public @Nullable String alias() {
            return aggregateCall.name;
        }

        @Override
        public AggregateCall aggregateCall() {
            return aggregateCall;
        }

        @Override
        public AggregateCall aggregateCall(
                Registrar registrar, ImmutableBitSet groupSet, RelNode r) {
            return aggregateCall;
        }

        @Override
        public void register(Registrar registrar) {
            // nothing to do
        }

        @Override
        public AggCall sort(Iterable<RexNode> orderKeys) {
            throw new UnsupportedOperationException();
        }

        @Override
        public AggCall sort(RexNode... orderKeys) {
            throw new UnsupportedOperationException();
        }

        @Override
        public AggCall unique(@Nullable Iterable<RexNode> distinctKeys) {
            throw new UnsupportedOperationException();
        }

        @Override
        public AggCall approximate(boolean approximate) {
            throw new UnsupportedOperationException();
        }

        @Override
        public AggCall filter(@Nullable RexNode condition) {
            throw new UnsupportedOperationException();
        }

        @Override
        public AggCall as(@Nullable String alias) {
            throw new UnsupportedOperationException();
        }

        @Override
        public AggCall distinct(boolean distinct) {
            throw new UnsupportedOperationException();
        }

        @Override
        public AggCall ignoreNulls(boolean ignoreNulls) {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Call to a windowed aggregate function.
     *
     * <p>To create an {@code OverCall}, start with an {@link AggCall} (created by a method such as
     * {@link #aggregateCall}, {@link #sum} or {@link #count}) and call its {@link AggCall#over()}
     * method. For example,
     *
     * <pre>{@code
     * b.scan("EMP")
     *    .project(b.field("DEPTNO"),
     *       b.aggregateCall(SqlStdOperatorTable.ROW_NUMBER)
     *          .over()
     *          .partitionBy()
     *          .orderBy(b.field("EMPNO"))
     *          .rowsUnbounded()
     *          .allowPartial(true)
     *          .nullWhenCountZero(false)
     *          .as("x"))
     * }</pre>
     *
     * <p>Unlike an aggregate call, a windowed aggregate call is an expression that you can use in a
     * {@link Project} or {@link Filter}. So, to finish, call {@link OverCall#toRex()} to convert
     * the {@code OverCall} to a {@link RexNode}; the {@link OverCall#as} method (used in the above
     * example) does the same but also assigns an column alias.
     */
    public interface OverCall {
        /** Performs an action on this OverCall. */
        default <R> R let(Function<OverCall, R> consumer) {
            return consumer.apply(this);
        }

        /** Sets the PARTITION BY clause to an array of expressions. */
        OverCall partitionBy(RexNode... expressions);

        /** Sets the PARTITION BY clause to a list of expressions. */
        OverCall partitionBy(Iterable<? extends RexNode> expressions);

        /**
         * Sets the ORDER BY BY clause to an array of expressions.
         *
         * <p>Use {@link #desc(RexNode)}, {@link #nullsFirst(RexNode)}, {@link #nullsLast(RexNode)}
         * to control the sort order.
         */
        OverCall orderBy(RexNode... expressions);

        /**
         * Sets the ORDER BY BY clause to a list of expressions.
         *
         * <p>Use {@link #desc(RexNode)}, {@link #nullsFirst(RexNode)}, {@link #nullsLast(RexNode)}
         * to control the sort order.
         */
        OverCall orderBy(Iterable<? extends RexNode> expressions);

        /**
         * Sets an unbounded ROWS window, equivalent to SQL {@code ROWS BETWEEN UNBOUNDED PRECEDING
         * AND UNBOUNDED FOLLOWING}.
         */
        default OverCall rowsUnbounded() {
            return rowsBetween(
                    RexWindowBounds.UNBOUNDED_PRECEDING, RexWindowBounds.UNBOUNDED_FOLLOWING);
        }

        /**
         * Sets a ROWS window with a lower bound, equivalent to SQL {@code ROWS BETWEEN lower AND
         * CURRENT ROW}.
         */
        default OverCall rowsFrom(RexWindowBound lower) {
            return rowsBetween(lower, RexWindowBounds.UNBOUNDED_FOLLOWING);
        }

        /**
         * Sets a ROWS window with an upper bound, equivalent to SQL {@code ROWS BETWEEN CURRENT ROW
         * AND upper}.
         */
        default OverCall rowsTo(RexWindowBound upper) {
            return rowsBetween(RexWindowBounds.UNBOUNDED_PRECEDING, upper);
        }

        /**
         * Sets a RANGE window with lower and upper bounds, equivalent to SQL {@code ROWS BETWEEN
         * lower ROW AND upper}.
         */
        OverCall rowsBetween(RexWindowBound lower, RexWindowBound upper);

        /**
         * Sets an unbounded RANGE window, equivalent to SQL {@code RANGE BETWEEN UNBOUNDED
         * PRECEDING AND UNBOUNDED FOLLOWING}.
         */
        default OverCall rangeUnbounded() {
            return rangeBetween(
                    RexWindowBounds.UNBOUNDED_PRECEDING, RexWindowBounds.UNBOUNDED_FOLLOWING);
        }

        /**
         * Sets a RANGE window with a lower bound, equivalent to SQL {@code RANGE BETWEEN lower AND
         * CURRENT ROW}.
         */
        default OverCall rangeFrom(RexWindowBound lower) {
            return rangeBetween(lower, RexWindowBounds.CURRENT_ROW);
        }

        /**
         * Sets a RANGE window with an upper bound, equivalent to SQL {@code RANGE BETWEEN CURRENT
         * ROW AND upper}.
         */
        default OverCall rangeTo(RexWindowBound upper) {
            return rangeBetween(RexWindowBounds.UNBOUNDED_PRECEDING, upper);
        }

        /**
         * Sets a RANGE window with lower and upper bounds, equivalent to SQL {@code RANGE BETWEEN
         * lower ROW AND upper}.
         */
        OverCall rangeBetween(RexWindowBound lower, RexWindowBound upper);

        /** Sets whether to allow partial width windows; default true. */
        OverCall allowPartial(boolean allowPartial);

        /**
         * Sets whether the aggregate function should evaluate to null if no rows are in the window;
         * default false.
         */
        OverCall nullWhenCountZero(boolean nullWhenCountZero);

        /**
         * Sets the alias of this expression, and converts it to a {@link RexNode}; default is the
         * alias that was set via {@link AggCall#as(String)}.
         */
        RexNode as(String alias);

        /** Converts this expression to a {@link RexNode}. */
        RexNode toRex();
    }

    /** Implementation of {@link OverCall}. */
    private class OverCallImpl implements OverCall {
        private final ImmutableList<RexNode> operands;
        private final boolean ignoreNulls;
        private final @Nullable String alias;
        private final boolean nullWhenCountZero;
        private final boolean allowPartial;
        private final boolean rows;
        private final RexWindowBound lowerBound;
        private final RexWindowBound upperBound;
        private final ImmutableList<RexNode> partitionKeys;
        private final ImmutableList<RexFieldCollation> sortKeys;
        private final SqlAggFunction op;
        private final boolean distinct;

        private OverCallImpl(
                SqlAggFunction op,
                boolean distinct,
                ImmutableList<RexNode> operands,
                boolean ignoreNulls,
                @Nullable String alias,
                ImmutableList<RexNode> partitionKeys,
                ImmutableList<RexFieldCollation> sortKeys,
                boolean rows,
                RexWindowBound lowerBound,
                RexWindowBound upperBound,
                boolean nullWhenCountZero,
                boolean allowPartial) {
            this.op = op;
            this.distinct = distinct;
            this.operands = operands;
            this.ignoreNulls = ignoreNulls;
            this.alias = alias;
            this.partitionKeys = partitionKeys;
            this.sortKeys = sortKeys;
            this.nullWhenCountZero = nullWhenCountZero;
            this.allowPartial = allowPartial;
            this.rows = rows;
            this.lowerBound = lowerBound;
            this.upperBound = upperBound;
        }

        /** Creates an OverCallImpl with default settings. */
        OverCallImpl(
                SqlAggFunction op,
                boolean distinct,
                ImmutableList<RexNode> operands,
                boolean ignoreNulls,
                @Nullable String alias) {
            this(
                    op,
                    distinct,
                    operands,
                    ignoreNulls,
                    alias,
                    ImmutableList.of(),
                    ImmutableList.of(),
                    true,
                    RexWindowBounds.UNBOUNDED_PRECEDING,
                    RexWindowBounds.UNBOUNDED_FOLLOWING,
                    false,
                    true);
        }

        @Override
        public OverCall partitionBy(Iterable<? extends RexNode> expressions) {
            return partitionBy_(ImmutableList.copyOf(expressions));
        }

        @Override
        public OverCall partitionBy(RexNode... expressions) {
            return partitionBy_(ImmutableList.copyOf(expressions));
        }

        private OverCall partitionBy_(ImmutableList<RexNode> partitionKeys) {
            return new OverCallImpl(
                    op,
                    distinct,
                    operands,
                    ignoreNulls,
                    alias,
                    partitionKeys,
                    sortKeys,
                    rows,
                    lowerBound,
                    upperBound,
                    nullWhenCountZero,
                    allowPartial);
        }

        private OverCall orderBy_(ImmutableList<RexFieldCollation> sortKeys) {
            return new OverCallImpl(
                    op,
                    distinct,
                    operands,
                    ignoreNulls,
                    alias,
                    partitionKeys,
                    sortKeys,
                    rows,
                    lowerBound,
                    upperBound,
                    nullWhenCountZero,
                    allowPartial);
        }

        @Override
        public OverCall orderBy(Iterable<? extends RexNode> sortKeys) {
            ImmutableList.Builder<RexFieldCollation> fieldCollations = ImmutableList.builder();
            sortKeys.forEach(
                    sortKey ->
                            fieldCollations.add(
                                    rexCollation(
                                            sortKey,
                                            RelFieldCollation.Direction.ASCENDING,
                                            RelFieldCollation.NullDirection.UNSPECIFIED)));
            return orderBy_(fieldCollations.build());
        }

        @Override
        public OverCall orderBy(RexNode... sortKeys) {
            return orderBy(Arrays.asList(sortKeys));
        }

        @Override
        public OverCall rowsBetween(RexWindowBound lowerBound, RexWindowBound upperBound) {
            return new OverCallImpl(
                    op,
                    distinct,
                    operands,
                    ignoreNulls,
                    alias,
                    partitionKeys,
                    sortKeys,
                    true,
                    lowerBound,
                    upperBound,
                    nullWhenCountZero,
                    allowPartial);
        }

        @Override
        public OverCall rangeBetween(RexWindowBound lowerBound, RexWindowBound upperBound) {
            return new OverCallImpl(
                    op,
                    distinct,
                    operands,
                    ignoreNulls,
                    alias,
                    partitionKeys,
                    sortKeys,
                    false,
                    lowerBound,
                    upperBound,
                    nullWhenCountZero,
                    allowPartial);
        }

        @Override
        public OverCall allowPartial(boolean allowPartial) {
            return new OverCallImpl(
                    op,
                    distinct,
                    operands,
                    ignoreNulls,
                    alias,
                    partitionKeys,
                    sortKeys,
                    rows,
                    lowerBound,
                    upperBound,
                    nullWhenCountZero,
                    allowPartial);
        }

        @Override
        public OverCall nullWhenCountZero(boolean nullWhenCountZero) {
            return new OverCallImpl(
                    op,
                    distinct,
                    operands,
                    ignoreNulls,
                    alias,
                    partitionKeys,
                    sortKeys,
                    rows,
                    lowerBound,
                    upperBound,
                    nullWhenCountZero,
                    allowPartial);
        }

        @Override
        public RexNode as(String alias) {
            return new OverCallImpl(
                            op,
                            distinct,
                            operands,
                            ignoreNulls,
                            alias,
                            partitionKeys,
                            sortKeys,
                            rows,
                            lowerBound,
                            upperBound,
                            nullWhenCountZero,
                            allowPartial)
                    .toRex();
        }

        @Override
        public RexNode toRex() {
            final RexCallBinding bind =
                    new RexCallBinding(getTypeFactory(), op, operands, ImmutableList.of()) {
                        @Override
                        public int getGroupCount() {
                            return SqlWindow.isAlwaysNonEmpty(lowerBound, upperBound) ? 1 : 0;
                        }
                    };
            final RelDataType type = op.inferReturnType(bind);
            final RexNode over =
                    getRexBuilder()
                            .makeOver(
                                    type,
                                    op,
                                    operands,
                                    partitionKeys,
                                    sortKeys,
                                    lowerBound,
                                    upperBound,
                                    rows,
                                    allowPartial,
                                    nullWhenCountZero,
                                    distinct,
                                    ignoreNulls);
            return alias == null ? over : alias(over, alias);
        }
    }

    /**
     * Collects the extra expressions needed for {@link #aggregate}.
     *
     * <p>The extra expressions come from the group key and as arguments to aggregate calls, and
     * later there will be a {@link #project} or a {@link #rename(List)} if necessary.
     */
    private static class Registrar {
        final List<RexNode> originalExtraNodes;
        final List<RexNode> extraNodes;
        final List<@Nullable String> names;

        Registrar(Iterable<RexNode> fields, List<String> fieldNames) {
            originalExtraNodes = ImmutableList.copyOf(fields);
            extraNodes = new ArrayList<>(originalExtraNodes);
            names = new ArrayList<>(fieldNames);
        }

        int registerExpression(RexNode node) {
            switch (node.getKind()) {
                case AS:
                    final List<RexNode> operands = ((RexCall) node).operands;
                    final int i = registerExpression(operands.get(0));
                    names.set(i, RexLiteral.stringValue(operands.get(1)));
                    return i;
                case DESCENDING:
                case NULLS_FIRST:
                case NULLS_LAST:
                    return registerExpression(((RexCall) node).operands.get(0));
                default:
                    final int i2 = extraNodes.indexOf(node);
                    if (i2 >= 0) {
                        return i2;
                    }
                    extraNodes.add(node);
                    names.add(null);
                    return extraNodes.size() - 1;
            }
        }

        List<Integer> registerExpressions(Iterable<? extends RexNode> nodes) {
            final List<Integer> builder = new ArrayList<>();
            for (RexNode node : nodes) {
                builder.add(registerExpression(node));
            }
            return builder;
        }

        List<RelFieldCollation> registerFieldCollations(Iterable<? extends RexNode> orderKeys) {
            final List<RelFieldCollation> fieldCollations = new ArrayList<>();
            for (RexNode orderKey : orderKeys) {
                final RelFieldCollation collation =
                        collation(
                                orderKey, RelFieldCollation.Direction.ASCENDING, null, extraNodes);
                if (!RelCollations.ordinals(fieldCollations).contains(collation.getFieldIndex())) {
                    fieldCollations.add(collation);
                }
            }
            return ImmutableList.copyOf(fieldCollations);
        }

        /** Returns the number of fields added. */
        int addedFieldCount() {
            return extraNodes.size() - originalExtraNodes.size();
        }
    }

    /**
     * Builder stack frame.
     *
     * <p>Describes a previously created relational expression and information about how table
     * aliases map into its row type.
     */
    private static class Frame {
        final RelNode rel;
        final ImmutableList<Field> fields;

        private Frame(RelNode rel, ImmutableList<Field> fields) {
            this.rel = rel;
            this.fields = fields;
        }

        private Frame(RelNode rel) {
            String tableAlias = deriveAlias(rel);
            ImmutableList.Builder<Field> builder = ImmutableList.builder();
            ImmutableSet<String> aliases =
                    tableAlias == null ? ImmutableSet.of() : ImmutableSet.of(tableAlias);
            for (RelDataTypeField field : rel.getRowType().getFieldList()) {
                builder.add(new Field(aliases, field));
            }
            this.rel = rel;
            this.fields = builder.build();
        }

        @Override
        public String toString() {
            return rel + ": " + fields;
        }

        private static @Nullable String deriveAlias(RelNode rel) {
            if (rel instanceof TableScan) {
                TableScan scan = (TableScan) rel;
                final List<String> names = scan.getTable().getQualifiedName();
                if (!names.isEmpty()) {
                    return Util.last(names);
                }
            }
            return null;
        }

        List<RelDataTypeField> fields() {
            return Pair.right(fields);
        }
    }

    /** A field that belongs to a stack {@link Frame}. */
    private static class Field extends Pair<ImmutableSet<String>, RelDataTypeField> {
        Field(ImmutableSet<String> left, RelDataTypeField right) {
            super(left, right);
        }

        Field addAlias(String alias) {
            if (left.contains(alias)) {
                return this;
            }
            final ImmutableSet<String> aliasList =
                    ImmutableSet.<String>builder().addAll(left).add(alias).build();
            return new Field(aliasList, right);
        }
    }

    /**
     * Shuttle that shifts a predicate's inputs to the left, replacing early ones with references to
     * a {@link RexCorrelVariable}.
     */
    private class Shifter extends RexShuttle {
        private final RelNode left;
        private final CorrelationId id;
        private final RelNode right;

        Shifter(RelNode left, CorrelationId id, RelNode right) {
            this.left = left;
            this.id = id;
            this.right = right;
        }

        @Override
        public RexNode visitInputRef(RexInputRef inputRef) {
            final RelDataType leftRowType = left.getRowType();
            final RexBuilder rexBuilder = getRexBuilder();
            final int leftCount = leftRowType.getFieldCount();
            if (inputRef.getIndex() < leftCount) {
                final RexNode v = rexBuilder.makeCorrel(leftRowType, id);
                return rexBuilder.makeFieldAccess(v, inputRef.getIndex());
            } else {
                return rexBuilder.makeInputRef(right, inputRef.getIndex() - leftCount);
            }
        }
    }

    /**
     * Configuration of RelBuilder.
     *
     * <p>It is immutable, and all fields are public.
     *
     * <p>Start with the {@link #DEFAULT} instance, and call {@code withXxx} methods to set its
     * properties.
     */
    @Value.Immutable
    public interface Config {
        /** Default configuration. */
        Config DEFAULT = ImmutableRelBuilder.Config.of();

        /**
         * Controls whether to merge two {@link Project} operators when inlining expressions causes
         * complexity to increase.
         *
         * <p>Usually merging projects is beneficial, but occasionally the result is more complex
         * than the original projects. Consider:
         *
         * <pre>
         * P: Project(a+b+c AS x, d+e+f AS y, g+h+i AS z)  # complexity 15
         * Q: Project(x*y*z AS p, x-y-z AS q)              # complexity 10
         * R: Project((a+b+c)*(d+e+f)*(g+h+i) AS s,
         *            (a+b+c)-(d+e+f)-(g+h+i) AS t)        # complexity 34
         * </pre>
         *
         * The complexity of an expression is the number of nodes (leaves and operators). For
         * example, {@code a+b+c} has complexity 5 (3 field references and 2 calls):
         *
         * <pre>
         *       +
         *      /  \
         *     +    c
         *    / \
         *   a   b
         * </pre>
         *
         * <p>A negative value never allows merges.
         *
         * <p>A zero or positive value, {@code bloat}, allows a merge if complexity of the result is
         * less than or equal to the sum of the complexity of the originals plus {@code bloat}.
         *
         * <p>The default value, 100, allows a moderate increase in complexity but prevents cases
         * where complexity would run away into the millions and run out of memory. Moderate
         * complexity is OK; the implementation, say via {@link
         * org.apache.calcite.adapter.enumerable.EnumerableCalc}, will often gather common
         * sub-expressions and compute them only once.
         */
        @Value.Default
        default int bloat() {
            return 100;
        }

        /** Sets {@link #bloat}. */
        Config withBloat(int bloat);

        /**
         * Whether {@link RelBuilder#aggregate} should eliminate duplicate aggregate calls; default
         * true.
         */
        @Value.Default
        default boolean dedupAggregateCalls() {
            return true;
        }

        /** Sets {@link #dedupAggregateCalls}. */
        Config withDedupAggregateCalls(boolean dedupAggregateCalls);

        /** Whether {@link RelBuilder#aggregate} should prune unused input columns; default true. */
        @Value.Default
        default boolean pruneInputOfAggregate() {
            return true;
        }

        /** Sets {@link #pruneInputOfAggregate}. */
        Config withPruneInputOfAggregate(boolean pruneInputOfAggregate);

        /**
         * Whether to push down join conditions; default false (but {@link
         * SqlToRelConverter#config()} by default sets this to true).
         */
        @Value.Default
        default boolean pushJoinCondition() {
            return false;
        }

        /** Sets {@link #pushJoinCondition()}. */
        Config withPushJoinCondition(boolean pushJoinCondition);

        /** Whether to simplify expressions; default true. */
        @Value.Default
        default boolean simplify() {
            return true;
        }

        /** Sets {@link #simplify}. */
        Config withSimplify(boolean simplify);

        /** Whether to simplify LIMIT 0 to an empty relation; default true. */
        @Value.Default
        default boolean simplifyLimit() {
            return true;
        }

        /** Sets {@link #simplifyLimit()}. */
        Config withSimplifyLimit(boolean simplifyLimit);

        /**
         * Whether to simplify {@code Union(Values, Values)} or {@code Union(Project(Values))} to
         * {@code Values}; default true.
         */
        @Value.Default
        default boolean simplifyValues() {
            return true;
        }

        /** Sets {@link #simplifyValues()}. */
        Config withSimplifyValues(boolean simplifyValues);

        /**
         * Whether to create an Aggregate even if we know that the input is already unique; default
         * false.
         */
        @Value.Default
        default boolean aggregateUnique() {
            return false;
        }

        /** Sets {@link #aggregateUnique()}. */
        Config withAggregateUnique(boolean aggregateUnique);
    }
}
