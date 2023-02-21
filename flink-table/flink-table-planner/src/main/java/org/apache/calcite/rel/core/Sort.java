/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.calcite.rel.core;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.hint.Hintable;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.util.Util;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Relational expression that imposes a particular sort order on its input without otherwise
 * changing its content.
 *
 * <p>Temporarily copy from calcite to cherry-pick [CALCITE-5107] and will be removed when upgrade
 * the latest calcite.
 */
public abstract class Sort extends SingleRel implements Hintable {
    // ~ Instance fields --------------------------------------------------------

    public final RelCollation collation;
    public final @Nullable RexNode offset;
    public final @Nullable RexNode fetch;
    protected final ImmutableList<RelHint> hints;

    // ~ Constructors -----------------------------------------------------------

    /**
     * Creates a Sort.
     *
     * @param cluster Cluster this relational expression belongs to
     * @param traits Traits
     * @param child input relational expression
     * @param collation array of sort specifications
     */
    protected Sort(
            RelOptCluster cluster, RelTraitSet traits, RelNode child, RelCollation collation) {
        this(cluster, traits, Collections.emptyList(), child, collation, null, null);
    }

    /**
     * Creates a Sort.
     *
     * @param cluster Cluster this relational expression belongs to
     * @param traits Traits
     * @param child input relational expression
     * @param collation array of sort specifications
     * @param offset Expression for number of rows to discard before returning first row
     * @param fetch Expression for number of rows to fetch
     */
    public Sort(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelNode child,
            RelCollation collation,
            RexNode offset,
            RexNode fetch) {
        this(cluster, traits, Collections.emptyList(), child, collation, offset, fetch);
    }

    /**
     * Creates a Sort.
     *
     * @param cluster Cluster this relational expression belongs to
     * @param traits Traits
     * @param hints Hints for this node
     * @param child input relational expression
     * @param collation array of sort specifications
     * @param offset Expression for number of rows to discard before returning first row
     * @param fetch Expression for number of rows to fetch
     */
    protected Sort(
            RelOptCluster cluster,
            RelTraitSet traits,
            List<RelHint> hints,
            RelNode child,
            RelCollation collation,
            @Nullable RexNode offset,
            @Nullable RexNode fetch) {
        super(cluster, traits, child);
        this.collation = collation;
        this.offset = offset;
        this.fetch = fetch;
        this.hints = ImmutableList.copyOf(hints);

        assert traits.containsIfApplicable(collation)
                : "traits=" + traits + ", collation=" + collation;
        assert !(fetch == null && offset == null && collation.getFieldCollations().isEmpty())
                : "trivial sort";
    }

    /** Creates a Sort by parsing serialized output. */
    protected Sort(RelInput input) {
        this(
                input.getCluster(),
                input.getTraitSet().plus(input.getCollation()),
                input.getInput(),
                RelCollationTraitDef.INSTANCE.canonize(input.getCollation()),
                input.getExpression("offset"),
                input.getExpression("fetch"));
    }

    // ~ Methods ----------------------------------------------------------------

    @Override
    public final Sort copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return copy(traitSet, sole(inputs), collation, offset, fetch);
    }

    public final Sort copy(RelTraitSet traitSet, RelNode newInput, RelCollation newCollation) {
        return copy(traitSet, newInput, newCollation, offset, fetch);
    }

    public abstract Sort copy(
            RelTraitSet traitSet,
            RelNode newInput,
            RelCollation newCollation,
            @Nullable RexNode offset,
            @Nullable RexNode fetch);

    /**
     * {@inheritDoc}
     *
     * <p>The CPU cost of a Sort has three main cases:
     *
     * <ul>
     *   <li>If {@code fetch} is zero, CPU cost is zero; otherwise,
     *   <li>if the sort keys are empty, we don't need to sort, only step over the rows, and
     *       therefore the CPU cost is {@code min(fetch + offset, inputRowCount) * bytesPerRow};
     *       otherwise
     *   <li>we need to read and sort {@code inputRowCount} rows, with at most {@code min(fetch +
     *       offset, inputRowCount)} of them in the sort data structure at a time, giving a CPU cost
     *       of {@code inputRowCount * log(min(fetch + offset, inputRowCount)) * bytesPerRow}.
     * </ul>
     *
     * <p>The cost model factors in row width via {@code bytesPerRow}, because sorts need to move
     * rows around, not just compare them; by making the cost higher if rows are wider, we
     * discourage pushing a Project through a Sort. We assume that each field is 4 bytes, and we add
     * 3 'virtual fields' to represent the per-row overhead. Thus a 1-field row is (3 + 1) * 4 = 16
     * bytes; a 5-field row is (3 + 5) * 4 = 32 bytes.
     *
     * <p>The cost model does not consider a 5-field sort to be more expensive than, say, a 2-field
     * sort, because both sorts will compare just one field most of the time.
     */
    @Override
    public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        final double offsetValue = Util.first(doubleValue(offset), 0d);
        assert offsetValue >= 0 : "offset should not be negative:" + offsetValue;

        final double inCount = mq.getRowCount(input);
        @Nullable Double fetchValue = doubleValue(fetch);
        final double readCount;
        if (fetchValue == null) {
            readCount = inCount;
        } else if (fetchValue <= 0) {
            // Case 1. Read zero rows from input, therefore CPU cost is zero.
            return planner.getCostFactory().makeCost(inCount, 0, 0);
        } else {
            readCount = Math.min(inCount, offsetValue + fetchValue);
        }

        final double bytesPerRow = (3 + getRowType().getFieldCount()) * 4;

        final double cpu;
        if (collation.getFieldCollations().isEmpty()) {
            // Case 2. If sort keys are empty, CPU cost is cheaper because we are just
            // stepping over the first "readCount" rows, rather than sorting all
            // "inCount" them. (Presumably we are applying FETCH and/or OFFSET,
            // otherwise this Sort is a no-op.)
            cpu = readCount * bytesPerRow;
        } else {
            // Case 3. Read and sort all "inCount" rows, keeping "readCount" in the
            // sort data structure at a time.
            cpu = Util.nLogM(inCount, readCount) * bytesPerRow;
        }
        return planner.getCostFactory().makeCost(readCount, cpu, 0);
    }

    @Override
    public RelNode accept(RexShuttle shuttle) {
        RexNode offset = shuttle.apply(this.offset);
        RexNode fetch = shuttle.apply(this.fetch);
        List<RexNode> originalSortExps = getSortExps();
        List<RexNode> sortExps = shuttle.apply(originalSortExps);
        assert sortExps == originalSortExps
                : "Sort node does not support modification of input field expressions."
                        + " Old expressions: "
                        + originalSortExps
                        + ", new ones: "
                        + sortExps;
        if (offset == this.offset && fetch == this.fetch) {
            return this;
        }
        return copy(traitSet, getInput(), collation, offset, fetch);
    }

    @Override
    public boolean isEnforcer() {
        return offset == null && fetch == null && collation.getFieldCollations().size() > 0;
    }

    /**
     * Returns the array of {@link RelFieldCollation}s asked for by the sort specification, from
     * most significant to least significant.
     *
     * <p>See also {@link RelMetadataQuery#collations(RelNode)}, which lists all known collations.
     * For example, <code>ORDER BY time_id</code> might also be sorted by <code>the_year, the_month
     * </code> because of a known monotonicity constraint among the columns. {@code getCollation}
     * would return <code>[time_id]</code> and {@code collations} would return <code>
     * [ [time_id], [the_year, the_month] ]</code>.
     */
    public RelCollation getCollation() {
        return collation;
    }

    /** Returns the sort expressions. */
    public List<RexNode> getSortExps() {
        //noinspection StaticPseudoFunctionalStyleMethod
        return Util.transform(
                collation.getFieldCollations(),
                field ->
                        getCluster()
                                .getRexBuilder()
                                .makeInputRef(
                                        input, Objects.requireNonNull(field).getFieldIndex()));
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        super.explainTerms(pw);
        if (pw.nest()) {
            pw.item("collation", collation);
        } else {
            for (Ord<RexNode> ord : Ord.zip(getSortExps())) {
                pw.item("sort" + ord.i, ord.e);
            }
            for (Ord<RelFieldCollation> ord : Ord.zip(collation.getFieldCollations())) {
                pw.item("dir" + ord.i, ord.e.shortString());
            }
        }
        pw.itemIf("offset", offset, offset != null);
        pw.itemIf("fetch", fetch, fetch != null);
        return pw;
    }

    @Override
    public ImmutableList<RelHint> getHints() {
        return hints;
    }

    /** Returns the double value of a node if it is a literal, otherwise null. */
    private static @Nullable Double doubleValue(@Nullable RexNode r) {
        return r instanceof RexLiteral ? ((RexLiteral) r).getValueAs(Double.class) : null;
    }
}
