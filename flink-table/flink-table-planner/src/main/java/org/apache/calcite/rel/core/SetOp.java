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

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.hint.Hintable;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.Util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * <code>SetOp</code> is an abstract base for relational set operators such as UNION, MINUS (aka
 * EXCEPT), and INTERSECT.
 *
 * <p>Temporarily copy from calcite to cherry-pick [CALCITE-5107] and will be removed when upgrade
 * the latest calcite.
 */
public abstract class SetOp extends AbstractRelNode implements Hintable {
    // ~ Instance fields --------------------------------------------------------

    protected com.google.common.collect.ImmutableList<RelNode> inputs;
    public final SqlKind kind;
    public final boolean all;
    protected final com.google.common.collect.ImmutableList<RelHint> hints;

    // ~ Constructors -----------------------------------------------------------

    /** Creates a SetOp. */
    protected SetOp(
            RelOptCluster cluster,
            RelTraitSet traits,
            List<RelHint> hints,
            List<RelNode> inputs,
            SqlKind kind,
            boolean all) {
        super(cluster, traits);
        com.google.common.base.Preconditions.checkArgument(
                kind == SqlKind.UNION || kind == SqlKind.INTERSECT || kind == SqlKind.EXCEPT);
        this.kind = kind;
        this.inputs = com.google.common.collect.ImmutableList.copyOf(inputs);
        this.all = all;
        this.hints = com.google.common.collect.ImmutableList.copyOf(hints);
    }

    /** Creates a SetOp. */
    protected SetOp(
            RelOptCluster cluster,
            RelTraitSet traits,
            List<RelNode> inputs,
            SqlKind kind,
            boolean all) {
        this(cluster, traits, Collections.emptyList(), inputs, kind, all);
    }

    /** Creates a SetOp by parsing serialized output. */
    protected SetOp(RelInput input) {
        this(
                input.getCluster(),
                input.getTraitSet(),
                Collections.emptyList(),
                input.getInputs(),
                SqlKind.UNION,
                input.getBoolean("all", false));
    }

    // ~ Methods ----------------------------------------------------------------

    public abstract SetOp copy(RelTraitSet traitSet, List<RelNode> inputs, boolean all);

    @Override
    public SetOp copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return copy(traitSet, inputs, all);
    }

    @Override
    public void replaceInput(int ordinalInParent, RelNode p) {
        final List<RelNode> newInputs = new ArrayList<>(inputs);
        newInputs.set(ordinalInParent, p);
        inputs = com.google.common.collect.ImmutableList.copyOf(newInputs);
        recomputeDigest();
    }

    @Override
    public List<RelNode> getInputs() {
        return inputs;
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        super.explainTerms(pw);
        for (Ord<RelNode> ord : Ord.zip(inputs)) {
            pw.input("input#" + ord.i, ord.e);
        }
        return pw.item("all", all);
    }

    @Override
    protected RelDataType deriveRowType() {
        final List<RelDataType> inputRowTypes = Util.transform(inputs, RelNode::getRowType);
        final RelDataType rowType = getCluster().getTypeFactory().leastRestrictive(inputRowTypes);
        if (rowType == null) {
            throw new IllegalArgumentException(
                    "Cannot compute compatible row type "
                            + "for arguments to set op: "
                            + Util.sepList(inputRowTypes, ", "));
        }
        return rowType;
    }

    /**
     * Returns whether all the inputs of this set operator have the same row type as its output row.
     *
     * @param compareNames Whether column names are important in the homogeneity comparison
     * @return Whether all the inputs of this set operator have the same row type as its output row
     */
    public boolean isHomogeneous(boolean compareNames) {
        RelDataType unionType = getRowType();
        for (RelNode input : getInputs()) {
            if (!RelOptUtil.areRowTypesEqual(input.getRowType(), unionType, compareNames)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public com.google.common.collect.ImmutableList<RelHint> getHints() {
        return hints;
    }
}
