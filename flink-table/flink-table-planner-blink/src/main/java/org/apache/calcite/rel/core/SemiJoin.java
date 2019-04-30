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

package org.apache.calcite.rel.core;

import org.apache.flink.table.plan.util.FlinkRelMdUtil;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

/*
 * THIS FILE HAS BEEN COPIED FROM THE APACHE CALCITE PROJECT
 * TO SUPPORT ANTI-JOIN AND NON-EQUI JOIN CONDITION.
 */

/**
 * Relational expression that joins two relational expressions according to some
 * condition, but outputs only columns from the left input, and eliminates
 * duplicates.
 *
 * <p>The effect is something like the SQL {@code IN} operator.
 */
public class SemiJoin extends Join {
	//~ Instance fields --------------------------------------------------------

	// `true` is anti-join, `false` is semi-join
	public final boolean isAnti;

	//~ Constructors -----------------------------------------------------------

	/**
	 * Creates a SemiJoin.
	 *
	 * <p>Use {@link #create} unless you know what you're doing.
	 *
	 * @param cluster cluster that join belongs to
	 * @param traitSet Trait set
	 * @param left left join input
	 * @param right right join input
	 * @param condition join condition
	 * @param isAnti `true` is anti-join, `false` is semi-join
	 */
	public SemiJoin(
			RelOptCluster cluster,
			RelTraitSet traitSet,
			RelNode left,
			RelNode right,
			RexNode condition,
			boolean isAnti) {
		super(
				cluster,
				traitSet,
				left,
				right,
				condition,
				ImmutableSet.of(),
				JoinRelType.INNER);
		this.isAnti = isAnti;
	}

	/** Creates a SemiJoin. */
	public static SemiJoin create(RelNode left, RelNode right, RexNode condition, boolean isAnti) {
		final RelOptCluster cluster = left.getCluster();
		return new SemiJoin(cluster, cluster.traitSetOf(Convention.NONE), left, right, condition, isAnti);
	}

	//~ Methods ----------------------------------------------------------------

	@Override
	public SemiJoin copy(RelTraitSet traitSet, RexNode condition,
			RelNode left, RelNode right, JoinRelType joinType, boolean semiJoinDone) {
		assert joinType == JoinRelType.INNER;
		return new SemiJoin(getCluster(), traitSet, left, right, condition, isAnti);
	}

	@Override
	public RelOptCost computeSelfCost(RelOptPlanner planner,
			RelMetadataQuery mq) {
		// REVIEW jvs 9-Apr-2006:  Just for now...
		return planner.getCostFactory().makeTinyCost();
	}

	@Override
	public double estimateRowCount(RelMetadataQuery mq) {
		return Util.first(FlinkRelMdUtil.getSemiJoinRowCount(mq, left, right, joinType, condition, isAnti), 1D);
	}

	/**
	 * {@inheritDoc}
	 *
	 * <p>In the case of semi-join, the row type consists of columns from left
	 * input only.
	 */
	@Override
	public RelDataType deriveRowType() {
		return SqlValidatorUtil.deriveJoinRowType(
				left.getRowType(),
				null,
				JoinRelType.INNER,
				getCluster().getTypeFactory(),
				null,
				ImmutableList.of());
	}

	@Override
	public RelWriter explainTerms(RelWriter pw) {
		return super.explainTerms(pw).item("isAnti", isAnti);
	}

}

// End SemiJoin.java
