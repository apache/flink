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

package org.apache.flink.table.planner.plan.rules.logical;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.rules.MultiJoin;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This rule is copied from Calcite's {@link org.apache.calcite.rel.rules.JoinToMultiJoinRule}.
 * This file should be removed while upgrading Calcite version to 1.21. [CALCITE-3225]
 * Modification:
 * - Does not match SEMI/ANTI join. lines changed (142-145)
 * - lines changed (440-451)
 */

/**
 * Planner rule to flatten a tree of
 * {@link org.apache.calcite.rel.logical.LogicalJoin}s
 * into a single {@link MultiJoin} with N inputs.
 *
 * <p>An input is not flattened if
 * the input is a null generating input in an outer join, i.e., either input in
 * a full outer join, the right hand side of a left outer join, or the left hand
 * side of a right outer join.
 *
 * <p>Join conditions are also pulled up from the inputs into the topmost
 * {@link MultiJoin},
 * unless the input corresponds to a null generating input in an outer join,
 *
 * <p>Outer join information is also stored in the {@link MultiJoin}. A
 * boolean flag indicates if the join is a full outer join, and in the case of
 * left and right outer joins, the join type and outer join conditions are
 * stored in arrays in the {@link MultiJoin}. This outer join information is
 * associated with the null generating input in the outer join. So, in the case
 * of a a left outer join between A and B, the information is associated with B,
 * not A.
 *
 * <p>Here are examples of the {@link MultiJoin}s constructed after this rule
 * has been applied on following join trees.
 *
 * <ul>
 * <li>A JOIN B &rarr; MJ(A, B)
 *
 * <li>A JOIN B JOIN C &rarr; MJ(A, B, C)
 *
 * <li>A LEFT JOIN B &rarr; MJ(A, B), left outer join on input#1
 *
 * <li>A RIGHT JOIN B &rarr; MJ(A, B), right outer join on input#0
 *
 * <li>A FULL JOIN B &rarr; MJ[full](A, B)
 *
 * <li>A LEFT JOIN (B JOIN C) &rarr; MJ(A, MJ(B, C))), left outer join on
 * input#1 in the outermost MultiJoin
 *
 * <li>(A JOIN B) LEFT JOIN C &rarr; MJ(A, B, C), left outer join on input#2
 *
 * <li>(A LEFT JOIN B) JOIN C &rarr; MJ(MJ(A, B), C), left outer join on input#1
 * of the inner MultiJoin        TODO
 *
 * <li>A LEFT JOIN (B FULL JOIN C) &rarr; MJ(A, MJ[full](B, C)), left outer join
 * on input#1 in the outermost MultiJoin
 *
 * <li>(A LEFT JOIN B) FULL JOIN (C RIGHT JOIN D) &rarr;
 *      MJ[full](MJ(A, B), MJ(C, D)), left outer join on input #1 in the first
 *      inner MultiJoin and right outer join on input#0 in the second inner
 *      MultiJoin
 * </ul>
 *
 * <p>The constructor is parameterized to allow any sub-class of
 * {@link org.apache.calcite.rel.core.Join}, not just
 * {@link org.apache.calcite.rel.logical.LogicalJoin}.</p>
 *
 * @see org.apache.calcite.rel.rules.FilterMultiJoinMergeRule
 * @see org.apache.calcite.rel.rules.ProjectMultiJoinMergeRule
 */
public class FlinkJoinToMultiJoinRule extends RelOptRule {
	public static final FlinkJoinToMultiJoinRule INSTANCE =
			new FlinkJoinToMultiJoinRule(LogicalJoin.class, RelFactories.LOGICAL_BUILDER);

	//~ Constructors -----------------------------------------------------------

	@Deprecated // to be removed before 2.0
	public FlinkJoinToMultiJoinRule(Class<? extends Join> clazz) {
		this(clazz, RelFactories.LOGICAL_BUILDER);
	}

	/**
	 * Creates a FlinkJoinToMultiJoinRule.
	 */
	public FlinkJoinToMultiJoinRule(Class<? extends Join> clazz,
			RelBuilderFactory relBuilderFactory) {
		super(
				operand(clazz,
						operand(RelNode.class, any()),
						operand(RelNode.class, any())),
				relBuilderFactory, null);
	}

	//~ Methods ----------------------------------------------------------------

	@Override
	public boolean matches(RelOptRuleCall call) {
		final Join origJoin = call.rel(0);
		return origJoin.getJoinType() != JoinRelType.SEMI && origJoin.getJoinType() != JoinRelType.ANTI;
	}

	public void onMatch(RelOptRuleCall call) {
		final Join origJoin = call.rel(0);
		final RelNode left = call.rel(1);
		final RelNode right = call.rel(2);

		// combine the children MultiJoin inputs into an array of inputs
		// for the new MultiJoin
		final List<ImmutableBitSet> projFieldsList = new ArrayList<>();
		final List<int[]> joinFieldRefCountsList = new ArrayList<>();
		final List<RelNode> newInputs =
				combineInputs(
						origJoin,
						left,
						right,
						projFieldsList,
						joinFieldRefCountsList);

		// combine the outer join information from the left and right
		// inputs, and include the outer join information from the current
		// join, if it's a left/right outer join
		final List<Pair<JoinRelType, RexNode>> joinSpecs = new ArrayList<>();
		combineOuterJoins(
				origJoin,
				newInputs,
				left,
				right,
				joinSpecs);

		// pull up the join filters from the children MultiJoinRels and
		// combine them with the join filter associated with this LogicalJoin to
		// form the join filter for the new MultiJoin
		List<RexNode> newJoinFilters = combineJoinFilters(origJoin, left, right);

		// add on the join field reference counts for the join condition
		// associated with this LogicalJoin
		final com.google.common.collect.ImmutableMap<Integer, ImmutableIntList> newJoinFieldRefCountsMap =
				addOnJoinFieldRefCounts(newInputs,
						origJoin.getRowType().getFieldCount(),
						origJoin.getCondition(),
						joinFieldRefCountsList);

		List<RexNode> newPostJoinFilters =
				combinePostJoinFilters(origJoin, left, right);

		final RexBuilder rexBuilder = origJoin.getCluster().getRexBuilder();
		RelNode multiJoin =
				new MultiJoin(
						origJoin.getCluster(),
						newInputs,
						RexUtil.composeConjunction(rexBuilder, newJoinFilters),
						origJoin.getRowType(),
						origJoin.getJoinType() == JoinRelType.FULL,
						Pair.right(joinSpecs),
						Pair.left(joinSpecs),
						projFieldsList,
						newJoinFieldRefCountsMap,
						RexUtil.composeConjunction(rexBuilder, newPostJoinFilters, true));

		call.transformTo(multiJoin);
	}

	/**
	 * Combines the inputs into a LogicalJoin into an array of inputs.
	 *
	 * @param join                   original join
	 * @param left                   left input into join
	 * @param right                  right input into join
	 * @param projFieldsList         returns a list of the new combined projection
	 *                               fields
	 * @param joinFieldRefCountsList returns a list of the new combined join
	 *                               field reference counts
	 * @return combined left and right inputs in an array
	 */
	private List<RelNode> combineInputs(
			Join join,
			RelNode left,
			RelNode right,
			List<ImmutableBitSet> projFieldsList,
			List<int[]> joinFieldRefCountsList) {
		final List<RelNode> newInputs = new ArrayList<>();

		// leave the null generating sides of an outer join intact; don't
		// pull up those children inputs into the array we're constructing
		if (canCombine(left, join.getJoinType().generatesNullsOnLeft())) {
			final MultiJoin leftMultiJoin = (MultiJoin) left;
			for (int i = 0; i < left.getInputs().size(); i++) {
				newInputs.add(leftMultiJoin.getInput(i));
				projFieldsList.add(leftMultiJoin.getProjFields().get(i));
				joinFieldRefCountsList.add(
						leftMultiJoin.getJoinFieldRefCountsMap().get(i).toIntArray());
			}
		} else {
			newInputs.add(left);
			projFieldsList.add(null);
			joinFieldRefCountsList.add(
					new int[left.getRowType().getFieldCount()]);
		}

		if (canCombine(right, join.getJoinType().generatesNullsOnRight())) {
			final MultiJoin rightMultiJoin = (MultiJoin) right;
			for (int i = 0; i < right.getInputs().size(); i++) {
				newInputs.add(rightMultiJoin.getInput(i));
				projFieldsList.add(
						rightMultiJoin.getProjFields().get(i));
				joinFieldRefCountsList.add(
						rightMultiJoin.getJoinFieldRefCountsMap().get(i).toIntArray());
			}
		} else {
			newInputs.add(right);
			projFieldsList.add(null);
			joinFieldRefCountsList.add(
					new int[right.getRowType().getFieldCount()]);
		}

		return newInputs;
	}

	/**
	 * Combines the outer join conditions and join types from the left and right
	 * join inputs. If the join itself is either a left or right outer join,
	 * then the join condition corresponding to the join is also set in the
	 * position corresponding to the null-generating input into the join. The
	 * join type is also set.
	 *
	 * @param joinRel        join rel
	 * @param combinedInputs the combined inputs to the join
	 * @param left           left child of the joinrel
	 * @param right          right child of the joinrel
	 * @param joinSpecs      the list where the join types and conditions will be
	 *                       copied
	 */
	private void combineOuterJoins(
			Join joinRel,
			List<RelNode> combinedInputs,
			RelNode left,
			RelNode right,
			List<Pair<JoinRelType, RexNode>> joinSpecs) {
		JoinRelType joinType = joinRel.getJoinType();
		boolean leftCombined =
				canCombine(left, joinType.generatesNullsOnLeft());
		boolean rightCombined =
				canCombine(right, joinType.generatesNullsOnRight());
		switch (joinType) {
			case LEFT:
				if (leftCombined) {
					copyOuterJoinInfo(
							(MultiJoin) left,
							joinSpecs,
							0,
							null,
							null);
				} else {
					joinSpecs.add(Pair.of(JoinRelType.INNER, (RexNode) null));
				}
				joinSpecs.add(Pair.of(joinType, joinRel.getCondition()));
				break;
			case RIGHT:
				joinSpecs.add(Pair.of(joinType, joinRel.getCondition()));
				if (rightCombined) {
					copyOuterJoinInfo(
							(MultiJoin) right,
							joinSpecs,
							left.getRowType().getFieldCount(),
							right.getRowType().getFieldList(),
							joinRel.getRowType().getFieldList());
				} else {
					joinSpecs.add(Pair.of(JoinRelType.INNER, (RexNode) null));
				}
				break;
			default:
				if (leftCombined) {
					copyOuterJoinInfo(
							(MultiJoin) left,
							joinSpecs,
							0,
							null,
							null);
				} else {
					joinSpecs.add(Pair.of(JoinRelType.INNER, (RexNode) null));
				}
				if (rightCombined) {
					copyOuterJoinInfo(
							(MultiJoin) right,
							joinSpecs,
							left.getRowType().getFieldCount(),
							right.getRowType().getFieldList(),
							joinRel.getRowType().getFieldList());
				} else {
					joinSpecs.add(Pair.of(JoinRelType.INNER, (RexNode) null));
				}
		}
	}

	/**
	 * Copies outer join data from a source MultiJoin to a new set of arrays.
	 * Also adjusts the conditions to reflect the new position of an input if
	 * that input ends up being shifted to the right.
	 *
	 * @param multiJoin     the source MultiJoin
	 * @param destJoinSpecs    the list where the join types and conditions will
	 *                         be copied
	 * @param adjustmentAmount if &gt; 0, the amount the RexInputRefs in the join
	 *                         conditions need to be adjusted by
	 * @param srcFields        the source fields that the original join conditions
	 *                         are referencing
	 * @param destFields       the destination fields that the new join conditions
	 */
	private void copyOuterJoinInfo(
			MultiJoin multiJoin,
			List<Pair<JoinRelType, RexNode>> destJoinSpecs,
			int adjustmentAmount,
			List<RelDataTypeField> srcFields,
			List<RelDataTypeField> destFields) {
		final List<Pair<JoinRelType, RexNode>> srcJoinSpecs =
				Pair.zip(
						multiJoin.getJoinTypes(),
						multiJoin.getOuterJoinConditions());

		if (adjustmentAmount == 0) {
			destJoinSpecs.addAll(srcJoinSpecs);
		} else {
			assert srcFields != null;
			assert destFields != null;
			int nFields = srcFields.size();
			int[] adjustments = new int[nFields];
			for (int idx = 0; idx < nFields; idx++) {
				adjustments[idx] = adjustmentAmount;
			}
			for (Pair<JoinRelType, RexNode> src
					: srcJoinSpecs) {
				destJoinSpecs.add(
						Pair.of(
								src.left,
								src.right == null
										? null
										: src.right.accept(
										new RelOptUtil.RexInputConverter(
												multiJoin.getCluster().getRexBuilder(),
												srcFields, destFields, adjustments))));
			}
		}
	}

	/**
	 * Combines the join filters from the left and right inputs (if they are
	 * MultiJoinRels) with the join filter in the joinrel into a single AND'd
	 * join filter, unless the inputs correspond to null generating inputs in an
	 * outer join.
	 *
	 * @param joinRel join rel
	 * @param left    left child of the join
	 * @param right   right child of the join
	 * @return combined join filters AND-ed together
	 */
	private List<RexNode> combineJoinFilters(
			Join joinRel,
			RelNode left,
			RelNode right) {
		JoinRelType joinType = joinRel.getJoinType();

		// AND the join condition if this isn't a left or right outer join;
		// in those cases, the outer join condition is already tracked
		// separately
		final List<RexNode> filters = new ArrayList<>();
		if ((joinType != JoinRelType.LEFT) && (joinType != JoinRelType.RIGHT)) {
			filters.add(joinRel.getCondition());
		}
		if (canCombine(left, joinType.generatesNullsOnLeft())) {
			filters.add(((MultiJoin) left).getJoinFilter());
		}
		// Need to adjust the RexInputs of the right child, since
		// those need to shift over to the right
		if (canCombine(right, joinType.generatesNullsOnRight())) {
			MultiJoin multiJoin = (MultiJoin) right;
			filters.add(
					shiftRightFilter(joinRel, left, multiJoin,
							multiJoin.getJoinFilter()));
		}

		return filters;
	}

	/**
	 * Returns whether an input can be merged into a given relational expression
	 * without changing semantics.
	 *
	 * @param input          input into a join
	 * @param nullGenerating true if the input is null generating
	 * @return true if the input can be combined into a parent MultiJoin
	 */
	private boolean canCombine(RelNode input, boolean nullGenerating) {
		return input instanceof MultiJoin
				&& !((MultiJoin) input).isFullOuterJoin()
				&& !(containsOuter((MultiJoin) input))
				&& !nullGenerating;
	}

	private boolean containsOuter(MultiJoin multiJoin) {
		for (JoinRelType joinType : multiJoin.getJoinTypes()) {
			if (joinType.isOuterJoin()) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Shifts a filter originating from the right child of the LogicalJoin to the
	 * right, to reflect the filter now being applied on the resulting
	 * MultiJoin.
	 *
	 * @param joinRel     the original LogicalJoin
	 * @param left        the left child of the LogicalJoin
	 * @param right       the right child of the LogicalJoin
	 * @param rightFilter the filter originating from the right child
	 * @return the adjusted right filter
	 */
	private RexNode shiftRightFilter(
			Join joinRel,
			RelNode left,
			MultiJoin right,
			RexNode rightFilter) {
		if (rightFilter == null) {
			return null;
		}

		int nFieldsOnLeft = left.getRowType().getFieldList().size();
		int nFieldsOnRight = right.getRowType().getFieldList().size();
		int[] adjustments = new int[nFieldsOnRight];
		for (int i = 0; i < nFieldsOnRight; i++) {
			adjustments[i] = nFieldsOnLeft;
		}
		rightFilter =
				rightFilter.accept(
						new RelOptUtil.RexInputConverter(
								joinRel.getCluster().getRexBuilder(),
								right.getRowType().getFieldList(),
								joinRel.getRowType().getFieldList(),
								adjustments));
		return rightFilter;
	}

	/**
	 * Adds on to the existing join condition reference counts the references
	 * from the new join condition.
	 *
	 * @param multiJoinInputs          inputs into the new MultiJoin
	 * @param nTotalFields             total number of fields in the MultiJoin
	 * @param joinCondition            the new join condition
	 * @param origJoinFieldRefCounts   existing join condition reference counts
	 *
	 * @return Map containing the new join condition
	 */
	private com.google.common.collect.ImmutableMap<Integer, ImmutableIntList> addOnJoinFieldRefCounts(
			List<RelNode> multiJoinInputs,
			int nTotalFields,
			RexNode joinCondition,
			List<int[]> origJoinFieldRefCounts) {
		// count the input references in the join condition
		int[] joinCondRefCounts = new int[nTotalFields];
		joinCondition.accept(new FlinkJoinToMultiJoinRule.InputReferenceCounter(joinCondRefCounts));

		// first, make a copy of the ref counters
		final Map<Integer, int[]> refCountsMap = new HashMap<>();
		int nInputs = multiJoinInputs.size();
		int currInput = 0;
		for (int[] origRefCounts : origJoinFieldRefCounts) {
			refCountsMap.put(
					currInput,
					origRefCounts.clone());
			currInput++;
		}

		// add on to the counts for each input into the MultiJoin the
		// reference counts computed for the current join condition
		currInput = -1;
		int startField = 0;
		int nFields = 0;
		for (int i = 0; i < nTotalFields; i++) {
			if (joinCondRefCounts[i] == 0) {
				continue;
			}
			while (i >= (startField + nFields)) {
				startField += nFields;
				currInput++;
				assert currInput < nInputs;
				nFields =
						multiJoinInputs.get(currInput).getRowType().getFieldCount();
			}
			int[] refCounts = refCountsMap.get(currInput);
			refCounts[i - startField] += joinCondRefCounts[i];
		}

		final com.google.common.collect.ImmutableMap.Builder<Integer, ImmutableIntList> builder =
				com.google.common.collect.ImmutableMap.builder();
		for (Map.Entry<Integer, int[]> entry : refCountsMap.entrySet()) {
			builder.put(entry.getKey(), ImmutableIntList.of(entry.getValue()));
		}
		return builder.build();
	}

	/**
	 * Combines the post-join filters from the left and right inputs (if they
	 * are MultiJoinRels) into a single AND'd filter.
	 *
	 * @param joinRel the original LogicalJoin
	 * @param left    left child of the LogicalJoin
	 * @param right   right child of the LogicalJoin
	 * @return combined post-join filters AND'd together
	 */
	private List<RexNode> combinePostJoinFilters(
			Join joinRel,
			RelNode left,
			RelNode right) {
		final List<RexNode> filters = new ArrayList<>();
		if (right instanceof MultiJoin) {
			final MultiJoin multiRight = (MultiJoin) right;
			filters.add(
					shiftRightFilter(joinRel, left, multiRight,
							multiRight.getPostJoinFilter()));
		}

		if (left instanceof MultiJoin) {
			filters.add(((MultiJoin) left).getPostJoinFilter());
		}

		return filters;
	}

	//~ Inner Classes ----------------------------------------------------------

	/**
	 * Visitor that keeps a reference count of the inputs used by an expression.
	 */
	private class InputReferenceCounter extends RexVisitorImpl<Void> {
		private final int[] refCounts;

		InputReferenceCounter(int[] refCounts) {
			super(true);
			this.refCounts = refCounts;
		}

		public Void visitInputRef(RexInputRef inputRef) {
			refCounts[inputRef.getIndex()]++;
			return null;
		}
	}
}

// End FlinkJoinToMultiJoinRule.java
