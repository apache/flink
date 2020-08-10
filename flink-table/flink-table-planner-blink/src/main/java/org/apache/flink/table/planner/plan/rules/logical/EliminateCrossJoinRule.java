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

import org.apache.flink.util.Preconditions;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.rules.LoptMultiJoin;
import org.apache.calcite.rel.rules.MultiJoin;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.Mappings;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * This rule tries to eliminate cross joins by reordering joins.
 * The new order of joins are determined with the following steps:
 *
 * <p>1. The inputs related with an equi-join filter (= or IS NOT DISTINCT FROM) will be joined first.
 *       Inputs with smaller indices has higher priority to preserve original join order as mush as it is possible.
 *
 * <p>2. The inputs related with other join filters will then be joined.
 *
 * <p>3. If not all inner join inputs are joined, they will be cross-joined in input order.
 * 		 We are unable to eliminate these cross joins.
 *
 * <p>4. Outer joins are added.
 *
 * <p>For example, consider the following SQL:
 * <blockquote><pre><code>
 *     SELECT * FROM A, B, C, D, E, F, G, H
 *     WHERE A.f1 = C.f1
 *     AND C.f2 = D.f2
 *     AND D.f3 = B.f3
 *     AND C.f4 = B.f4
 *     AND E.f5 = G.f5
 *     AND F.f6 = H.f6
 *     AND (A.f7 < E.f7 OR B.f8 > G.f8)
 * </code></pre></blockquote>
 *
 * <p>The inputs are joined with the following steps:
 * <ol>
 *     <li>Join (A&C, B&C, B&D), (E&G), (F&H) as they are related with an equi-join filter.</li>
 *     <li>Join ABCD & EG, as there is a non-equal filter between them.</li>
 *     <li>Join ABCDEG & FH, as all filters are consumed and not all inputs have been joined together.</li>
 * </ol>
 */
public class EliminateCrossJoinRule extends RelOptRule {

	public static final EliminateCrossJoinRule INSTANCE = new EliminateCrossJoinRule();

	private EliminateCrossJoinRule() {
		super(operand(MultiJoin.class, any()), "EliminateCrossJoinRule");
	}

	@Override
	public void onMatch(RelOptRuleCall call) {
		MultiJoin join = call.rel(0);
		RelBuilder relBuilder = call.builder();

		if (join.isFullOuterJoin()) {
			// full outer join, do not reorder joins
			Preconditions.checkArgument(
				join.getInputs().size() == 2,
				"Full outer join must have exactly 2 inputs. This is a bug.");

			relBuilder
				.push(join.getInput(0))
				.push(join.getInput(1))
				.join(JoinRelType.FULL, join.getJoinFilter());
			if (join.getPostJoinFilter() != null) {
				relBuilder.filter(join.getPostJoinFilter());
			}
		} else {
			validateOuterJoins(join);

			LoptMultiJoin loptMultiJoin = new LoptMultiJoin(join);

			// try to eliminate cross join
			Vertex joinVertexTree = multiJoinToJoinVertexTree(loptMultiJoin);
			Mappings.TargetMapping mapping = joinVertexTreeToJoinRelTree(joinVertexTree, loptMultiJoin, relBuilder);

			// apply post-join filters
			if (join.getPostJoinFilter() != null) {
				RexNode adjustedFilter = adjustInputRefsInFilter(join.getPostJoinFilter(), mapping);
				relBuilder.filter(adjustedFilter);
			}

			// use projections to keep the output of the join unchanged
			List<RexNode> projects = generateProjection(join, mapping);
			relBuilder.project(projects);
		}

		RelNode rel = relBuilder.build();
		call.transformTo(rel);
	}

	private Vertex multiJoinToJoinVertexTree(LoptMultiJoin multiJoin) {
		JoinVertexTreeBuilder builder = new JoinVertexTreeBuilder(
			multiJoin,
			(left, right) -> {
				boolean leftIsEqui = isEquiFilter(left.filter);
				boolean rightIsEqui = isEquiFilter(right.filter);
				if (leftIsEqui ^ rightIsEqui) {
					// one of the filter is not an equi-filter
					// equi-filter has higher priority
					return leftIsEqui ? -1 : 1;
				} else {
					// both or none of the filter is an equi-filter
					// the one with the smallest input wins
					int leftSetBit = -1;
					int rightSetBit = -1;
					do {
						leftSetBit = left.inputBitSet.nextSetBit(leftSetBit + 1);
						rightSetBit = right.inputBitSet.nextSetBit(rightSetBit + 1);
					} while (leftSetBit == rightSetBit && leftSetBit >= 0);

					if (leftSetBit >= 0 && rightSetBit >= 0) {
						return leftSetBit - rightSetBit;
					} else if (leftSetBit < 0 && rightSetBit < 0) {
						return 0;
					} else {
						return leftSetBit;
					}
				}
			});

		JoinFilter bestFilter;
		while ((bestFilter = builder.getBestFilter()) != null) {
			builder.innerJoin(bestFilter.inputBitSet);
		}

		return builder.toJoinVertexTree();
	}

	private void validateOuterJoins(MultiJoin join) {
		int outerJoinCount = 0;
		for (int i = 0; i < join.getInputs().size(); i++) {
			if (join.getJoinTypes().get(i) != JoinRelType.INNER) {
				outerJoinCount++;
			}
		}
		Preconditions.checkState(
			outerJoinCount <= 1,
			"EliminateCrossJoinRule assumes that there is at most 1 outer join " +
				"in a layer of multi-join, but " + outerJoinCount + " outer joins were found.");
		if (outerJoinCount == 1) {
			int numInputs = join.getInputs().size();
			Preconditions.checkState(
				join.getJoinTypes().get(0) == JoinRelType.RIGHT ||
					join.getJoinTypes().get(numInputs - 1) == JoinRelType.LEFT,
				"EliminateCrossJoinRule assumes that " +
					"the only left outer join input must locate at the end, or" +
					"the only right outer join input must locate at the beginning");
		}
	}

	private boolean isEquiFilter(RexNode filter) {
		if (filter.isA(SqlKind.EQUALS) || filter.isA(SqlKind.IS_NOT_DISTINCT_FROM)) {
			RexCall rexCall = (RexCall) filter;
			Preconditions.checkState(
				rexCall.operands.size() == 2,
				"Expecting EQUALS and IS_NOT_DISTINCT_FROM filters to have exactly 2 inputs, " +
					"but found " + rexCall.operands.size());
			RexNode left = rexCall.operands.get(0);
			RexNode right = rexCall.operands.get(1);
			return left instanceof RexInputRef && right instanceof RexInputRef;
		}
		return false;
	}

	private static Mappings.TargetMapping joinVertexTreeToJoinRelTree(
		Vertex joinVertexTree,
		LoptMultiJoin multiJoin,
		RelBuilder relBuilder) {
		if (joinVertexTree instanceof LeafVertex) {
			LeafVertex leaf = (LeafVertex) joinVertexTree;
			int numFields = multiJoin.getNumFieldsInJoinFactor(leaf.smallestInputIdx);
			int joinStart = multiJoin.getJoinStart(leaf.smallestInputIdx);

			relBuilder.push(leaf.input).filter(leaf.filters);
			return Mappings.createShiftMapping(
				joinStart + numFields, 0, joinStart, numFields);
		} else {
			JoinVertex joinVertex = (JoinVertex) joinVertexTree;
			Mappings.TargetMapping leftMapping =
				joinVertexTreeToJoinRelTree(joinVertex.left, multiJoin, relBuilder);
			Mappings.TargetMapping rightMapping =
				joinVertexTreeToJoinRelTree(joinVertex.right, multiJoin, relBuilder);
			Mappings.TargetMapping mergedMapping = mergeMapping(leftMapping, rightMapping);
			RexBuilder rexBuilder = multiJoin.getMultiJoinRel().getCluster().getRexBuilder();

			RexNode adjustedFilter = adjustInputRefsInFilter(
				RexUtil.composeConjunction(rexBuilder, joinVertex.filters, false),
				mergedMapping);
			relBuilder.join(joinVertex.joinType, adjustedFilter);
			return mergedMapping;
		}
	}

	private static List<RexNode> generateProjection(MultiJoin join, Mappings.TargetMapping mapping) {
		List<RelDataTypeField> fields = join.getRowType().getFieldList();
		List<RexNode> projects = new ArrayList<>();
		for (int i = 0; i < mapping.getSourceCount(); i++) {
			int newIdx = mapping.getTargetOpt(i);
			projects.add(new RexInputRef(newIdx, fields.get(i).getType()));
		}
		return projects;
	}

	private static RexNode adjustInputRefsInFilter(RexNode filter, Mappings.TargetMapping mapping) {
		return filter.accept(new RexInputConverter(mapping));
	}

	private static Mappings.TargetMapping mergeMapping(Mappings.TargetMapping left, Mappings.TargetMapping right) {
		return Mappings.merge(left, Mappings.offsetTarget(right, left.getTargetCount()));
	}

	/**
	 * A wrapper class for a join filter.
	 * The bit set and list indicate that which inputs this filter is related to.
	 */
	private static class JoinFilter {
		final RexNode filter;
		final ImmutableBitSet inputBitSet;

		JoinFilter(RexNode filter, ImmutableBitSet inputBitSet) {
			this.filter = filter;
			this.inputBitSet = inputBitSet;
		}
	}

	/**
	 * A vertex in the join tree.
	 */
	private abstract static class Vertex {
		final int numFields;
		final ImmutableBitSet inputBitSet;
		final int smallestInputIdx;
		final List<RexNode> filters;

		/**
		 * @param numFields				Total number of input fields in this join tree
		 * @param inputBitSet			Indices of inputs in this join tree
		 * @param smallestInputIdx		Smallest input index in this join tree
		 * @param filters				All filters that can be applied to this join tree
		 */
		Vertex(int numFields, ImmutableBitSet inputBitSet, int smallestInputIdx, List<RexNode> filters) {
			this.numFields = numFields;
			this.inputBitSet = inputBitSet;
			this.smallestInputIdx = smallestInputIdx;
			this.filters = filters;
		}
	}

	/**
	 * A non-leaf vertex in the join tree.
	 */
	private static class JoinVertex extends Vertex {
		final JoinRelType joinType;
		final Vertex left;
		final Vertex right;

		/**
		 * @param joinType			Join type of this join vertex (INNER, LEFT, RIGHT or FULL)
		 * @param left				The left input of this join vertex
		 * @param right				The right input of this join vertex
		 * @param joinFilters		A list of join filters applicable to this join vertex
		 */
		JoinVertex(
			JoinRelType joinType,
			Vertex left,
			Vertex right,
			List<RexNode> joinFilters) {
			super(left.numFields + right.numFields,
				left.inputBitSet.union(right.inputBitSet),
				Math.min(left.smallestInputIdx, right.smallestInputIdx),
				joinFilters);
			this.joinType = joinType;
			this.left = left;
			this.right = right;
		}
	}

	/**
	 * A leaf vertex of a join tree, representing an input of the join.
	 */
	private static class LeafVertex extends Vertex {
		final RelNode input;

		/**
		 * @param input				The input
		 * @param inputIdx			The index of this input in the original multi-join
		 * @param filters			Filters which can be applied on leaf nodes
		 */
		LeafVertex(RelNode input, int inputIdx, List<RexNode> filters) {
			super(
				input.getRowType().getFieldCount(),
				ImmutableBitSet.of(inputIdx),
				inputIdx,
				filters);
			this.input = input;
		}
	}

	/**
	 * Build a {@link Vertex} tree from a {@link LoptMultiJoin}.
	 */
	private static class JoinVertexTreeBuilder {
		private final LoptMultiJoin multiJoin;
		private final Vertex[] rootVertex;

		private final Comparator<JoinFilter> comparator;
		private final List<JoinFilter> filters;
		private JoinFilter bestFilter;

		private boolean finished = false;

		JoinVertexTreeBuilder(LoptMultiJoin multiJoin, Comparator<JoinFilter> comparator) {
			this.multiJoin = multiJoin;
			this.comparator = comparator;

			this.filters = new LinkedList<>();
			List<RexNode> rexFilters = multiJoin.getJoinFilters();
			for (RexNode rex : rexFilters) {
				filters.add(new JoinFilter(rex, multiJoin.getFactorsRefByJoinFilter(rex)));
			}

			int numInputs = multiJoin.getNumJoinFactors();
			this.rootVertex = new Vertex[numInputs];
			for (int i = 0; i < numInputs; i++) {
				RelNode input = multiJoin.getJoinFactor(i);
				// current method to extract filters on input is not efficient (O(n^2) complexity in total),
				// but as there will not be many filters and this method is easy to write and understand,
				// we will not optimize it until a bad case in performance really occurs
				List<RexNode> filtersOnInput = pickJoinFilters(ImmutableBitSet.of(i), filters);
				rootVertex[i] = new LeafVertex(input, i, filtersOnInput);
			}

			updateBestFilter();
		}

		/**
		 * This method will iterate through a list of filters.
		 * If all the input refs in a filter have appeared in the given bit set,
		 * it will be put into the returned list and be removed from the original list.
		 *
		 * @param mergedBitSet		All the input refs in a filter must also appear in this bit set,
		 *                          so that the filter can be picked out
		 * @param filters			List of filters to check.
		 * 							{@link java.util.LinkedList} (or other lists whose iterator can remove
		 * 							current element in O(1) time) is recommended for better performance
		 */
		static List<RexNode> pickJoinFilters(ImmutableBitSet mergedBitSet, List<JoinFilter> filters) {
			List<RexNode> ret = new ArrayList<>();
			Iterator<JoinFilter> iter = filters.iterator();
			while (iter.hasNext()) {
				JoinFilter filter = iter.next();
				if (mergedBitSet.contains(filter.inputBitSet)) {
					ret.add(filter.filter);
					iter.remove();
				}
			}
			return ret;
		}

		JoinFilter getBestFilter() {
			return bestFilter;
		}

		/**
		 * Perform inner joins between the inputs specified by the bit set.
		 * If the inputs have been joined into other {@link Vertex}, those vertices will be joined instead.
		 *
		 * @param joinedInput			A bit set specifying which inputs to join
		 */
		void innerJoin(ImmutableBitSet joinedInput) {
			innerJoin(joinedInput.toList());
		}

		/**
		 * @param joinedInput			A list specifying which inputs to join
		 */
		void innerJoin(List<Integer> joinedInput) {
			Preconditions.checkArgument(
				joinedInput.size() >= 2, "At least 2 inputs are needed to perform a join.");
			for (int i = 1; i < joinedInput.size(); i++) {
				Vertex left = findRootVertex(joinedInput.get(i - 1));
				Vertex right = findRootVertex(joinedInput.get(i));
				if (left.smallestInputIdx < right.smallestInputIdx) {
					innerJoin(left, right);
				} else if (left.smallestInputIdx > right.smallestInputIdx) {
					innerJoin(right, left);
				}
			}
		}

		/**
		 * NOTE: This method can only be called once for each builder instance.
		 *
		 * @return		A {@link Vertex} tree equivalent to the given {@link LoptMultiJoin}
		 */
		Vertex toJoinVertexTree() {
			Preconditions.checkState(
				!finished,
				"`toJoinVertexTree` can only be called once for each builder instance");
			finished = true;

			// if not all filters have been applied, join and apply them first
			while (!filters.isEmpty()) {
				innerJoin(filters.get(0).inputBitSet);
			}

			// it is possible that all inner join inputs haven't been joined together,
			// so join them in order
			List<JoinRelType> joinTypes = multiJoin.getMultiJoinRel().getJoinTypes();
			Vertex lastVertex = null;
			for (int i = 0; i < rootVertex.length; i++) {
				if (joinTypes.get(i) == JoinRelType.INNER) {
					Vertex currentVertex = findRootVertex(i);
					if (lastVertex == null) {
						lastVertex = currentVertex;
					} else if (lastVertex.smallestInputIdx != currentVertex.smallestInputIdx) {
						lastVertex = innerJoin(lastVertex, currentVertex);
					}
				}
			}
			Preconditions.checkNotNull(lastVertex, "Inner join input not found. This is a bug.");

			// finally add outer join
			int numInputs = joinTypes.size();
			if (joinTypes.get(0) == JoinRelType.RIGHT) {
				lastVertex = new JoinVertex(
					JoinRelType.RIGHT,
					findRootVertex(0),
					lastVertex,
					Collections.singletonList(multiJoin.getOuterJoinCond(0)));
			} else if (joinTypes.get(numInputs - 1) == JoinRelType.LEFT) {
				lastVertex = new JoinVertex(
					JoinRelType.LEFT,
					lastVertex,
					findRootVertex(numInputs - 1),
					Collections.singletonList(multiJoin.getOuterJoinCond(numInputs - 1)));
			}

			return lastVertex;
		}

		private Vertex innerJoin(Vertex left, Vertex right) {
			List<RexNode> pickedFilters = pickJoinFilters(left.inputBitSet.union(right.inputBitSet), filters);
			updateBestFilter();
			Vertex merged = new JoinVertex(JoinRelType.INNER, left, right, pickedFilters);
			rootVertex[left.smallestInputIdx] = merged;
			rootVertex[right.smallestInputIdx] = merged;
			return merged;
		}

		private Vertex findRootVertex(int inputIdx) {
			// union-find algorithm to find out
			// which join vertex this inputIdx belongs to
			if (rootVertex[inputIdx].smallestInputIdx != inputIdx) {
				rootVertex[inputIdx] = findRootVertex(rootVertex[inputIdx].smallestInputIdx);
			}
			return rootVertex[inputIdx];
		}

		private void updateBestFilter() {
			bestFilter = null;
			for (JoinFilter joinFilter : filters) {
				if (bestFilter == null || comparator.compare(joinFilter, bestFilter) < 0) {
					bestFilter = joinFilter;
				}
			}
		}
	}

	/**
	 * Simple converter which converts input refs to a new index according to a {@link Mappings.TargetMapping}.
	 */
	private static class RexInputConverter extends RexShuttle {
		private Mappings.TargetMapping mapping;

		RexInputConverter(Mappings.TargetMapping mapping) {
			this.mapping = mapping;
		}

		@Override
		public RexNode visitInputRef(RexInputRef ref) {
			int target = mapping.getTargetOpt(ref.getIndex());
			return new RexInputRef(target, ref.getType());
		}
	}
}
