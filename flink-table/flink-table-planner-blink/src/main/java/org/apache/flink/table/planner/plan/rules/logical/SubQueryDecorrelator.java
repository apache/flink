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

import org.apache.flink.table.planner.calcite.FlinkRelBuilder;
import org.apache.flink.table.planner.plan.utils.FlinkRelOptUtil;
import org.apache.flink.table.planner.plan.utils.FlinkRexUtil;
import org.apache.flink.util.Preconditions;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalIntersect;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalMinus;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Bug;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.ReflectUtil;
import org.apache.calcite.util.ReflectiveVisitor;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.mapping.Mappings;

import javax.annotation.Nonnull;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * SubQueryDecorrelator finds all correlated expressions in a SubQuery,
 * and gets an equivalent non-correlated relational expression tree and correlation conditions.
 *
 * <p>The Basic idea of SubQueryDecorrelator is from {@link org.apache.calcite.sql2rel.RelDecorrelator},
 * however there are differences between them:
 * 1. This class works with {@link RexSubQuery}, while RelDecorrelator works with {@link LogicalCorrelate}.
 * 2. This class will get an equivalent non-correlated expressions tree and correlation conditions,
 * while RelDecorrelator will replace all correlated expressions with non-correlated expressions that are produced
 * from joining the RelNode.
 * 3. This class supports both equi and non-equi correlation conditions,
 * while RelDecorrelator only supports equi correlation conditions.
 */
public class SubQueryDecorrelator extends RelShuttleImpl {
	private final SubQueryRelDecorrelator decorrelator;
	private final RelBuilder relBuilder;

	// map a SubQuery to an equivalent RelNode and correlation-condition pair
	private final Map<RexSubQuery, Pair<RelNode, RexNode>> subQueryMap = new HashMap<>();

	private SubQueryDecorrelator(SubQueryRelDecorrelator decorrelator, RelBuilder relBuilder) {
		this.decorrelator = decorrelator;
		this.relBuilder = relBuilder;
	}

	/**
	 * Decorrelates a subquery.
	 *
	 * <p>This is the main entry point to {@code SubQueryDecorrelator}.
	 *
	 * @param rootRel The node which has SubQuery.
	 * @return Decorrelate result.
	 */
	public static Result decorrelateQuery(RelNode rootRel) {
		int maxCnfNodeCount = FlinkRelOptUtil.getMaxCnfNodeCount(rootRel);

		final CorelMapBuilder builder = new CorelMapBuilder(maxCnfNodeCount);
		final CorelMap corelMap = builder.build(rootRel);
		if (builder.hasNestedCorScope || builder.hasUnsupportedCorCondition) {
			return null;
		}

		if (!corelMap.hasCorrelation()) {
			return Result.EMPTY;
		}

		RelOptCluster cluster = rootRel.getCluster();
		RelBuilder relBuilder = new FlinkRelBuilder(cluster.getPlanner().getContext(), cluster, null);
		RexBuilder rexBuilder = cluster.getRexBuilder();

		final SubQueryDecorrelator decorrelator = new SubQueryDecorrelator(
				new SubQueryRelDecorrelator(corelMap, relBuilder, rexBuilder, maxCnfNodeCount),
				relBuilder);
		rootRel.accept(decorrelator);

		return new Result(decorrelator.subQueryMap);
	}

	@Override
	protected RelNode visitChild(RelNode parent, int i, RelNode input) {
		return super.visitChild(parent, i, stripHep(input));
	}

	@Override
	public RelNode visit(final LogicalFilter filter) {
		try {
			stack.push(filter);
			filter.getCondition().accept(handleSubQuery(filter));
		} finally {
			stack.pop();
		}
		return super.visit(filter);
	}

	private RexVisitorImpl<Void> handleSubQuery(final RelNode rel) {
		return new RexVisitorImpl<Void>(true) {

			@Override
			public Void visitSubQuery(RexSubQuery subQuery) {
				RelNode newRel = subQuery.rel;
				if (subQuery.getKind() == SqlKind.IN) {
					newRel = addProjectionForIn(subQuery.rel);
				}
				final Frame frame = decorrelator.getInvoke(newRel);
				if (frame != null && frame.c != null) {

					Frame target = frame;
					if (subQuery.getKind() == SqlKind.EXISTS) {
						target = addProjectionForExists(frame);
					}

					final DecorrelateRexShuttle shuttle = new DecorrelateRexShuttle(
							rel.getRowType(),
							target.r.getRowType(),
							rel.getVariablesSet());

					final RexNode newCondition = target.c.accept(shuttle);
					Pair<RelNode, RexNode> newNodeAndCondition = new Pair<>(target.r, newCondition);
					subQueryMap.put(subQuery, newNodeAndCondition);
				}
				return null;
			}
		};
	}

	/**
	 * Adds Projection to adjust the field index for join condition.
	 *
	 * <p>e.g. SQL: SELECT * FROM l WHERE b IN (SELECT COUNT(*) FROM r WHERE l.c = r.f
	 * the rel in SubQuery is `LogicalAggregate(group=[{}], EXPR$1=[COUNT()])`.
	 * After decorrelated, it was changed to `LogicalAggregate(group=[{0}], EXPR$0=[COUNT()])`,
	 * and the output index of `COUNT()` was changed from 0 to 1.
	 * So, add a project (`LogicalProject(EXPR$0=[$1], f=[$0])`) to adjust output fields order.
	 */
	private RelNode addProjectionForIn(RelNode relNode) {
		if (relNode instanceof LogicalProject) {
			return relNode;
		}

		RelDataType rowType = relNode.getRowType();
		final List<RexNode> projects = new ArrayList<>();
		for (int i = 0; i < rowType.getFieldCount(); ++i) {
			projects.add(RexInputRef.of(i, rowType));
		}

		relBuilder.clear();
		relBuilder.push(relNode);
		relBuilder.project(projects, rowType.getFieldNames(), true);
		return relBuilder.build();
	}

	/**
	 * Adds Projection to choose the fields used by join condition.
	 */
	private Frame addProjectionForExists(Frame frame) {
		final List<Integer> corIndices = new ArrayList<>(frame.getCorInputRefIndices());
		final RelNode rel = frame.r;
		final RelDataType rowType = rel.getRowType();
		if (corIndices.size() == rowType.getFieldCount()) {
			// no need projection
			return frame;
		}

		final List<RexNode> projects = new ArrayList<>();
		final Map<Integer, Integer> mapInputToOutput = new HashMap<>();

		Collections.sort(corIndices);
		int newPos = 0;
		for (int index : corIndices) {
			projects.add(RexInputRef.of(index, rowType));
			mapInputToOutput.put(index, newPos++);
		}

		relBuilder.clear();
		relBuilder.push(frame.r);
		relBuilder.project(projects);
		final RelNode newProject = relBuilder.build();
		final RexNode newCondition = adjustInputRefs(frame.c, mapInputToOutput, newProject.getRowType());

		// There is no old RelNode corresponding to newProject, so oldToNewOutputs is empty.
		return new Frame(rel, newProject, newCondition, new HashMap<>());
	}

	private static RelNode stripHep(RelNode rel) {
		if (rel instanceof HepRelVertex) {
			HepRelVertex hepRelVertex = (HepRelVertex) rel;
			rel = hepRelVertex.getCurrentRel();
		}
		return rel;
	}

	private static void analyzeCorConditions(
			final Set<CorrelationId> variableSet,
			final RexNode condition,
			final RexBuilder rexBuilder,
			final int maxCnfNodeCount,
			final List<RexNode> corConditions,
			final List<RexNode> nonCorConditions,
			final List<RexNode> unsupportedCorConditions) {
		// converts the expanded expression to conjunctive normal form,
		// like "(a AND b) OR c" will be converted to "(a OR c) AND (b OR c)"
		final RexNode cnf = FlinkRexUtil.toCnf(rexBuilder, maxCnfNodeCount, condition);
		// converts the cnf condition to a list of AND conditions
		final List<RexNode> conjunctions = RelOptUtil.conjunctions(cnf);
		// `true` for RexNode is supported correlation condition,
		// `false` for RexNode is unsupported correlation condition,
		// `null` for RexNode is not a correlation condition.
		final RexVisitorImpl<Boolean> visitor = new RexVisitorImpl<Boolean>(true) {

			@Override
			public Boolean visitFieldAccess(RexFieldAccess fieldAccess) {
				final RexNode ref = fieldAccess.getReferenceExpr();
				if (ref instanceof RexCorrelVariable) {
					return visitCorrelVariable((RexCorrelVariable) ref);
				} else {
					return super.visitFieldAccess(fieldAccess);
				}
			}

			@Override
			public Boolean visitCorrelVariable(RexCorrelVariable correlVariable) {
				return variableSet.contains(correlVariable.id);
			}

			@Override
			public Boolean visitSubQuery(RexSubQuery subQuery) {
				final List<Boolean> result = new ArrayList<>();
				for (RexNode operand : subQuery.operands) {
					result.add(operand.accept(this));
				}
				// we do not support nested correlation variables in SubQuery, such as:
				// select * from t1 where exists(select * from t2 where t1.a = t2.c and t1.b in (select t3.d from t3)
				if (result.contains(true) || result.contains(false)) {
					return false;
				} else {
					return null;
				}
			}

			@Override
			public Boolean visitCall(RexCall call) {
				final List<Boolean> result = new ArrayList<>();
				for (RexNode operand : call.operands) {
					result.add(operand.accept(this));
				}
				if (result.contains(false)) {
					return false;
				} else if (result.contains(true)) {
					// TODO supports correlation variable with OR
					//	return call.op.getKind() != SqlKind.OR || !result.contains(null);
					return call.op.getKind() != SqlKind.OR;
				} else {
					return null;
				}
			}
		};

		for (RexNode c : conjunctions) {
			Boolean r = c.accept(visitor);
			if (r == null) {
				nonCorConditions.add(c);
			} else if (r) {
				corConditions.add(c);
			} else {
				unsupportedCorConditions.add(c);
			}
		}
	}

	/**
	 * Adjust the condition's field indices according to mapOldToNewIndex.
	 *
	 * @param c The condition to be adjusted.
	 * @param mapOldToNewIndex A map containing the mapping the old field indices to new field indices.
	 * @param rowType The row type of the new output.
	 * @return Return new condition with new field indices.
	 */
	private static RexNode adjustInputRefs(
			final RexNode c,
			final Map<Integer, Integer> mapOldToNewIndex,
			final RelDataType rowType) {
		return c.accept(new RexShuttle() {
			@Override
			public RexNode visitInputRef(RexInputRef inputRef) {
				assert mapOldToNewIndex.containsKey(inputRef.getIndex());
				int newIndex = mapOldToNewIndex.get(inputRef.getIndex());
				final RexInputRef ref = RexInputRef.of(newIndex, rowType);
				if (ref.getIndex() == inputRef.getIndex() && ref.getType() == inputRef.getType()) {
					return inputRef; // re-use old object, to prevent needless expr cloning
				} else {
					return ref;
				}
			}
		});
	}

	private static class DecorrelateRexShuttle extends RexShuttle {
		private final RelDataType leftRowType;
		private final RelDataType rightRowType;
		private final Set<CorrelationId> variableSet;

		private DecorrelateRexShuttle(
				RelDataType leftRowType,
				RelDataType rightRowType,
				Set<CorrelationId> variableSet) {
			this.leftRowType = leftRowType;
			this.rightRowType = rightRowType;
			this.variableSet = variableSet;
		}

		@Override
		public RexNode visitFieldAccess(RexFieldAccess fieldAccess) {
			final RexNode ref = fieldAccess.getReferenceExpr();
			if (ref instanceof RexCorrelVariable) {
				final RexCorrelVariable var = (RexCorrelVariable) ref;
				assert variableSet.contains(var.id);
				final RelDataTypeField field = fieldAccess.getField();
				return new RexInputRef(field.getIndex(), field.getType());
			} else {
				return super.visitFieldAccess(fieldAccess);
			}
		}

		@Override
		public RexNode visitInputRef(RexInputRef inputRef) {
			assert inputRef.getIndex() < rightRowType.getFieldCount();
			int newIndex = inputRef.getIndex() + leftRowType.getFieldCount();
			return new RexInputRef(newIndex, inputRef.getType());
		}
	}

	/**
	 * Pull out all correlation conditions from a given subquery to top level,
	 * and rebuild the subquery rel tree without correlation conditions.
	 *
	 * <p>`public` is for reflection.
	 * We use ReflectiveVisitor instead of RelShuttle because RelShuttle returns RelNode.
	 */
	public static class SubQueryRelDecorrelator implements ReflectiveVisitor {
		// map built during translation
		private final CorelMap cm;
		private final RelBuilder relBuilder;
		private final RexBuilder rexBuilder;
		private final ReflectUtil.MethodDispatcher<Frame> dispatcher =
				ReflectUtil.createMethodDispatcher(Frame.class, this, "decorrelateRel", RelNode.class);
		private final int maxCnfNodeCount;

		SubQueryRelDecorrelator(CorelMap cm, RelBuilder relBuilder, RexBuilder rexBuilder, int maxCnfNodeCount) {
			this.cm = cm;
			this.relBuilder = relBuilder;
			this.rexBuilder = rexBuilder;
			this.maxCnfNodeCount = maxCnfNodeCount;
		}

		Frame getInvoke(RelNode r) {
			return dispatcher.invoke(r);
		}

		/**
		 * Rewrite LogicalProject.
		 *
		 * <p>Rewrite logic:
		 * Pass along any correlated variables coming from the input.
		 *
		 * @param rel the project rel to rewrite
		 */
		public Frame decorrelateRel(LogicalProject rel) {
			final RelNode oldInput = rel.getInput();
			Frame frame = getInvoke(oldInput);
			if (frame == null) {
				// If input has not been rewritten, do not rewrite this rel.
				return null;
			}

			final List<RexNode> oldProjects = rel.getProjects();
			final List<RelDataTypeField> relOutput = rel.getRowType().getFieldList();
			final RelNode newInput = frame.r;

			// Project projects the original expressions,
			// plus any correlated variables the input wants to pass along.
			final List<Pair<RexNode, String>> projects = new ArrayList<>();

			// If this Project has correlated reference, produce the correlated variables in the new output.
			// TODO Currently, correlation in projection is not supported.
			assert !cm.mapRefRelToCorRef.containsKey(rel);

			final Map<Integer, Integer> mapInputToOutput = new HashMap<>();
			final Map<Integer, Integer> mapOldToNewOutputs = new HashMap<>();
			// Project projects the original expressions
			int newPos;
			for (newPos = 0; newPos < oldProjects.size(); newPos++) {
				RexNode project = adjustInputRefs(
						oldProjects.get(newPos), frame.oldToNewOutputs, newInput.getRowType());
				projects.add(newPos, Pair.of(project, relOutput.get(newPos).getName()));
				mapOldToNewOutputs.put(newPos, newPos);
				if (project instanceof RexInputRef) {
					mapInputToOutput.put(((RexInputRef) project).getIndex(), newPos);
				}
			}

			if (frame.c != null) {
				// Project any correlated variables the input wants to pass along.
				final ImmutableBitSet corInputIndices = RelOptUtil.InputFinder.bits(frame.c);
				final RelDataType inputRowType = newInput.getRowType();
				for (int inputIndex : corInputIndices.toList()) {
					if (!mapInputToOutput.containsKey(inputIndex)) {
						projects.add(newPos, Pair.of(
								RexInputRef.of(inputIndex, inputRowType),
								inputRowType.getFieldNames().get(inputIndex)));
						mapInputToOutput.put(inputIndex, newPos);
						newPos++;
					}
				}
			}
			RelNode newProject = RelOptUtil.createProject(newInput, projects, false);

			final RexNode newCorCondition;
			if (frame.c != null) {
				newCorCondition = adjustInputRefs(frame.c, mapInputToOutput, newProject.getRowType());
			} else {
				newCorCondition = null;
			}

			return new Frame(rel, newProject, newCorCondition, mapOldToNewOutputs);
		}

		/**
		 * Rewrite LogicalFilter.
		 *
		 * <p>Rewrite logic:
		 * 1. If a Filter references a correlated field in its filter condition,
		 * rewrite the Filter references only non-correlated fields,
		 * and the condition references correlated fields will be push to it's output.
		 * 2. If Filter does not reference correlated variables,
		 * simply rewrite the filter condition using new input.
		 *
		 * @param rel the filter rel to rewrite
		 */
		public Frame decorrelateRel(LogicalFilter rel) {
			final RelNode oldInput = rel.getInput();
			Frame frame = getInvoke(oldInput);
			if (frame == null) {
				// If input has not been rewritten, do not rewrite this rel.
				return null;
			}

			// Conditions reference only correlated fields
			final List<RexNode> corConditions = new ArrayList<>();
			// Conditions do not reference any correlated fields
			final List<RexNode> nonCorConditions = new ArrayList<>();
			// Conditions reference correlated fields, but not supported now
			final List<RexNode> unsupportedCorConditions = new ArrayList<>();

			analyzeCorConditions(
					cm.mapSubQueryNodeToCorSet.get(rel),
					rel.getCondition(),
					rexBuilder,
					maxCnfNodeCount,
					corConditions,
					nonCorConditions,
					unsupportedCorConditions);
			assert unsupportedCorConditions.isEmpty();

			final RexNode remainingCondition = RexUtil.composeConjunction(rexBuilder, nonCorConditions, false);

			// Using LogicalFilter.create instead of RelBuilder.filter to create Filter
			// because RelBuilder.filter method does not have VariablesSet arg.
			final LogicalFilter newFilter = LogicalFilter.create(
					frame.r,
					remainingCondition,
					com.google.common.collect.ImmutableSet.copyOf(rel.getVariablesSet()));

			// Adds input's correlation condition
			if (frame.c != null) {
				corConditions.add(frame.c);
			}

			final RexNode corCondition = RexUtil.composeConjunction(rexBuilder, corConditions, true);
			// Filter does not change the input ordering.
			// All corVars produced by filter will have the same output positions in the input rel.
			return new Frame(rel, newFilter, corCondition, frame.oldToNewOutputs);
		}

		/**
		 * Rewrites a {@link LogicalAggregate}.
		 *
		 * <p>Rewrite logic:
		 * 1. Permute the group by keys to the front.
		 * 2. If the input of an aggregate produces correlated variables, add them to the group list.
		 * 3. Change aggCalls to reference the new project.
		 *
		 * @param rel Aggregate to rewrite
		 */
		public Frame decorrelateRel(LogicalAggregate rel) {
			// Aggregate itself should not reference corVars.
			assert !cm.mapRefRelToCorRef.containsKey(rel);

			final RelNode oldInput = rel.getInput();
			final Frame frame = getInvoke(oldInput);
			if (frame == null) {
				// If input has not been rewritten, do not rewrite this rel.
				return null;
			}

			final RelNode newInput = frame.r;
			// map from newInput
			final Map<Integer, Integer> mapNewInputToProjOutputs = new HashMap<>();
			final int oldGroupKeyCount = rel.getGroupSet().cardinality();

			// Project projects the original expressions,
			// plus any correlated variables the input wants to pass along.
			final List<Pair<RexNode, String>> projects = new ArrayList<>();
			final List<RelDataTypeField> newInputOutput = newInput.getRowType().getFieldList();

			// oldInput has the original group by keys in the front.
			final NavigableMap<Integer, RexLiteral> omittedConstants = new TreeMap<>();
			int newPos = 0;
			for (int i = 0; i < oldGroupKeyCount; i++) {
				final RexLiteral constant = projectedLiteral(newInput, i);
				if (constant != null) {
					// Exclude constants. Aggregate({true}) occurs because Aggregate({})
					// would generate 1 row even when applied to an empty table.
					omittedConstants.put(i, constant);
					continue;
				}

				int newInputPos = frame.oldToNewOutputs.get(i);
				projects.add(newPos, RexInputRef.of2(newInputPos, newInputOutput));
				mapNewInputToProjOutputs.put(newInputPos, newPos);
				newPos++;
			}

			if (frame.c != null) {
				// If input produces correlated variables, move them to the front,
				// right after any existing GROUP BY fields.

				// Now add the corVars from the input, starting from position oldGroupKeyCount.
				for (Integer index : frame.getCorInputRefIndices()) {
					if (!mapNewInputToProjOutputs.containsKey(index)) {
						projects.add(newPos, RexInputRef.of2(index, newInputOutput));
						mapNewInputToProjOutputs.put(index, newPos);
						newPos++;
					}
				}
			}

			// add the remaining fields
			final int newGroupKeyCount = newPos;
			for (int i = 0; i < newInputOutput.size(); i++) {
				if (!mapNewInputToProjOutputs.containsKey(i)) {
					projects.add(newPos, RexInputRef.of2(i, newInputOutput));
					mapNewInputToProjOutputs.put(i, newPos);
					newPos++;
				}
			}

			assert newPos == newInputOutput.size();

			// This Project will be what the old input maps to,
			// replacing any previous mapping from old input).
			final RelNode newProject = RelOptUtil.createProject(newInput, projects, false);

			final RexNode newCondition;
			if (frame.c != null) {
				newCondition = adjustInputRefs(frame.c, mapNewInputToProjOutputs, newProject.getRowType());
			} else {
				newCondition = null;
			}

			// update mappings:
			// oldInput ----> newInput
			//
			//                newProject
			//                   |
			// oldInput ----> newInput
			//
			// is transformed to
			//
			// oldInput ----> newProject
			//                   |
			//                newInput

			final Map<Integer, Integer> combinedMap = new HashMap<>();
			final Map<Integer, Integer> oldToNewOutputs = new HashMap<>();
			final List<Integer> originalGrouping = rel.getGroupSet().toList();
			for (Integer oldInputPos : frame.oldToNewOutputs.keySet()) {
				final Integer newIndex = mapNewInputToProjOutputs.get(frame.oldToNewOutputs.get(oldInputPos));
				combinedMap.put(oldInputPos, newIndex);
				// mapping grouping fields
				if (originalGrouping.contains(oldInputPos)) {
					oldToNewOutputs.put(oldInputPos, newIndex);
				}
			}

			// now it's time to rewrite the Aggregate
			final ImmutableBitSet newGroupSet = ImmutableBitSet.range(newGroupKeyCount);
			final List<AggregateCall> newAggCalls = new ArrayList<>();
			final List<AggregateCall> oldAggCalls = rel.getAggCallList();

			for (AggregateCall oldAggCall : oldAggCalls) {
				final List<Integer> oldAggArgs = oldAggCall.getArgList();
				final List<Integer> aggArgs = new ArrayList<>();

				// Adjust the Aggregate argument positions.
				// Note Aggregate does not change input ordering, so the input
				// output position mapping can be used to derive the new positions
				// for the argument.
				for (int oldPos : oldAggArgs) {
					aggArgs.add(combinedMap.get(oldPos));
				}
				final int filterArg = oldAggCall.filterArg < 0
						? oldAggCall.filterArg
						: combinedMap.get(oldAggCall.filterArg);

				newAggCalls.add(
						oldAggCall.adaptTo(
								newProject, aggArgs, filterArg, oldGroupKeyCount, newGroupKeyCount));
			}

			relBuilder.push(LogicalAggregate.create(newProject, false, newGroupSet, null, newAggCalls));

			if (!omittedConstants.isEmpty()) {
				final List<RexNode> postProjects = new ArrayList<>(relBuilder.fields());
				for (Map.Entry<Integer, RexLiteral> entry : omittedConstants.entrySet()) {
					postProjects.add(mapNewInputToProjOutputs.get(entry.getKey()), entry.getValue());
				}
				relBuilder.project(postProjects);
			}

			// mapping aggCall output fields
			for (int i = 0; i < oldAggCalls.size(); ++i) {
				oldToNewOutputs.put(oldGroupKeyCount + i, newGroupKeyCount + omittedConstants.size() + i);
			}

			// Aggregate does not change input ordering so corVars will be
			// located at the same position as the input newProject.
			return new Frame(rel, relBuilder.build(), newCondition, oldToNewOutputs);
		}

		/**
		 * Rewrite LogicalJoin.
		 *
		 * <p>Rewrite logic:
		 * 1. rewrite join condition.
		 * 2. map output positions and produce corVars if any.
		 *
		 * @param rel Join
		 */
		public Frame decorrelateRel(LogicalJoin rel) {
			final RelNode oldLeft = rel.getInput(0);
			final RelNode oldRight = rel.getInput(1);

			final Frame leftFrame = getInvoke(oldLeft);
			final Frame rightFrame = getInvoke(oldRight);

			if (leftFrame == null || rightFrame == null) {
				// If any input has not been rewritten, do not rewrite this rel.
				return null;
			}

			switch (rel.getJoinType()) {
				case LEFT:
					assert rightFrame.c == null;
					break;
				case RIGHT:
					assert leftFrame.c == null;
					break;
				case FULL:
					assert leftFrame.c == null && rightFrame.c == null;
					break;
				default:
					break;
			}

			final int oldLeftFieldCount = oldLeft.getRowType().getFieldCount();
			final int newLeftFieldCount = leftFrame.r.getRowType().getFieldCount();
			final int oldRightFieldCount = oldRight.getRowType().getFieldCount();
			assert rel.getRowType().getFieldCount() == oldLeftFieldCount + oldRightFieldCount;

			final RexNode newJoinCondition = adjustJoinCondition(
					rel.getCondition(),
					oldLeftFieldCount,
					newLeftFieldCount,
					leftFrame.oldToNewOutputs,
					rightFrame.oldToNewOutputs);

			final RelNode newJoin = LogicalJoin.create(
					leftFrame.r, rightFrame.r, newJoinCondition, rel.getVariablesSet(), rel.getJoinType());

			// Create the mapping between the output of the old correlation rel and the new join rel
			final Map<Integer, Integer> mapOldToNewOutputs = new HashMap<>();
			// Left input positions are not changed.
			mapOldToNewOutputs.putAll(leftFrame.oldToNewOutputs);

			// Right input positions are shifted by newLeftFieldCount.
			for (int i = 0; i < oldRightFieldCount; i++) {
				mapOldToNewOutputs.put(i + oldLeftFieldCount, rightFrame.oldToNewOutputs.get(i) + newLeftFieldCount);
			}

			final List<RexNode> corConditions = new ArrayList<>();
			if (leftFrame.c != null) {
				corConditions.add(leftFrame.c);
			}
			if (rightFrame.c != null) {
				// Right input positions are shifted by newLeftFieldCount.
				final Map<Integer, Integer> rightMapOldToNewOutputs = new HashMap<>();
				for (int index : rightFrame.getCorInputRefIndices()) {
					rightMapOldToNewOutputs.put(index, index + newLeftFieldCount);
				}
				final RexNode newRightCondition = adjustInputRefs(
						rightFrame.c, rightMapOldToNewOutputs, newJoin.getRowType());
				corConditions.add(newRightCondition);
			}

			final RexNode newCondition = RexUtil.composeConjunction(rexBuilder, corConditions, true);
			return new Frame(rel, newJoin, newCondition, mapOldToNewOutputs);
		}

		private RexNode adjustJoinCondition(
				final RexNode joinCondition,
				final int oldLeftFieldCount,
				final int newLeftFieldCount,
				final Map<Integer, Integer> leftOldToNewOutputs,
				final Map<Integer, Integer> rightOldToNewOutputs) {
			return joinCondition.accept(new RexShuttle() {
				@Override
				public RexNode visitInputRef(RexInputRef inputRef) {
					int oldIndex = inputRef.getIndex();
					final int newIndex;
					if (oldIndex < oldLeftFieldCount) {
						// field from left
						assert leftOldToNewOutputs.containsKey(oldIndex);
						newIndex = leftOldToNewOutputs.get(oldIndex);
					} else {
						// field from right
						oldIndex = oldIndex - oldLeftFieldCount;
						assert rightOldToNewOutputs.containsKey(oldIndex);
						newIndex = rightOldToNewOutputs.get(oldIndex) + newLeftFieldCount;
					}
					return new RexInputRef(newIndex, inputRef.getType());
				}
			});
		}

		/**
		 * Rewrite Sort.
		 *
		 * <p>Rewrite logic:
		 * change the collations field to reference the new input.
		 *
		 * @param rel Sort to be rewritten
		 */
		public Frame decorrelateRel(Sort rel) {
			// Sort itself should not reference corVars.
			assert !cm.mapRefRelToCorRef.containsKey(rel);

			// Sort only references field positions in collations field.
			// The collations field in the newRel now need to refer to the
			// new output positions in its input.
			// Its output does not change the input ordering, so there's no
			// need to call propagateExpr.
			final RelNode oldInput = rel.getInput();
			final Frame frame = getInvoke(oldInput);
			if (frame == null) {
				// If input has not been rewritten, do not rewrite this rel.
				return null;
			}
			final RelNode newInput = frame.r;

			Mappings.TargetMapping mapping =
					Mappings.target(frame.oldToNewOutputs,
							oldInput.getRowType().getFieldCount(),
							newInput.getRowType().getFieldCount());

			RelCollation oldCollation = rel.getCollation();
			RelCollation newCollation = RexUtil.apply(mapping, oldCollation);

			final Sort newSort = LogicalSort.create(newInput, newCollation, rel.offset, rel.fetch);

			// Sort does not change input ordering
			return new Frame(rel, newSort, frame.c, frame.oldToNewOutputs);
		}

		/**
		 * Rewrites a {@link Values}.
		 *
		 * @param rel Values to be rewritten
		 */
		public Frame decorrelateRel(Values rel) {
			// There are no inputs, so rel does not need to be changed.
			return null;
		}

		public Frame decorrelateRel(LogicalCorrelate rel) {
			// does not allow correlation condition in its inputs now, so choose default behavior
			return decorrelateRel((RelNode) rel);
		}

		/** Fallback if none of the other {@code decorrelateRel} methods match. */
		public Frame decorrelateRel(RelNode rel) {
			RelNode newRel = rel.copy(rel.getTraitSet(), rel.getInputs());
			if (rel.getInputs().size() > 0) {
				List<RelNode> oldInputs = rel.getInputs();
				List<RelNode> newInputs = new ArrayList<>();
				for (int i = 0; i < oldInputs.size(); ++i) {
					final Frame frame = getInvoke(oldInputs.get(i));
					if (frame == null || frame.c != null) {
						// if input is not rewritten, or if it produces correlated variables, terminate rewrite
						return null;
					}
					newInputs.add(frame.r);
					newRel.replaceInput(i, frame.r);
				}

				if (!Util.equalShallow(oldInputs, newInputs)) {
					newRel = rel.copy(rel.getTraitSet(), newInputs);
				}
			}
			// the output position should not change since there are no corVars coming from below.
			return new Frame(rel, newRel, null, identityMap(rel.getRowType().getFieldCount()));
		}

		/* Returns an immutable map with the identity [0: 0, .., count-1: count-1]. */
		private static Map<Integer, Integer> identityMap(int count) {
			com.google.common.collect.ImmutableMap.Builder<Integer, Integer> builder =
					com.google.common.collect.ImmutableMap.builder();
			for (int i = 0; i < count; i++) {
				builder.put(i, i);
			}
			return builder.build();
		}

		/** Returns a literal output field, or null if it is not literal. */
		private static RexLiteral projectedLiteral(RelNode rel, int i) {
			if (rel instanceof Project) {
				final Project project = (Project) rel;
				final RexNode node = project.getProjects().get(i);
				if (node instanceof RexLiteral) {
					return (RexLiteral) node;
				}
			}
			return null;
		}
	}

	/** Builds a {@link CorelMap}. */
	private static class CorelMapBuilder extends RelShuttleImpl {
		private final int maxCnfNodeCount;
		// nested correlation variables in SubQuery, such as:
		// SELECT * FROM t1 WHERE EXISTS (SELECT * FROM t2 WHERE t1.a = t2.c AND
		// t2.d IN (SELECT t3.d FROM t3 WHERE t1.b = t3.e)
		boolean hasNestedCorScope = false;
		// has unsupported correlation condition, such as:
		// SELECT * FROM l WHERE a IN (SELECT c FROM r WHERE l.b IN (SELECT e FROM t))
		// SELECT a FROM l WHERE b IN (SELECT r1.e FROM r1 WHERE l.a = r1.d UNION SELECT r2.i FROM r2)
		// SELECT * FROM l WHERE EXISTS (SELECT * FROM r LEFT JOIN (SELECT * FROM t WHERE t.j = l.b) t1 ON r.f = t1.k)
		// SELECT * FROM l WHERE b IN (SELECT MIN(e) FROM r WHERE l.c > r.f)
		// SELECT * FROM l WHERE b IN (SELECT MIN(e) OVER() FROM r WHERE l.c > r.f)
		boolean hasUnsupportedCorCondition = false;
		// true if SubQuery rel tree has Aggregate node, else false.
		boolean hasAggregateNode = false;
		// true if SubQuery rel tree has Over node, else false.
		boolean hasOverNode = false;

		public CorelMapBuilder(int maxCnfNodeCount) {
			this.maxCnfNodeCount = maxCnfNodeCount;
		}

		final SortedMap<CorrelationId, RelNode> mapCorToCorRel = new TreeMap<>();
		final com.google.common.collect.SortedSetMultimap<RelNode, CorRef> mapRefRelToCorRef =
				com.google.common.collect.Multimaps.newSortedSetMultimap(
						new HashMap<RelNode, Collection<CorRef>>(),
						new com.google.common.base.Supplier<TreeSet<CorRef>>() {
							public TreeSet<CorRef> get() {
								Bug.upgrade("use MultimapBuilder when we're on Guava-16");
								return com.google.common.collect.Sets.newTreeSet();
							}
						});
		final Map<RexFieldAccess, CorRef> mapFieldAccessToCorVar = new HashMap<>();
		final Map<RelNode, Set<CorrelationId>> mapSubQueryNodeToCorSet = new HashMap<>();

		int corrIdGenerator = 0;
		final Deque<RelNode> corNodeStack = new ArrayDeque<>();

		/** Creates a CorelMap by iterating over a {@link RelNode} tree. */
		CorelMap build(RelNode... rels) {
			for (RelNode rel : rels) {
				stripHep(rel).accept(this);
			}
			return CorelMap.of(mapRefRelToCorRef, mapCorToCorRel, mapSubQueryNodeToCorSet);
		}

		@Override
		protected RelNode visitChild(RelNode parent, int i, RelNode input) {
			return super.visitChild(parent, i, stripHep(input));
		}

		@Override
		public RelNode visit(LogicalCorrelate correlate) {
			// TODO does not allow correlation condition in its inputs now
			// If correlation conditions in correlate inputs reference to correlate outputs variable,
			// that should not be supported, e.g.
			// SELECT * FROM outer_table l WHERE l.c IN (
			//  SELECT f1 FROM (
			//   SELECT * FROM inner_table r WHERE r.d IN (SELECT x.i FROM x WHERE x.j = l.b)) t,
			//   LATERAL TABLE(table_func(t.f)) AS T(f1)
			//  ))
			// other cases should be supported, e.g.
			// SELECT * FROM outer_table l WHERE l.c IN (
			//  SELECT f1 FROM (
			//   SELECT * FROM inner_table r WHERE r.d IN (SELECT x.i FROM x WHERE x.j = r.e)) t,
			//   LATERAL TABLE(table_func(t.f)) AS T(f1)
			//  ))
			checkCorConditionOfInput(correlate.getLeft());
			checkCorConditionOfInput(correlate.getRight());

			visitChild(correlate, 0, correlate.getLeft());
			visitChild(correlate, 1, correlate.getRight());
			return correlate;
		}

		@Override
		public RelNode visit(LogicalJoin join) {
			switch (join.getJoinType()) {
				case LEFT:
					checkCorConditionOfInput(join.getRight());
					break;
				case RIGHT:
					checkCorConditionOfInput(join.getLeft());
					break;
				case FULL:
					checkCorConditionOfInput(join.getLeft());
					checkCorConditionOfInput(join.getRight());
					break;
				default:
					break;
			}

			final boolean hasSubQuery = RexUtil.SubQueryFinder.find(join.getCondition()) != null;
			try {
				if (!corNodeStack.isEmpty()) {
					mapSubQueryNodeToCorSet.put(join, corNodeStack.peek().getVariablesSet());
				}
				if (hasSubQuery) {
					corNodeStack.push(join);
				}
				checkCorCondition(join);
				join.getCondition().accept(rexVisitor(join));
			} finally {
				if (hasSubQuery) {
					corNodeStack.pop();
				}
			}
			visitChild(join, 0, join.getLeft());
			visitChild(join, 1, join.getRight());
			return join;
		}

		@Override
		public RelNode visit(LogicalFilter filter) {
			final boolean hasSubQuery = RexUtil.SubQueryFinder.find(filter.getCondition()) != null;
			try {
				if (!corNodeStack.isEmpty()) {
					mapSubQueryNodeToCorSet.put(filter, corNodeStack.peek().getVariablesSet());
				}
				if (hasSubQuery) {
					corNodeStack.push(filter);
				}
				checkCorCondition(filter);
				filter.getCondition().accept(rexVisitor(filter));
				for (CorrelationId correlationId : filter.getVariablesSet()) {
					mapCorToCorRel.put(correlationId, filter);
				}
			} finally {
				if (hasSubQuery) {
					corNodeStack.pop();
				}
			}
			return super.visit(filter);
		}

		@Override
		public RelNode visit(LogicalProject project) {
			hasOverNode = RexOver.containsOver(project.getProjects(), null);
			final boolean hasSubQuery = RexUtil.SubQueryFinder.find(project.getProjects()) != null;
			try {
				if (!corNodeStack.isEmpty()) {
					mapSubQueryNodeToCorSet.put(project, corNodeStack.peek().getVariablesSet());
				}
				if (hasSubQuery) {
					corNodeStack.push(project);
				}
				checkCorCondition(project);
				for (RexNode node : project.getProjects()) {
					node.accept(rexVisitor(project));
				}
			} finally {
				if (hasSubQuery) {
					corNodeStack.pop();
				}
			}
			return super.visit(project);
		}

		@Override
		public RelNode visit(LogicalAggregate aggregate) {
			hasAggregateNode = true;
			return super.visit(aggregate);
		}

		@Override
		public RelNode visit(LogicalUnion union) {
			checkCorConditionOfSetOpInputs(union);
			return super.visit(union);
		}

		@Override
		public RelNode visit(LogicalMinus minus) {
			checkCorConditionOfSetOpInputs(minus);
			return super.visit(minus);
		}

		@Override
		public RelNode visit(LogicalIntersect intersect) {
			checkCorConditionOfSetOpInputs(intersect);
			return super.visit(intersect);
		}

		/**
		 * check whether the predicate on filter has unsupported correlation condition.
		 * e.g. SELECT * FROM l WHERE a IN (SELECT c FROM r WHERE l.b = r.d OR r.d > 10)
		 */
		private void checkCorCondition(final LogicalFilter filter) {
			if (mapSubQueryNodeToCorSet.containsKey(filter) && !hasUnsupportedCorCondition) {
				final List<RexNode> corConditions = new ArrayList<>();
				final List<RexNode> unsupportedCorConditions = new ArrayList<>();
				analyzeCorConditions(
						mapSubQueryNodeToCorSet.get(filter),
						filter.getCondition(),
						filter.getCluster().getRexBuilder(),
						maxCnfNodeCount,
						corConditions,
						new ArrayList<>(),
						unsupportedCorConditions);
				if (!unsupportedCorConditions.isEmpty()) {
					hasUnsupportedCorCondition = true;
				} else if (!corConditions.isEmpty()) {
					boolean hasNonEquals = false;
					for (RexNode node : corConditions) {
						if (node instanceof RexCall && ((RexCall) node).getOperator() != SqlStdOperatorTable.EQUALS) {
							hasNonEquals = true;
							break;
						}
					}
					// agg or over with non-equality correlation condition is unsupported, e.g.
					// SELECT * FROM l WHERE b IN (SELECT MIN(e) FROM r WHERE l.c > r.f)
					// SELECT * FROM l WHERE b IN (SELECT MIN(e) OVER() FROM r WHERE l.c > r.f)
					hasUnsupportedCorCondition = hasNonEquals && (hasAggregateNode || hasOverNode);
				}
			}
		}

		/**
		 * check whether the predicate on join has unsupported correlation condition.
		 * e.g. SELECT * FROM l WHERE a IN (SELECT c FROM r WHERE l.b IN (SELECT e FROM t))
		 */
		private void checkCorCondition(final LogicalJoin join) {
			if (!hasUnsupportedCorCondition) {
				join.getCondition().accept(new RexVisitorImpl<Void>(true) {
					@Override
					public Void visitCorrelVariable(RexCorrelVariable correlVariable) {
						hasUnsupportedCorCondition = true;
						return super.visitCorrelVariable(correlVariable);
					}
				});
			}
		}

		/**
		 * check whether the project has correlation expressions.
		 * e.g. SELECT * FROM l WHERE a IN (SELECT l.b FROM r)
		 */
		private void checkCorCondition(final LogicalProject project) {
			if (!hasUnsupportedCorCondition) {
				for (RexNode node : project.getProjects()) {
					node.accept(new RexVisitorImpl<Void>(true) {
						@Override
						public Void visitCorrelVariable(RexCorrelVariable correlVariable) {
							hasUnsupportedCorCondition = true;
							return super.visitCorrelVariable(correlVariable);
						}
					});
				}
			}
		}

		/**
		 * check whether a node has some input which have correlation condition.
		 * e.g. SELECT * FROM l WHERE EXISTS (SELECT * FROM r LEFT JOIN (SELECT * FROM t WHERE t.j=l.b) t1 ON r.f=t1.k)
		 * the above sql can not be converted to semi-join plan,
		 * because the right input of Left-Join has the correlation condition(t.j=l.b).
		 */
		private void checkCorConditionOfInput(final RelNode input) {
			final RelShuttleImpl shuttle = new RelShuttleImpl() {
				final RexVisitor<Void> visitor = new RexVisitorImpl<Void>(true) {
					@Override
					public Void visitCorrelVariable(RexCorrelVariable correlVariable) {
						hasUnsupportedCorCondition = true;
						return super.visitCorrelVariable(correlVariable);
					}
				};

				@Override
				public RelNode visit(LogicalFilter filter) {
					filter.getCondition().accept(visitor);
					return super.visit(filter);
				}

				@Override
				public RelNode visit(LogicalProject project) {
					for (RexNode rex : project.getProjects()) {
						rex.accept(visitor);
					}
					return super.visit(project);
				}

				@Override
				public RelNode visit(LogicalJoin join) {
					join.getCondition().accept(visitor);
					return super.visit(join);
				}
			};
			input.accept(shuttle);
		}

		/**
		 * check whether a SetOp has some children node which have correlation condition.
		 * e.g. SELECT a FROM l WHERE b IN (SELECT r1.e FROM r1 WHERE l.a = r1.d UNION SELECT r2.i FROM r2)
		 */
		private void checkCorConditionOfSetOpInputs(SetOp setOp) {
			for (RelNode child : setOp.getInputs()) {
				checkCorConditionOfInput(child);
			}
		}

		private RexVisitorImpl<Void> rexVisitor(final RelNode rel) {
			return new RexVisitorImpl<Void>(true) {
				@Override
				public Void visitSubQuery(RexSubQuery subQuery) {
					hasAggregateNode = false; // reset to default value
					hasOverNode = false; // reset to default value
					subQuery.rel.accept(CorelMapBuilder.this);
					return super.visitSubQuery(subQuery);
				}

				@Override
				public Void visitFieldAccess(RexFieldAccess fieldAccess) {
					final RexNode ref = fieldAccess.getReferenceExpr();
					if (ref instanceof RexCorrelVariable) {
						final RexCorrelVariable var = (RexCorrelVariable) ref;
						// check the scope of correlation id
						// we do not support nested correlation variables in SubQuery, such as:
						// select * from t1 where exists (select * from t2 where t1.a = t2.c and
						// t2.d in (select t3.d from t3 where t1.b = t3.e)
						if (!hasUnsupportedCorCondition) {
							hasUnsupportedCorCondition = !mapSubQueryNodeToCorSet.containsKey(rel);
						}
						if (!hasNestedCorScope && mapSubQueryNodeToCorSet.containsKey(rel)) {
							hasNestedCorScope = !mapSubQueryNodeToCorSet.get(rel).contains(var.id);
						}

						if (mapFieldAccessToCorVar.containsKey(fieldAccess)) {
							// for cases where different Rel nodes are referring to
							// same correlation var (e.g. in case of NOT IN)
							// avoid generating another correlation var
							// and record the 'rel' is using the same correlation
							mapRefRelToCorRef.put(rel, mapFieldAccessToCorVar.get(fieldAccess));
						} else {
							final CorRef correlation = new CorRef(
									var.id,
									fieldAccess.getField().getIndex(),
									corrIdGenerator++);
							mapFieldAccessToCorVar.put(fieldAccess, correlation);
							mapRefRelToCorRef.put(rel, correlation);
						}
					}
					return super.visitFieldAccess(fieldAccess);
				}
			};
		}
	}

	/**
	 * A unique reference to a correlation field.
	 *
	 * <p>For instance, if a RelNode references emp.name multiple times, it would
	 * result in multiple {@code CorRef} objects that differ just in
	 * {@link CorRef#uniqueKey}.
	 */
	private static class CorRef implements Comparable<CorRef> {
		final int uniqueKey;
		final CorrelationId corr;
		final int field;

		CorRef(CorrelationId corr, int field, int uniqueKey) {
			this.corr = corr;
			this.field = field;
			this.uniqueKey = uniqueKey;
		}

		@Override
		public String toString() {
			return corr.getName() + '.' + field;
		}

		@Override
		public int hashCode() {
			return Objects.hash(uniqueKey, corr, field);
		}

		@Override
		public boolean equals(Object o) {
			return this == o
					|| o instanceof CorRef
					&& uniqueKey == ((CorRef) o).uniqueKey
					&& corr == ((CorRef) o).corr
					&& field == ((CorRef) o).field;
		}

		public int compareTo(@Nonnull CorRef o) {
			int c = corr.compareTo(o.corr);
			if (c != 0) {
				return c;
			}
			c = Integer.compare(field, o.field);
			if (c != 0) {
				return c;
			}
			return Integer.compare(uniqueKey, o.uniqueKey);
		}
	}

	/**
	 * A map of the locations of correlation variables in a tree of {@link RelNode}s.
	 *
	 * <p>It is used to drive the decorrelation process.
	 * Treat it as immutable; rebuild if you modify the tree.
	 *
	 * <p>There are three maps:<ol>
	 *
	 * <li>{@link #mapRefRelToCorRef} maps a {@link RelNode} to the correlated variables it references;
	 *
	 * <li>{@link #mapCorToCorRel} maps a correlated variable to the {@link RelNode} providing it;
	 *
	 * <li>{@link #mapSubQueryNodeToCorSet} maps a {@link RelNode} to the correlated variables it has;
	 *
	 * </ol>
	 */
	private static class CorelMap {
		private final com.google.common.collect.Multimap<RelNode, CorRef> mapRefRelToCorRef;
		private final SortedMap<CorrelationId, RelNode> mapCorToCorRel;
		private final Map<RelNode, Set<CorrelationId>> mapSubQueryNodeToCorSet;

		// TODO: create immutable copies of all maps
		private CorelMap(
				com.google.common.collect.Multimap<RelNode, CorRef> mapRefRelToCorRef,
				SortedMap<CorrelationId, RelNode> mapCorToCorRel,
				Map<RelNode, Set<CorrelationId>> mapSubQueryNodeToCorSet) {
			this.mapRefRelToCorRef = mapRefRelToCorRef;
			this.mapCorToCorRel = mapCorToCorRel;
			this.mapSubQueryNodeToCorSet = com.google.common.collect.ImmutableMap.copyOf(mapSubQueryNodeToCorSet);
		}

		@Override
		public String toString() {
			return "mapRefRelToCorRef=" + mapRefRelToCorRef
					+ "\nmapCorToCorRel=" + mapCorToCorRel
					+ "\nmapSubQueryNodeToCorSet=" + mapSubQueryNodeToCorSet
					+ "\n";
		}

		@Override
		public boolean equals(Object obj) {
			return obj == this
					|| obj instanceof CorelMap
					&& mapRefRelToCorRef.equals(((CorelMap) obj).mapRefRelToCorRef)
					&& mapCorToCorRel.equals(((CorelMap) obj).mapCorToCorRel)
					&& mapSubQueryNodeToCorSet.equals(((CorelMap) obj).mapSubQueryNodeToCorSet);
		}

		@Override
		public int hashCode() {
			return Objects.hash(mapRefRelToCorRef, mapCorToCorRel, mapSubQueryNodeToCorSet);
		}

		/** Creates a CorelMap with given contents. */
		public static CorelMap of(
				com.google.common.collect.SortedSetMultimap<RelNode, CorRef> mapRefRelToCorVar,
				SortedMap<CorrelationId, RelNode> mapCorToCorRel,
				Map<RelNode, Set<CorrelationId>> mapSubQueryNodeToCorSet) {
			return new CorelMap(mapRefRelToCorVar, mapCorToCorRel, mapSubQueryNodeToCorSet);
		}

		/**
		 * Returns whether there are any correlating variables in this statement.
		 *
		 * @return whether there are any correlating variables
		 */
		boolean hasCorrelation() {
			return !mapCorToCorRel.isEmpty();
		}
	}

	/**
	 * Frame describing the relational expression after decorrelation
	 * and where to find the output fields and correlation condition.
	 */
	private static class Frame {
		// the new rel
		final RelNode r;
		// the condition contains correlation variables
		final RexNode c;
		// map the oldRel's field indices to newRel's field indices
		final com.google.common.collect.ImmutableSortedMap<Integer, Integer> oldToNewOutputs;

		Frame(RelNode oldRel, RelNode newRel, RexNode corCondition, Map<Integer, Integer> oldToNewOutputs) {
			this.r = Preconditions.checkNotNull(newRel);
			this.c = corCondition;
			this.oldToNewOutputs = com.google.common.collect.ImmutableSortedMap.copyOf(oldToNewOutputs);
			assert allLessThan(this.oldToNewOutputs.keySet(), oldRel.getRowType().getFieldCount(), Litmus.THROW);
			assert allLessThan(this.oldToNewOutputs.values(), r.getRowType().getFieldCount(), Litmus.THROW);
		}

		List<Integer> getCorInputRefIndices() {
			final List<Integer> inputRefIndices;
			if (c != null) {
				inputRefIndices = RelOptUtil.InputFinder.bits(c).toList();
			} else {
				inputRefIndices = new ArrayList<>();
			}
			return inputRefIndices;
		}

		private static boolean allLessThan(Collection<Integer> integers, int limit, Litmus ret) {
			for (int value : integers) {
				if (value >= limit) {
					return ret.fail("out of range; value: {}, limit: {}", value, limit);
				}
			}
			return ret.succeed();
		}
	}

	/**
	 * Result describing the relational expression after decorrelation
	 * and where to find the equivalent non-correlated expressions and correlated conditions.
	 */
	public static class Result {
		private final com.google.common.collect.ImmutableMap<RexSubQuery, Pair<RelNode, RexNode>> subQueryMap;
		static final Result EMPTY = new Result(new HashMap<>());

		private Result(Map<RexSubQuery, Pair<RelNode, RexNode>> subQueryMap) {
			this.subQueryMap = com.google.common.collect.ImmutableMap.copyOf(subQueryMap);
		}

		public Pair<RelNode, RexNode> getSubQueryEquivalent(RexSubQuery subQuery) {
			return subQueryMap.get(subQuery);
		}
	}
}
