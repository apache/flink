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
package org.apache.calcite.rel.rules;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.CompositeList;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/*
 * THIS FILE HAS BEEN COPIED FROM THE APACHE CALCITE PROJECT TO MAKE IT MORE EXTENSIBLE.
 *
 * We have opened an issue to port this change to Apache Calcite (CALCITE-2216).
 * Once CALCITE-2216 is fixed and included in a release, we can remove the copied class.
 *
 * Modification:
 * - Added newCalcRel() method to be able to add fields to the projection.
 */

/**
 * Planner rule that reduces aggregate functions in
 * {@link org.apache.calcite.rel.core.Aggregate}s to simpler forms.
 *
 * <p>Rewrites:
 * <ul>
 *
 * <li>AVG(x) &rarr; SUM(x) / COUNT(x)
 *
 * <li>STDDEV_POP(x) &rarr; SQRT(
 *     (SUM(x * x) - SUM(x) * SUM(x) / COUNT(x))
 *    / COUNT(x))
 *
 * <li>STDDEV_SAMP(x) &rarr; SQRT(
 *     (SUM(x * x) - SUM(x) * SUM(x) / COUNT(x))
 *     / CASE COUNT(x) WHEN 1 THEN NULL ELSE COUNT(x) - 1 END)
 *
 * <li>VAR_POP(x) &rarr; (SUM(x * x) - SUM(x) * SUM(x) / COUNT(x))
 *     / COUNT(x)
 *
 * <li>VAR_SAMP(x) &rarr; (SUM(x * x) - SUM(x) * SUM(x) / COUNT(x))
 *        / CASE COUNT(x) WHEN 1 THEN NULL ELSE COUNT(x) - 1 END
 * </ul>
 *
 * <p>Since many of these rewrites introduce multiple occurrences of simpler
 * forms like {@code COUNT(x)}, the rule gathers common sub-expressions as it
 * goes.
 */
public class AggregateReduceFunctionsRule extends RelOptRule {
	//~ Static fields/initializers ---------------------------------------------

	/** The singleton. */
	public static final AggregateReduceFunctionsRule INSTANCE =
		new AggregateReduceFunctionsRule(operand(LogicalAggregate.class, any()),
			RelFactories.LOGICAL_BUILDER);

	//~ Constructors -----------------------------------------------------------

	/** Creates an AggregateReduceFunctionsRule. */
	public AggregateReduceFunctionsRule(RelOptRuleOperand operand,
										RelBuilderFactory relBuilderFactory) {
		super(operand, relBuilderFactory, null);
	}

	//~ Methods ----------------------------------------------------------------

	@Override public boolean matches(RelOptRuleCall call) {
		if (!super.matches(call)) {
			return false;
		}
		Aggregate oldAggRel = (Aggregate) call.rels[0];
		return containsAvgStddevVarCall(oldAggRel.getAggCallList());
	}

	public void onMatch(RelOptRuleCall ruleCall) {
		Aggregate oldAggRel = (Aggregate) ruleCall.rels[0];
		reduceAggs(ruleCall, oldAggRel);
	}

	/**
	 * Returns whether any of the aggregates are calls to AVG, STDDEV_*, VAR_*.
	 *
	 * @param aggCallList List of aggregate calls
	 */
	private boolean containsAvgStddevVarCall(List<AggregateCall> aggCallList) {
		for (AggregateCall call : aggCallList) {
			if (isReducible(call.getAggregation().getKind())) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Returns whether the aggregate call is a reducible function
	 */
	private boolean isReducible(final SqlKind kind) {
		if (SqlKind.AVG_AGG_FUNCTIONS.contains(kind)) {
			return true;
		}
		switch (kind) {
			case SUM:
				return true;
		}
		return false;
	}

	/**
	 * Reduces all calls to AVG, STDDEV_POP, STDDEV_SAMP, VAR_POP, VAR_SAMP in
	 * the aggregates list to.
	 *
	 * <p>It handles newly generated common subexpressions since this was done
	 * at the sql2rel stage.
	 */
	private void reduceAggs(
		RelOptRuleCall ruleCall,
		Aggregate oldAggRel) {
		RexBuilder rexBuilder = oldAggRel.getCluster().getRexBuilder();

		List<AggregateCall> oldCalls = oldAggRel.getAggCallList();
		final int groupCount = oldAggRel.getGroupCount();
		final int indicatorCount = oldAggRel.getIndicatorCount();

		final List<AggregateCall> newCalls = Lists.newArrayList();
		final Map<AggregateCall, RexNode> aggCallMapping = Maps.newHashMap();

		final List<RexNode> projList = Lists.newArrayList();

		// pass through group key (+ indicators if present)
		for (int i = 0; i < groupCount + indicatorCount; ++i) {
			projList.add(
				rexBuilder.makeInputRef(
					getFieldType(oldAggRel, i),
					i));
		}

		// List of input expressions. If a particular aggregate needs more, it
		// will add an expression to the end, and we will create an extra
		// project.
		final RelBuilder relBuilder = ruleCall.builder();
		relBuilder.push(oldAggRel.getInput());
		final List<RexNode> inputExprs = new ArrayList<>(relBuilder.fields());

		// create new agg function calls and rest of project list together
		for (AggregateCall oldCall : oldCalls) {
			projList.add(
				reduceAgg(
					oldAggRel, oldCall, newCalls, aggCallMapping, inputExprs));
		}

		final int extraArgCount =
			inputExprs.size() - relBuilder.peek().getRowType().getFieldCount();
		if (extraArgCount > 0) {
			relBuilder.project(inputExprs,
				CompositeList.of(
					relBuilder.peek().getRowType().getFieldNames(),
					Collections.<String>nCopies(extraArgCount, null)));
		}
		newAggregateRel(relBuilder, oldAggRel, newCalls);
		newCalcRel(relBuilder, oldAggRel, projList);
		ruleCall.transformTo(relBuilder.build());
	}

	private RexNode reduceAgg(
		Aggregate oldAggRel,
		AggregateCall oldCall,
		List<AggregateCall> newCalls,
		Map<AggregateCall, RexNode> aggCallMapping,
		List<RexNode> inputExprs) {
		final SqlKind kind = oldCall.getAggregation().getKind();
		if (isReducible(kind)) {
			switch (kind) {
				case SUM:
					// replace original SUM(x) with
					// case COUNT(x) when 0 then null else SUM0(x) end
					return reduceSum(oldAggRel, oldCall, newCalls, aggCallMapping);
				case AVG:
					// replace original AVG(x) with SUM(x) / COUNT(x)
					return reduceAvg(oldAggRel, oldCall, newCalls, aggCallMapping, inputExprs);
				case STDDEV_POP:
					// replace original STDDEV_POP(x) with
					//   SQRT(
					//     (SUM(x * x) - SUM(x) * SUM(x) / COUNT(x))
					//     / COUNT(x))
					return reduceStddev(oldAggRel, oldCall, true, true, newCalls,
						aggCallMapping, inputExprs);
				case STDDEV_SAMP:
					// replace original STDDEV_POP(x) with
					//   SQRT(
					//     (SUM(x * x) - SUM(x) * SUM(x) / COUNT(x))
					//     / CASE COUNT(x) WHEN 1 THEN NULL ELSE COUNT(x) - 1 END)
					return reduceStddev(oldAggRel, oldCall, false, true, newCalls,
						aggCallMapping, inputExprs);
				case VAR_POP:
					// replace original VAR_POP(x) with
					//     (SUM(x * x) - SUM(x) * SUM(x) / COUNT(x))
					//     / COUNT(x)
					return reduceStddev(oldAggRel, oldCall, true, false, newCalls,
						aggCallMapping, inputExprs);
				case VAR_SAMP:
					// replace original VAR_POP(x) with
					//     (SUM(x * x) - SUM(x) * SUM(x) / COUNT(x))
					//     / CASE COUNT(x) WHEN 1 THEN NULL ELSE COUNT(x) - 1 END
					return reduceStddev(oldAggRel, oldCall, false, false, newCalls,
						aggCallMapping, inputExprs);
				default:
					throw Util.unexpected(kind);
			}
		} else {
			// anything else:  preserve original call
			RexBuilder rexBuilder = oldAggRel.getCluster().getRexBuilder();
			final int nGroups = oldAggRel.getGroupCount();
			List<RelDataType> oldArgTypes =
				SqlTypeUtil.projectTypes(
					oldAggRel.getInput().getRowType(), oldCall.getArgList());
			return rexBuilder.addAggCall(oldCall,
				nGroups,
				oldAggRel.indicator,
				newCalls,
				aggCallMapping,
				oldArgTypes);
		}
	}

	private AggregateCall createAggregateCallWithBinding(
		RelDataTypeFactory typeFactory,
		SqlAggFunction aggFunction,
		RelDataType operandType,
		Aggregate oldAggRel,
		AggregateCall oldCall,
		int argOrdinal) {
		final Aggregate.AggCallBinding binding =
			new Aggregate.AggCallBinding(typeFactory, aggFunction,
				ImmutableList.of(operandType), oldAggRel.getGroupCount(),
				oldCall.filterArg >= 0);
		return AggregateCall.create(aggFunction,
			oldCall.isDistinct(),
			oldCall.isApproximate(),
			ImmutableIntList.of(argOrdinal),
			oldCall.filterArg,
			aggFunction.inferReturnType(binding),
			null);
	}

	private RexNode reduceAvg(
		Aggregate oldAggRel,
		AggregateCall oldCall,
		List<AggregateCall> newCalls,
		Map<AggregateCall, RexNode> aggCallMapping,
		List<RexNode> inputExprs) {
		final int nGroups = oldAggRel.getGroupCount();
		final RexBuilder rexBuilder = oldAggRel.getCluster().getRexBuilder();
		final int iAvgInput = oldCall.getArgList().get(0);
		final RelDataType avgInputType =
			getFieldType(
				oldAggRel.getInput(),
				iAvgInput);
		final AggregateCall sumCall =
			AggregateCall.create(SqlStdOperatorTable.SUM,
				oldCall.isDistinct(),
				oldCall.isApproximate(),
				oldCall.getArgList(),
				oldCall.filterArg,
				oldAggRel.getGroupCount(),
				oldAggRel.getInput(),
				null,
				null);
		final AggregateCall countCall =
			AggregateCall.create(SqlStdOperatorTable.COUNT,
				oldCall.isDistinct(),
				oldCall.isApproximate(),
				oldCall.getArgList(),
				oldCall.filterArg,
				oldAggRel.getGroupCount(),
				oldAggRel.getInput(),
				null,
				null);

		// NOTE:  these references are with respect to the output
		// of newAggRel
		RexNode numeratorRef =
			rexBuilder.addAggCall(sumCall,
				nGroups,
				oldAggRel.indicator,
				newCalls,
				aggCallMapping,
				ImmutableList.of(avgInputType));
		final RexNode denominatorRef =
			rexBuilder.addAggCall(countCall,
				nGroups,
				oldAggRel.indicator,
				newCalls,
				aggCallMapping,
				ImmutableList.of(avgInputType));

		final RelDataTypeFactory typeFactory = oldAggRel.getCluster().getTypeFactory();
		final RelDataType avgType = typeFactory.createTypeWithNullability(
			oldCall.getType(), numeratorRef.getType().isNullable());
		numeratorRef = rexBuilder.ensureType(avgType, numeratorRef, true);
		final RexNode divideRef =
			rexBuilder.makeCall(SqlStdOperatorTable.DIVIDE, numeratorRef, denominatorRef);
		return rexBuilder.makeCast(oldCall.getType(), divideRef);
	}

	private RexNode reduceSum(
		Aggregate oldAggRel,
		AggregateCall oldCall,
		List<AggregateCall> newCalls,
		Map<AggregateCall, RexNode> aggCallMapping) {
		final int nGroups = oldAggRel.getGroupCount();
		RexBuilder rexBuilder = oldAggRel.getCluster().getRexBuilder();
		int arg = oldCall.getArgList().get(0);
		RelDataType argType =
			getFieldType(
				oldAggRel.getInput(),
				arg);
		final AggregateCall sumZeroCall =
			AggregateCall.create(SqlStdOperatorTable.SUM0, oldCall.isDistinct(),
				oldCall.isApproximate(), oldCall.getArgList(), oldCall.filterArg,
				oldAggRel.getGroupCount(), oldAggRel.getInput(), null,
				oldCall.name);
		final AggregateCall countCall =
			AggregateCall.create(SqlStdOperatorTable.COUNT,
				oldCall.isDistinct(),
				oldCall.isApproximate(),
				oldCall.getArgList(),
				oldCall.filterArg,
				oldAggRel.getGroupCount(),
				oldAggRel,
				null,
				null);

		// NOTE:  these references are with respect to the output
		// of newAggRel
		RexNode sumZeroRef =
			rexBuilder.addAggCall(sumZeroCall,
				nGroups,
				oldAggRel.indicator,
				newCalls,
				aggCallMapping,
				ImmutableList.of(argType));
		if (!oldCall.getType().isNullable()) {
			// If SUM(x) is not nullable, the validator must have determined that
			// nulls are impossible (because the group is never empty and x is never
			// null). Therefore we translate to SUM0(x).
			return sumZeroRef;
		}
		RexNode countRef =
			rexBuilder.addAggCall(countCall,
				nGroups,
				oldAggRel.indicator,
				newCalls,
				aggCallMapping,
				ImmutableList.of(argType));
		return rexBuilder.makeCall(SqlStdOperatorTable.CASE,
			rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
				countRef, rexBuilder.makeExactLiteral(BigDecimal.ZERO)),
			rexBuilder.makeCast(sumZeroRef.getType(), rexBuilder.constantNull()),
			sumZeroRef);
	}

	private RexNode reduceStddev(
		Aggregate oldAggRel,
		AggregateCall oldCall,
		boolean biased,
		boolean sqrt,
		List<AggregateCall> newCalls,
		Map<AggregateCall, RexNode> aggCallMapping,
		List<RexNode> inputExprs) {
		// stddev_pop(x) ==>
		//   power(
		//     (sum(x * x) - sum(x) * sum(x) / count(x))
		//     / count(x),
		//     .5)
		//
		// stddev_samp(x) ==>
		//   power(
		//     (sum(x * x) - sum(x) * sum(x) / count(x))
		//     / nullif(count(x) - 1, 0),
		//     .5)
		final int nGroups = oldAggRel.getGroupCount();
		final RelOptCluster cluster = oldAggRel.getCluster();
		final RexBuilder rexBuilder = cluster.getRexBuilder();
		final RelDataTypeFactory typeFactory = cluster.getTypeFactory();

		assert oldCall.getArgList().size() == 1 : oldCall.getArgList();
		final int argOrdinal = oldCall.getArgList().get(0);
		final RelDataType argOrdinalType = getFieldType(oldAggRel.getInput(), argOrdinal);
		final RelDataType oldCallType =
			typeFactory.createTypeWithNullability(oldCall.getType(),
				argOrdinalType.isNullable());

		final RexNode argRef =
			rexBuilder.ensureType(oldCallType, inputExprs.get(argOrdinal), true);
		final int argRefOrdinal = lookupOrAdd(inputExprs, argRef);

		final RexNode argSquared = rexBuilder.makeCall(SqlStdOperatorTable.MULTIPLY,
			argRef, argRef);
		final int argSquaredOrdinal = lookupOrAdd(inputExprs, argSquared);

		final AggregateCall sumArgSquaredAggCall =
			createAggregateCallWithBinding(typeFactory, SqlStdOperatorTable.SUM,
				argSquared.getType(), oldAggRel, oldCall, argSquaredOrdinal);

		final RexNode sumArgSquared =
			rexBuilder.addAggCall(sumArgSquaredAggCall,
				nGroups,
				oldAggRel.indicator,
				newCalls,
				aggCallMapping,
				ImmutableList.of(sumArgSquaredAggCall.getType()));

		final AggregateCall sumArgAggCall =
			AggregateCall.create(SqlStdOperatorTable.SUM,
				oldCall.isDistinct(),
				oldCall.isApproximate(),
				ImmutableIntList.of(argOrdinal),
				oldCall.filterArg,
				oldAggRel.getGroupCount(),
				oldAggRel.getInput(),
				null,
				null);

		final RexNode sumArg =
			rexBuilder.addAggCall(sumArgAggCall,
				nGroups,
				oldAggRel.indicator,
				newCalls,
				aggCallMapping,
				ImmutableList.of(sumArgAggCall.getType()));
		final RexNode sumArgCast = rexBuilder.ensureType(oldCallType, sumArg, true);
		final RexNode sumSquaredArg =
			rexBuilder.makeCall(
				SqlStdOperatorTable.MULTIPLY, sumArgCast, sumArgCast);

		final AggregateCall countArgAggCall =
			AggregateCall.create(SqlStdOperatorTable.COUNT,
				oldCall.isDistinct(),
				oldCall.isApproximate(),
				oldCall.getArgList(),
				oldCall.filterArg,
				oldAggRel.getGroupCount(),
				oldAggRel,
				null,
				null);

		final RexNode countArg =
			rexBuilder.addAggCall(countArgAggCall,
				nGroups,
				oldAggRel.indicator,
				newCalls,
				aggCallMapping,
				ImmutableList.of(argOrdinalType));

		final RexNode avgSumSquaredArg =
			rexBuilder.makeCall(
				SqlStdOperatorTable.DIVIDE, sumSquaredArg, countArg);

		final RexNode diff =
			rexBuilder.makeCall(
				SqlStdOperatorTable.MINUS,
				sumArgSquared, avgSumSquaredArg);

		final RexNode denominator;
		if (biased) {
			denominator = countArg;
		} else {
			final RexLiteral one =
				rexBuilder.makeExactLiteral(BigDecimal.ONE);
			final RexNode nul =
				rexBuilder.makeCast(countArg.getType(), rexBuilder.constantNull());
			final RexNode countMinusOne =
				rexBuilder.makeCall(
					SqlStdOperatorTable.MINUS, countArg, one);
			final RexNode countEqOne =
				rexBuilder.makeCall(
					SqlStdOperatorTable.EQUALS, countArg, one);
			denominator =
				rexBuilder.makeCall(
					SqlStdOperatorTable.CASE,
					countEqOne, nul, countMinusOne);
		}

		final RexNode div =
			rexBuilder.makeCall(
				SqlStdOperatorTable.DIVIDE, diff, denominator);

		RexNode result = div;
		if (sqrt) {
			final RexNode half =
				rexBuilder.makeExactLiteral(new BigDecimal("0.5"));
			result =
				rexBuilder.makeCall(
					SqlStdOperatorTable.POWER, div, half);
		}

		return rexBuilder.makeCast(
			oldCall.getType(), result);
	}

	/**
	 * Finds the ordinal of an element in a list, or adds it.
	 *
	 * @param list    List
	 * @param element Element to lookup or add
	 * @param <T>     Element type
	 * @return Ordinal of element in list
	 */
	private static <T> int lookupOrAdd(List<T> list, T element) {
		int ordinal = list.indexOf(element);
		if (ordinal == -1) {
			ordinal = list.size();
			list.add(element);
		}
		return ordinal;
	}

	/**
	 * Do a shallow clone of oldAggRel and update aggCalls. Could be refactored
	 * into Aggregate and subclasses - but it's only needed for some
	 * subclasses.
	 *
	 * @param relBuilder Builder of relational expressions; at the top of its
	 *                   stack is its input
	 * @param oldAggregate LogicalAggregate to clone.
	 * @param newCalls  New list of AggregateCalls
	 */
	protected void newAggregateRel(RelBuilder relBuilder,
								   Aggregate oldAggregate,
								   List<AggregateCall> newCalls) {
		relBuilder.aggregate(
			relBuilder.groupKey(oldAggregate.getGroupSet(),
				oldAggregate.getGroupSets()),
			newCalls);
	}

	/**
	 * Add a calc with the expressions to compute the original agg calls from the
	 * decomposed ones.
	 *
	 * @param relBuilder Builder of relational expressions; at the top of its
	 *                   stack is its input
	 * @param oldAggregate The original LogicalAggregate that is replaced.
	 * @param exprs The expressions to compute the original agg calls.
	 */
	protected void newCalcRel(RelBuilder relBuilder,
							  Aggregate oldAggregate,
							  List<RexNode> exprs) {
		relBuilder.project(exprs, oldAggregate.getRowType().getFieldNames());
	}

	private RelDataType getFieldType(RelNode relNode, int i) {
		final RelDataTypeField inputField =
			relNode.getRowType().getFieldList().get(i);
		return inputField.getType();
	}
}

// End AggregateReduceFunctionsRule.java
