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
package org.apache.calcite.sql;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.mapping.Mappings;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

/**
 * This file is copied from Calcite and made the following changes:
 * 1. makeProperRexNodeForOuterJoin function added for CountSplitter and AbstractSumSplitter.
 * 2. If the join type is left or right outer join then make the proper rexNode, or follow the previous logic.
 *
 * This copy can be removed once [CALCITE-2378] is fixed.
 */

/**
 * Aggregate function that can be split into partial aggregates.
 *
 * <p>For example, {@code COUNT(x)} can be split into {@code COUNT(x)} on
 * subsets followed by {@code SUM} to combine those counts.
 */
public interface SqlSplittableAggFunction {
	AggregateCall split(AggregateCall aggregateCall,
			Mappings.TargetMapping mapping);

	/** Called to generate an aggregate for the other side of the join
	 * than the side aggregate call's arguments come from. Returns null if
	 * no aggregate is required. */
	AggregateCall other(RelDataTypeFactory typeFactory, AggregateCall e);

	/** Generates an aggregate call to merge sub-totals.
	 *
	 * <p>Most implementations will add a single aggregate call to
	 * {@code aggCalls}, and return a {@link RexInputRef} that points to it.
	 *
	 * @param rexBuilder Rex builder
	 * @param extra Place to define extra input expressions
	 * @param offset Offset due to grouping columns (and indicator columns if
	 *     applicable)
	 * @param inputRowType Input row type
	 * @param aggregateCall Source aggregate call
	 * @param leftSubTotal Ordinal of the sub-total coming from the left side of
	 *     the join, or -1 if there is no such sub-total
	 * @param rightSubTotal Ordinal of the sub-total coming from the right side
	 *     of the join, or -1 if there is no such sub-total
	 * @param joinRelType the join type
	 *
	 * @return Aggregate call
	 */
	AggregateCall topSplit(RexBuilder rexBuilder, Registry<RexNode> extra,
			int offset, RelDataType inputRowType, AggregateCall aggregateCall,
			int leftSubTotal, int rightSubTotal, JoinRelType joinRelType);

	/** Generates an expression for the value of the aggregate function when
	 * applied to a single row.
	 *
	 * <p>For example, if there is one row:
	 * <ul>
	 *   <li>{@code SUM(x)} is {@code x}
	 *   <li>{@code MIN(x)} is {@code x}
	 *   <li>{@code MAX(x)} is {@code x}
	 *   <li>{@code COUNT(x)} is {@code CASE WHEN x IS NOT NULL THEN 1 ELSE 0 END 1}
	 *   which can be simplified to {@code 1} if {@code x} is never null
	 *   <li>{@code COUNT(*)} is 1
	 * </ul>
	 *
	 * @param rexBuilder Rex builder
	 * @param inputRowType Input row type
	 * @param aggregateCall Aggregate call
	 *
	 * @return Expression for single row
	 */
	RexNode singleton(RexBuilder rexBuilder, RelDataType inputRowType,
			AggregateCall aggregateCall);

	/** Collection in which one can register an element. Registering may return
	 * a reference to an existing element.
	 *
	 * @param <E> element type */
	interface Registry<E> {
		int register(E e);
	}

	/** Splitting strategy for {@code COUNT}.
	 *
	 * <p>COUNT splits into itself followed by SUM. (Actually
	 * SUM0, because the total needs to be 0, not null, if there are 0 rows.)
	 * This rule works for any number of arguments to COUNT, including COUNT(*).
	 */
	class CountSplitter implements SqlSplittableAggFunction {
		public static final CountSplitter INSTANCE = new CountSplitter();

		public AggregateCall split(AggregateCall aggregateCall,
								   Mappings.TargetMapping mapping) {
			return aggregateCall.transform(mapping);
		}

		public AggregateCall other(RelDataTypeFactory typeFactory, AggregateCall e) {
			return AggregateCall.create(SqlStdOperatorTable.COUNT, false, false,
				ImmutableIntList.of(), -1,
				typeFactory.createSqlType(SqlTypeName.BIGINT), null);
		}

		/**
		 * This new function create a proper RexNode for {@coide COUNT} Agg with OuterJoin Condition.
		 */
		private RexNode makeProperRexNodeForOuterJoin(RexBuilder rexBuilder,
													  RelDataType inputRowType,
													  AggregateCall aggregateCall,
													  int index) {
			RexInputRef inputRef = rexBuilder.makeInputRef(inputRowType.getFieldList().get(index).getType(), index);
			RexLiteral literal;
			boolean isCountStar = aggregateCall.getArgList() == null || aggregateCall.getArgList().isEmpty();
			if (isCountStar) {
				literal = rexBuilder.makeExactLiteral(BigDecimal.ONE);
			} else {
				literal = rexBuilder.makeExactLiteral(BigDecimal.ZERO);
			}
			RexNode predicate = rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL, inputRef);
			return rexBuilder.makeCall(SqlStdOperatorTable.CASE,
				predicate,
				literal,
				rexBuilder.makeCast(aggregateCall.type, inputRef)
			);
		}

		public AggregateCall topSplit(RexBuilder rexBuilder,
									  Registry<RexNode> extra, int offset, RelDataType inputRowType,
									  AggregateCall aggregateCall, int leftSubTotal, int rightSubTotal,
									  JoinRelType joinRelType) {
			final List<RexNode> merges = new ArrayList<>();
			if (leftSubTotal >= 0) {
				// add support for right outer join
				if (joinRelType == JoinRelType.RIGHT) {
					merges.add(
						makeProperRexNodeForOuterJoin(rexBuilder, inputRowType, aggregateCall, leftSubTotal)
					);
				} else {
					// if it's a inner join, then do the previous logic
					merges.add(
						rexBuilder.makeInputRef(aggregateCall.type, leftSubTotal));
				}
			}
			if (rightSubTotal >= 0) {
				// add support for left outer join
				if (joinRelType == JoinRelType.LEFT) {
					merges.add(
						makeProperRexNodeForOuterJoin(rexBuilder, inputRowType, aggregateCall, rightSubTotal)
					);
				} else {
					// if it's a inner join, then do the previous logic
					merges.add(
						rexBuilder.makeInputRef(aggregateCall.type, rightSubTotal));
				}
			}
			RexNode node;
			switch (merges.size()) {
				case 1:
					node = merges.get(0);
					break;
				case 2:
					node = rexBuilder.makeCall(SqlStdOperatorTable.MULTIPLY, merges);
					break;
				default:
					throw new AssertionError("unexpected count " + merges);
			}
			int ordinal = extra.register(node);
			return AggregateCall.create(SqlStdOperatorTable.SUM0, false, false,
				ImmutableList.of(ordinal), -1, aggregateCall.type,
				aggregateCall.name);
		}

		/**
		 * {@inheritDoc}
		 *
		 * <p>{@code COUNT(*)}, and {@code COUNT} applied to all NOT NULL arguments,
		 * become {@code 1}; otherwise
		 * {@code CASE WHEN arg0 IS NOT NULL THEN 1 ELSE 0 END}.
		 */
		public RexNode singleton(RexBuilder rexBuilder, RelDataType inputRowType,
								 AggregateCall aggregateCall) {
			final List<RexNode> predicates = new ArrayList<>();
			for (Integer arg : aggregateCall.getArgList()) {
				final RelDataType type = inputRowType.getFieldList().get(arg).getType();
				if (type.isNullable()) {
					predicates.add(
						rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL,
							rexBuilder.makeInputRef(type, arg)));
				}
			}
			final RexNode predicate =
				RexUtil.composeConjunction(rexBuilder, predicates, true);
			if (predicate == null) {
				return rexBuilder.makeExactLiteral(BigDecimal.ONE);
			} else {
				return rexBuilder.makeCall(SqlStdOperatorTable.CASE, predicate,
					rexBuilder.makeExactLiteral(BigDecimal.ONE),
					rexBuilder.makeExactLiteral(BigDecimal.ZERO));
			}
		}
	}

	/** Aggregate function that splits into two applications of itself.
	 *
	 * <p>Examples are MIN and MAX. */
	class SelfSplitter implements SqlSplittableAggFunction {
		public static final SelfSplitter INSTANCE = new SelfSplitter();

		public RexNode singleton(RexBuilder rexBuilder,
								 RelDataType inputRowType, AggregateCall aggregateCall) {
			final int arg = aggregateCall.getArgList().get(0);
			final RelDataTypeField field = inputRowType.getFieldList().get(arg);
			return rexBuilder.makeInputRef(field.getType(), arg);
		}

		public AggregateCall split(AggregateCall aggregateCall,
								   Mappings.TargetMapping mapping) {
			return aggregateCall.transform(mapping);
		}

		public AggregateCall other(RelDataTypeFactory typeFactory, AggregateCall e) {
			return null; // no aggregate function required on other side
		}

		public AggregateCall topSplit(RexBuilder rexBuilder,
									  Registry<RexNode> extra, int offset, RelDataType inputRowType,
									  AggregateCall aggregateCall, int leftSubTotal, int rightSubTotal,
									  JoinRelType joinRelType) {
			assert (leftSubTotal >= 0) != (rightSubTotal >= 0);
			final int arg = leftSubTotal >= 0 ? leftSubTotal : rightSubTotal;
			return aggregateCall.copy(ImmutableIntList.of(arg), -1);
		}
	}

	/** Common Splitting strategy for {@coide SUM} and {@coide SUM0}. */
	abstract class AbstractSumSplitter implements SqlSplittableAggFunction {

		public RexNode singleton(RexBuilder rexBuilder,
								 RelDataType inputRowType, AggregateCall aggregateCall) {
			final int arg = aggregateCall.getArgList().get(0);
			final RelDataTypeField field = inputRowType.getFieldList().get(arg);
			return rexBuilder.makeInputRef(field.getType(), arg);
		}

		public AggregateCall split(AggregateCall aggregateCall,
								   Mappings.TargetMapping mapping) {
			return aggregateCall.transform(mapping);
		}

		public AggregateCall other(RelDataTypeFactory typeFactory, AggregateCall e) {
			return AggregateCall.create(SqlStdOperatorTable.COUNT, false, false,
				ImmutableIntList.of(), -1,
				typeFactory.createSqlType(SqlTypeName.BIGINT), null);
		}

		/**
		 * This new function create a proper RexNode for {@coide SUM} Agg with OuterJoin Condition.
		 */
		private RexNode makeProperRexNodeForOuterJoin(RexBuilder rexBuilder,
													  RelDataType inputRowType,
													  AggregateCall aggregateCall,
													  int index) {
			RexInputRef inputRef = rexBuilder.makeInputRef(inputRowType.getFieldList().get(index).getType(), index);
			RexLiteral literal = rexBuilder.makeExactLiteral(BigDecimal.ZERO);
			RexNode predicate = rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL, inputRef);
			return rexBuilder.makeCall(SqlStdOperatorTable.CASE,
				predicate,
				literal,
				rexBuilder.makeCast(aggregateCall.type, inputRef)
			);
		}

		public AggregateCall topSplit(RexBuilder rexBuilder,
									  Registry<RexNode> extra, int offset, RelDataType inputRowType,
									  AggregateCall aggregateCall, int leftSubTotal, int rightSubTotal,
									  JoinRelType joinRelType) {
			final List<RexNode> merges = new ArrayList<>();
			final List<RelDataTypeField> fieldList = inputRowType.getFieldList();
			if (leftSubTotal >= 0) {
				// add support for left outer join
				if (joinRelType == JoinRelType.RIGHT && getMergeAggFunctionOfTopSplit() == SqlStdOperatorTable.SUM0) {
					merges.add(makeProperRexNodeForOuterJoin(rexBuilder, inputRowType, aggregateCall, leftSubTotal));
				} else {
					// if it's a inner join, then do the previous logic
					final RelDataType type = fieldList.get(leftSubTotal).getType();
					merges.add(rexBuilder.makeInputRef(type, leftSubTotal));
				}
			}
			if (rightSubTotal >= 0) {
				// add support for right outer join
				if (joinRelType == JoinRelType.LEFT && getMergeAggFunctionOfTopSplit() == SqlStdOperatorTable.SUM0) {
					merges.add(makeProperRexNodeForOuterJoin(rexBuilder, inputRowType, aggregateCall, offset + rightSubTotal));
				} else {
					// if it's a inner join, then do the previous logic
					final RelDataType type = fieldList.get(rightSubTotal).getType();
					merges.add(rexBuilder.makeInputRef(type, rightSubTotal));
				}
			}
			RexNode node;
			switch (merges.size()) {
				case 1:
					node = merges.get(0);
					break;
				case 2:
					node = rexBuilder.makeCall(SqlStdOperatorTable.MULTIPLY, merges);
					node = rexBuilder.makeAbstractCast(aggregateCall.type, node);
					break;
				default:
					throw new AssertionError("unexpected count " + merges);
			}
			int ordinal = extra.register(node);
			return AggregateCall.create(getMergeAggFunctionOfTopSplit(), false, false,
				ImmutableList.of(ordinal), -1, aggregateCall.type,
				aggregateCall.name);
		}

		protected abstract SqlAggFunction getMergeAggFunctionOfTopSplit();

	}

	/** Splitting strategy for {@coide SUM}. */
	class SumSplitter extends AbstractSumSplitter {

		public static final SumSplitter INSTANCE = new SumSplitter();

		@Override public SqlAggFunction getMergeAggFunctionOfTopSplit() {
			return SqlStdOperatorTable.SUM;
		}

	}

	/** Splitting strategy for {@code SUM0}. */
	class Sum0Splitter extends AbstractSumSplitter {

		public static final Sum0Splitter INSTANCE = new Sum0Splitter();

		@Override public SqlAggFunction getMergeAggFunctionOfTopSplit() {
			return SqlStdOperatorTable.SUM0;
		}
	}
}

// End SqlSplittableAggFunction.java
