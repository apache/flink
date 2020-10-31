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

package org.apache.calcite.rex;

import com.google.common.collect.Range;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.Sarg;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Utility class to simplify SEARCH rex nodes.
 *
 * <p>This class is a temporary solution for CALCITE-4364 and FLINK-19811.
 * When CALCITE-4364 is fixed, all commits of FLINK-19811 should be reverted.
 */
public class FlinkRexSimplifySearchUtil {

	public static RexNode simplify(RexBuilder builder, RexNode rex) {
		if (rex.isA(SqlKind.AND)) {
			List<RexNode> operands = RelOptUtil.conjunctions(rex).stream()
				.map(operand -> simplify(builder, operand))
				.collect(Collectors.toList());

			SargCollector sargCollector = new SargCollector(builder, true);
			List<RexNode> simplifiedOperands = collectSarg(builder, operands, sargCollector);
			return RexUtil.composeConjunction(builder, simplifiedOperands);
		} else if (rex.isA(SqlKind.OR)) {
			List<RexNode> operands = RelOptUtil.disjunctions(rex).stream()
				.map(operand -> simplify(builder, operand))
				.collect(Collectors.toList());

			SargCollector sargCollector = new SargCollector(builder, false);
			List<RexNode> simplifiedOperands = collectSarg(builder, operands, sargCollector);
			return RexUtil.composeDisjunction(builder, simplifiedOperands);
		} else {
			return rex;
		}
	}

	private static List<RexNode> collectSarg(RexBuilder builder, List<RexNode> operands, SargCollector sargCollector) {
		// the following code are copied and modified
		// from RexSimplify#simplifyAnd and RexSimplify#simplifyOrs
		List<RexNode> terms = new ArrayList<>();
		operands.forEach(t -> sargCollector.accept(t, terms));

		// NOTE: Ideally we should treat each value in the map of sargCollector separately and
		// extract sargs out of searches if the complexity of that search is not more than 1.
		//
		// However if we do so and if there exists an value whose complexity is more than 1,
		// Calcite's RexSimplify#simplifyAnd and RexSimplify#simplifyOrs will change it back
		// (see similar line with the line below in these methods),
		// causing our SimplifyFilterConditionRule to be applied again and again.
		//
		// So we can only do this simplification if none of the complexity exceed 1.
		boolean canSimplify = sargCollector.map.values().stream().noneMatch(b -> b.complexity() > 1);
		return terms.stream()
			.map(t -> {
				RexNode fixed = sargCollector.fix(builder, t);
				if (canSimplify
					&& t instanceof RexSimplify.RexSargBuilder
					&& ((RexSimplify.RexSargBuilder) t).complexity() <= 1) {
					// we only simplify searches if they're not changed to ANDs and ORs,
					// otherwise Calcite's simplify method may change them back and cause
					// infinite loop.
					RexNode expanded = RexUtil.expandSearch(builder, null, fixed);
					if (!expanded.isA(SqlKind.AND) && !expanded.isA(SqlKind.OR)) {
						fixed = expanded;
					}
				}
				return fixed;
			})
			.collect(Collectors.toList());
	}

	/**
	 * This class is directly copied from {@link RexSimplify.SargCollector}
	 * to work around the private access of "accept" method.
	 *
	 * <p>Gathers expressions that can be converted into {@link Sarg search arguments}.
	 */
	static class SargCollector {
		final Map<RexNode, RexSimplify.RexSargBuilder> map = new HashMap<>();
		private final RexBuilder rexBuilder;
		private final boolean negate;

		SargCollector(RexBuilder rexBuilder, boolean negate) {
			this.rexBuilder = rexBuilder;
			this.negate = negate;
		}

		private void accept(RexNode term, List<RexNode> newTerms) {
			if (!accept_(term, newTerms)) {
				newTerms.add(term);
			}
		}

		private boolean accept_(RexNode e, List<RexNode> newTerms) {
			switch (e.getKind()) {
				case LESS_THAN:
				case LESS_THAN_OR_EQUAL:
				case GREATER_THAN:
				case GREATER_THAN_OR_EQUAL:
				case EQUALS:
				case NOT_EQUALS:
				case SEARCH:
					return accept2(((RexCall) e).operands.get(0),
						((RexCall) e).operands.get(1), e.getKind(), newTerms);
				case IS_NULL:
					if (negate) {
						return false;
					}
					final RexNode arg = ((RexCall) e).operands.get(0);
					return accept1(arg, e.getKind(),
						rexBuilder.makeNullLiteral(arg.getType()), newTerms);
				default:
					return false;
			}
		}

		private boolean accept2(RexNode left, RexNode right, SqlKind kind, List<RexNode> newTerms) {
			switch (left.getKind()) {
				case INPUT_REF:
				case FIELD_ACCESS:
					switch (right.getKind()) {
						case LITERAL:
							return accept1(left, kind, (RexLiteral) right, newTerms);
					}
					return false;
				case LITERAL:
					switch (right.getKind()) {
						case INPUT_REF:
						case FIELD_ACCESS:
							return accept1(right, kind.reverse(), (RexLiteral) left, newTerms);
					}
					return false;
			}
			return false;
		}

		private static <E> E addFluent(List<? super E> list, E e) {
			list.add(e);
			return e;
		}

		// always returns true
		private boolean accept1(RexNode e, SqlKind kind, @Nonnull RexLiteral literal, List<RexNode> newTerms) {
			final RexSimplify.RexSargBuilder b =
				map.computeIfAbsent(e, e2 ->
					addFluent(newTerms, new RexSimplify.RexSargBuilder(e2, rexBuilder, negate)));
			if (negate) {
				kind = kind.negateNullSafe();
			}
			final Comparable value = literal.getValueAs(Comparable.class);
			switch (kind) {
				case LESS_THAN:
					b.addRange(Range.lessThan(value), literal.getType());
					return true;
				case LESS_THAN_OR_EQUAL:
					b.addRange(Range.atMost(value), literal.getType());
					return true;
				case GREATER_THAN:
					b.addRange(Range.greaterThan(value), literal.getType());
					return true;
				case GREATER_THAN_OR_EQUAL:
					b.addRange(Range.atLeast(value), literal.getType());
					return true;
				case EQUALS:
					b.addRange(Range.singleton(value), literal.getType());
					return true;
				case NOT_EQUALS:
					b.addRange(Range.lessThan(value), literal.getType());
					b.addRange(Range.greaterThan(value), literal.getType());
					return true;
				case SEARCH:
					final Sarg sarg = literal.getValueAs(Sarg.class);
					b.addSarg(sarg, negate, literal.getType());
					return true;
				case IS_NULL:
					if (negate) {
						throw new AssertionError("negate is not supported for IS_NULL");
					}
					b.containsNull = true;
					return true;
				default:
					throw new AssertionError("unexpected " + kind);
			}
		}

		/** If a term is a call to {@code SEARCH} on a {@link RexSimplify.RexSargBuilder},
		 * converts it to a {@code SEARCH} on a {@link Sarg}. */
		RexNode fix(RexBuilder rexBuilder, RexNode term) {
			if (term instanceof RexSimplify.RexSargBuilder) {
				RexSimplify.RexSargBuilder sargBuilder = (RexSimplify.RexSargBuilder) term;
				return rexBuilder.makeCall(SqlStdOperatorTable.SEARCH, sargBuilder.ref,
					rexBuilder.makeSearchArgumentLiteral(sargBuilder.build(negate),
						term.getType()));
			}
			return term;
		}
	}
}
