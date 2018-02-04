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
package org.apache.calcite.rex;

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.Strong;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;

import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.BoundType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.collect.Range;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/*
 * THIS FILE HAS BEEN COPIED FROM THE APACHE CALCITE PROJECT UNTIL CALCITE-2110 IS FIXED.
 */

/**
 * Context required to simplify a row-expression.
 */
public class RexSimplify {
	public final RexBuilder rexBuilder;
	private final RelOptPredicateList predicates;
	final boolean unknownAsFalse;
	private final RexExecutor executor;

	/**
	 * Creates a RexSimplify.
	 *
	 * @param rexBuilder Rex builder
	 * @param predicates Predicates known to hold on input fields
	 * @param unknownAsFalse Whether to convert UNKNOWN values to FALSE
	 * @param executor Executor for constant reduction, not null
	 */
	public RexSimplify(RexBuilder rexBuilder, RelOptPredicateList predicates,
	                   boolean unknownAsFalse, RexExecutor executor) {
		this.rexBuilder = Preconditions.checkNotNull(rexBuilder);
		this.predicates = Preconditions.checkNotNull(predicates);
		this.unknownAsFalse = unknownAsFalse;
		this.executor = Preconditions.checkNotNull(executor);
	}

	@Deprecated // to be removed before 2.0
	public RexSimplify(RexBuilder rexBuilder, boolean unknownAsFalse,
	                   RexExecutor executor) {
		this(rexBuilder, RelOptPredicateList.EMPTY, unknownAsFalse, executor);
	}

	//~ Methods ----------------------------------------------------------------

	/** Returns a RexSimplify the same as this but with a specified
	 * {@link #unknownAsFalse} value. */
	public RexSimplify withUnknownAsFalse(boolean unknownAsFalse) {
		return unknownAsFalse == this.unknownAsFalse
				? this
				: new RexSimplify(rexBuilder, predicates, unknownAsFalse, executor);
	}

	/** Returns a RexSimplify the same as this but with a specified
	 * {@link #predicates} value. */
	public RexSimplify withPredicates(RelOptPredicateList predicates) {
		return predicates == this.predicates
				? this
				: new RexSimplify(rexBuilder, predicates, unknownAsFalse, executor);
	}

	/** Simplifies a boolean expression, always preserving its type and its
	 * nullability.
	 *
	 * <p>This is useful if you are simplifying expressions in a
	 * {@link Project}. */
	public RexNode simplifyPreservingType(RexNode e) {
		final RexNode e2 = simplify(e);
		if (e2.getType() == e.getType()) {
			return e2;
		}
		final RexNode e3 = rexBuilder.makeCast(e.getType(), e2, true);
		if (e3.equals(e)) {
			return e;
		}
		return e3;
	}

	/**
	 * Simplifies a boolean expression.
	 *
	 * <p>In particular:</p>
	 * <ul>
	 * <li>{@code simplify(x = 1 AND y = 2 AND NOT x = 1)}
	 * returns {@code y = 2}</li>
	 * <li>{@code simplify(x = 1 AND FALSE)}
	 * returns {@code FALSE}</li>
	 * </ul>
	 *
	 * <p>If the expression is a predicate in a WHERE clause, UNKNOWN values have
	 * the same effect as FALSE. In situations like this, specify
	 * {@code unknownAsFalse = true}, so and we can switch from 3-valued logic to
	 * simpler 2-valued logic and make more optimizations.
	 *
	 * @param e Expression to simplify
	 */
	public RexNode simplify(RexNode e) {
		switch (e.getKind()) {
			case AND:
				return simplifyAnd((RexCall) e);
			case OR:
				return simplifyOr((RexCall) e);
			case NOT:
				return simplifyNot((RexCall) e);
			case CASE:
				return simplifyCase((RexCall) e);
			case CAST:
				return simplifyCast((RexCall) e);
			case IS_NULL:
			case IS_NOT_NULL:
			case IS_TRUE:
			case IS_NOT_TRUE:
			case IS_FALSE:
			case IS_NOT_FALSE:
				assert e instanceof RexCall;
				return simplifyIs((RexCall) e);
			case EQUALS:
			case GREATER_THAN:
			case GREATER_THAN_OR_EQUAL:
			case LESS_THAN:
			case LESS_THAN_OR_EQUAL:
			case NOT_EQUALS:
				return simplifyComparison((RexCall) e);
			default:
				return e;
		}
	}

	// e must be a comparison (=, >, >=, <, <=, !=)
	private RexNode simplifyComparison(RexCall e) {
		//noinspection unchecked
		return simplifyComparison(e, Comparable.class);
	}

	// e must be a comparison (=, >, >=, <, <=, !=)
	private <C extends Comparable<C>> RexNode simplifyComparison(RexCall e,
	                                                             Class<C> clazz) {
		final List<RexNode> operands = new ArrayList<>(e.operands);
		simplifyList(operands);

		// Simplify "x <op> x"
		final RexNode o0 = operands.get(0);
		final RexNode o1 = operands.get(1);
		if (RexUtil.eq(o0, o1)
				&& (unknownAsFalse
				|| (!o0.getType().isNullable()
				&& !o1.getType().isNullable()))) {
			switch (e.getKind()) {
				case EQUALS:
				case GREATER_THAN_OR_EQUAL:
				case LESS_THAN_OR_EQUAL:
					// "x = x" simplifies to "x is not null" (similarly <= and >=)
					return simplify(
							rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, o0));
				default:
					// "x != x" simplifies to "false" (similarly < and >)
					return rexBuilder.makeLiteral(false);
			}
		}

		// Simplify "<literal1> <op> <literal2>"
		// For example, "1 = 2" becomes FALSE;
		// "1 != 1" becomes FALSE;
		// "1 != NULL" becomes UNKNOWN (or FALSE if unknownAsFalse);
		// "1 != '1'" is unchanged because the types are not the same.
		if (o0.isA(SqlKind.LITERAL)
				&& o1.isA(SqlKind.LITERAL)
				&& o0.getType().equals(o1.getType())) {
			final C v0 = ((RexLiteral) o0).getValueAs(clazz);
			final C v1 = ((RexLiteral) o1).getValueAs(clazz);
			if (v0 == null || v1 == null) {
				return unknownAsFalse
						? rexBuilder.makeLiteral(false)
						: rexBuilder.makeNullLiteral(e.getType());
			}
			final int comparisonResult = v0.compareTo(v1);
			switch (e.getKind()) {
				case EQUALS:
					return rexBuilder.makeLiteral(comparisonResult == 0);
				case GREATER_THAN:
					return rexBuilder.makeLiteral(comparisonResult > 0);
				case GREATER_THAN_OR_EQUAL:
					return rexBuilder.makeLiteral(comparisonResult >= 0);
				case LESS_THAN:
					return rexBuilder.makeLiteral(comparisonResult < 0);
				case LESS_THAN_OR_EQUAL:
					return rexBuilder.makeLiteral(comparisonResult <= 0);
				case NOT_EQUALS:
					return rexBuilder.makeLiteral(comparisonResult != 0);
				default:
					throw new AssertionError();
			}
		}

		// If none of the arguments were simplified, return the call unchanged.
		final RexNode e2;
		if (operands.equals(e.operands)) {
			e2 = e;
		} else {
			e2 = rexBuilder.makeCall(e.op, operands);
		}
		return simplifyUsingPredicates(e2, clazz);
	}

	/**
	 * Simplifies a conjunction of boolean expressions.
	 */
	public RexNode simplifyAnds(Iterable<? extends RexNode> nodes) {
		final List<RexNode> terms = new ArrayList<>();
		final List<RexNode> notTerms = new ArrayList<>();
		for (RexNode e : nodes) {
			RelOptUtil.decomposeConjunction(e, terms, notTerms);
		}
		simplifyList(terms);
		simplifyList(notTerms);
		if (unknownAsFalse) {
			return simplifyAnd2ForUnknownAsFalse(terms, notTerms);
		}
		return simplifyAnd2(terms, notTerms);
	}

	private void simplifyList(List<RexNode> terms) {
		for (int i = 0; i < terms.size(); i++) {
			terms.set(i, withUnknownAsFalse(false).simplify(terms.get(i)));
		}
	}

	private RexNode simplifyNot(RexCall call) {
		final RexNode a = call.getOperands().get(0);
		switch (a.getKind()) {
			case NOT:
				// NOT NOT x ==> x
				return simplify(((RexCall) a).getOperands().get(0));
		}
		final SqlKind negateKind = a.getKind().negate();
		if (a.getKind() != negateKind) {
			return simplify(
					rexBuilder.makeCall(RexUtil.op(negateKind),
							ImmutableList.of(((RexCall) a).getOperands().get(0))));
		}
		final SqlKind negateKind2 = a.getKind().negateNullSafe();
		if (a.getKind() != negateKind2) {
			return simplify(
					rexBuilder.makeCall(RexUtil.op(negateKind2),
							((RexCall) a).getOperands()));
		}
		if (a.getKind() == SqlKind.AND) {
			// NOT distributivity for AND
			final List<RexNode> newOperands = new ArrayList<>();
			for (RexNode operand : ((RexCall) a).getOperands()) {
				newOperands.add(
						simplify(rexBuilder.makeCall(SqlStdOperatorTable.NOT, operand)));
			}
			return simplify(rexBuilder.makeCall(SqlStdOperatorTable.OR, newOperands));
		}
		if (a.getKind() == SqlKind.OR) {
			// NOT distributivity for OR
			final List<RexNode> newOperands = new ArrayList<>();
			for (RexNode operand : ((RexCall) a).getOperands()) {
				newOperands.add(
						simplify(rexBuilder.makeCall(SqlStdOperatorTable.NOT, operand)));
			}
			return simplify(
					rexBuilder.makeCall(SqlStdOperatorTable.AND, newOperands));
		}
		return call;
	}

	private RexNode simplifyIs(RexCall call) {
		final SqlKind kind = call.getKind();
		final RexNode a = call.getOperands().get(0);
		final RexNode simplified = simplifyIs2(kind, a);
		if (simplified != null) {
			return simplified;
		}
		return call;
	}

	private RexNode simplifyIs2(SqlKind kind, RexNode a) {
		switch (kind) {
			case IS_NULL:
				// x IS NULL ==> FALSE (if x is not nullable)
				if (!a.getType().isNullable()) {
					return rexBuilder.makeLiteral(false);
				}
				break;
			case IS_NOT_NULL:
				// x IS NOT NULL ==> TRUE (if x is not nullable)
				RexNode simplified = simplifyIsNotNull(a);
				if (simplified != null) {
					return simplified;
				}
				break;
			case IS_TRUE:
			case IS_NOT_FALSE:
				// x IS TRUE ==> x (if x is not nullable)
				// x IS NOT FALSE ==> x (if x is not nullable)
				if (!a.getType().isNullable()) {
					return simplify(a);
				}
				break;
			case IS_FALSE:
			case IS_NOT_TRUE:
				// x IS NOT TRUE ==> NOT x (if x is not nullable)
				// x IS FALSE ==> NOT x (if x is not nullable)
				if (!a.getType().isNullable()) {
					return simplify(rexBuilder.makeCall(SqlStdOperatorTable.NOT, a));
				}
				break;
		}
		switch (a.getKind()) {
			case NOT:
				// (NOT x) IS TRUE ==> x IS FALSE
				// Similarly for IS NOT TRUE, IS FALSE, etc.
				//
				// Note that
				//   (NOT x) IS TRUE !=> x IS FALSE
				// because of null values.
				final SqlOperator notKind = RexUtil.op(kind.negateNullSafe());
				final RexNode arg = ((RexCall) a).operands.get(0);
				return simplify(rexBuilder.makeCall(notKind, arg));
		}
		RexNode a2 = simplify(a);
		if (a != a2) {
			return rexBuilder.makeCall(RexUtil.op(kind), ImmutableList.of(a2));
		}
		return null; // cannot be simplified
	}

	private RexNode simplifyIsNotNull(RexNode a) {
		if (!a.getType().isNullable()) {
			return rexBuilder.makeLiteral(true);
		}
		switch (Strong.policy(a.getKind())) {
			case ANY:
				final List<RexNode> operands = new ArrayList<>();
				for (RexNode operand : ((RexCall) a).getOperands()) {
					final RexNode simplified = simplifyIsNotNull(operand);
					if (simplified == null) {
						operands.add(
								rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, operand));
					} else if (simplified.isAlwaysFalse()) {
						return rexBuilder.makeLiteral(false);
					} else {
						operands.add(simplified);
					}
				}
				return RexUtil.composeConjunction(rexBuilder, operands, false);
			case CUSTOM:
				switch (a.getKind()) {
					case LITERAL:
						return rexBuilder.makeLiteral(!((RexLiteral) a).isNull());
					default:
						throw new AssertionError("every CUSTOM policy needs a handler, "
								+ a.getKind());
				}
			case AS_IS:
			default:
				return null;
		}
	}

	private RexNode simplifyCase(RexCall call) {
		final List<RexNode> operands = call.getOperands();
		final List<RexNode> newOperands = new ArrayList<>();
		final Set<String> values = new HashSet<>();
		for (int i = 0; i < operands.size(); i++) {
			RexNode operand = operands.get(i);
			if (RexUtil.isCasePredicate(call, i)) {
				if (operand.isAlwaysTrue()) {
					// Predicate is always TRUE. Make value the ELSE and quit.
					newOperands.add(operands.get(++i));
					if (unknownAsFalse && RexUtil.isNull(operands.get(i))) {
						values.add(rexBuilder.makeLiteral(false).toString());
					} else {
						values.add(operands.get(i).toString());
					}
					break;
				} else if (operand.isAlwaysFalse() || RexUtil.isNull(operand)) {
					// Predicate is always FALSE or NULL. Skip predicate and value.
					++i;
					continue;
				}
			} else {
				if (unknownAsFalse && RexUtil.isNull(operand)) {
					values.add(rexBuilder.makeLiteral(false).toString());
				} else {
					values.add(operand.toString());
				}
			}
			newOperands.add(operand);
		}
		assert newOperands.size() % 2 == 1;
		if (newOperands.size() == 1 || values.size() == 1) {
			final RexNode last = Util.last(newOperands);
			if (!call.getType().equals(last.getType())) {
				return rexBuilder.makeAbstractCast(call.getType(), last);
			}
			return last;
		}
		trueFalse:
		if (call.getType().getSqlTypeName() == SqlTypeName.BOOLEAN) {
			// Optimize CASE where every branch returns constant true or constant
			// false.
			final List<Pair<RexNode, RexNode>> pairs =
					casePairs(rexBuilder, newOperands);
			// 1) Possible simplification if unknown is treated as false:
			//   CASE
			//   WHEN p1 THEN TRUE
			//   WHEN p2 THEN TRUE
			//   ELSE FALSE
			//   END
			// can be rewritten to: (p1 or p2)
			if (unknownAsFalse) {
				final List<RexNode> terms = new ArrayList<>();
				int pos = 0;
				for (; pos < pairs.size(); pos++) {
					// True block
					Pair<RexNode, RexNode> pair = pairs.get(pos);
					if (!pair.getValue().isAlwaysTrue()) {
						break;
					}
					terms.add(pair.getKey());
				}
				for (; pos < pairs.size(); pos++) {
					// False block
					Pair<RexNode, RexNode> pair = pairs.get(pos);
					if (!pair.getValue().isAlwaysFalse()
							&& !RexUtil.isNull(pair.getValue())) {
						break;
					}
				}
				if (pos == pairs.size()) {
					final RexNode disjunction =
							RexUtil.composeDisjunction(rexBuilder, terms);
					if (!call.getType().equals(disjunction.getType())) {
						return rexBuilder.makeCast(call.getType(), disjunction);
					}
					return disjunction;
				}
			}
			// 2) Another simplification
			//   CASE
			//   WHEN p1 THEN TRUE
			//   WHEN p2 THEN FALSE
			//   WHEN p3 THEN TRUE
			//   ELSE FALSE
			//   END
			// if p1...pn cannot be nullable
			for (Ord<Pair<RexNode, RexNode>> pair : Ord.zip(pairs)) {
				if (pair.e.getKey().getType().isNullable()) {
					break trueFalse;
				}
				if (!pair.e.getValue().isAlwaysTrue()
						&& !pair.e.getValue().isAlwaysFalse()
						&& (!unknownAsFalse || !RexUtil.isNull(pair.e.getValue()))) {
					break trueFalse;
				}
			}
			final List<RexNode> terms = new ArrayList<>();
			final List<RexNode> notTerms = new ArrayList<>();
			for (Ord<Pair<RexNode, RexNode>> pair : Ord.zip(pairs)) {
				if (pair.e.getValue().isAlwaysTrue()) {
					terms.add(RexUtil.andNot(rexBuilder, pair.e.getKey(), notTerms));
				} else {
					notTerms.add(pair.e.getKey());
				}
			}
			final RexNode disjunction = RexUtil.composeDisjunction(rexBuilder, terms);
			if (!call.getType().equals(disjunction.getType())) {
				return rexBuilder.makeCast(call.getType(), disjunction);
			}
			return disjunction;
		}
		if (newOperands.equals(operands)) {
			return call;
		}
		return call.clone(call.getType(), newOperands);
	}

	/** Given "CASE WHEN p1 THEN v1 ... ELSE e END"
	 * returns [(p1, v1), ..., (true, e)]. */
	private static List<Pair<RexNode, RexNode>> casePairs(RexBuilder rexBuilder,
	                                                      List<RexNode> operands) {
		final ImmutableList.Builder<Pair<RexNode, RexNode>> builder =
				ImmutableList.builder();
		for (int i = 0; i < operands.size() - 1; i += 2) {
			builder.add(Pair.of(operands.get(i), operands.get(i + 1)));
		}
		builder.add(
				Pair.of((RexNode) rexBuilder.makeLiteral(true), Util.last(operands)));
		return builder.build();
	}

	public RexNode simplifyAnd(RexCall e) {
		final List<RexNode> terms = new ArrayList<>();
		final List<RexNode> notTerms = new ArrayList<>();
		RelOptUtil.decomposeConjunction(e, terms, notTerms);
		simplifyList(terms);
		simplifyList(notTerms);
		if (unknownAsFalse) {
			return simplifyAnd2ForUnknownAsFalse(terms, notTerms);
		}
		return simplifyAnd2(terms, notTerms);
	}

	RexNode simplifyAnd2(List<RexNode> terms, List<RexNode> notTerms) {
		for (RexNode term : terms) {
			if (term.isAlwaysFalse()) {
				return rexBuilder.makeLiteral(false);
			}
		}
		if (terms.isEmpty() && notTerms.isEmpty()) {
			return rexBuilder.makeLiteral(true);
		}
		// If one of the not-disjunctions is a disjunction that is wholly
		// contained in the disjunctions list, the expression is not
		// satisfiable.
		//
		// Example #1. x AND y AND z AND NOT (x AND y)  - not satisfiable
		// Example #2. x AND y AND NOT (x AND y)        - not satisfiable
		// Example #3. x AND y AND NOT (x AND y AND z)  - may be satisfiable
		for (RexNode notDisjunction : notTerms) {
			final List<RexNode> terms2 = RelOptUtil.conjunctions(notDisjunction);
			if (terms.containsAll(terms2)) {
				return rexBuilder.makeLiteral(false);
			}
		}
		// Add the NOT disjunctions back in.
		for (RexNode notDisjunction : notTerms) {
			terms.add(
					simplify(
							rexBuilder.makeCall(SqlStdOperatorTable.NOT, notDisjunction)));
		}
		return RexUtil.composeConjunction(rexBuilder, terms, false);
	}

	/** As {@link #simplifyAnd2(List, List)} but we assume that if the expression
	 * returns UNKNOWN it will be interpreted as FALSE. */
	RexNode simplifyAnd2ForUnknownAsFalse(List<RexNode> terms,
	                                      List<RexNode> notTerms) {
		//noinspection unchecked
		return simplifyAnd2ForUnknownAsFalse(terms, notTerms, Comparable.class);
	}

	private <C extends Comparable<C>> RexNode simplifyAnd2ForUnknownAsFalse(
			List<RexNode> terms, List<RexNode> notTerms, Class<C> clazz) {
		for (RexNode term : terms) {
			if (term.isAlwaysFalse()) {
				return rexBuilder.makeLiteral(false);
			}
		}
		if (terms.isEmpty() && notTerms.isEmpty()) {
			return rexBuilder.makeLiteral(true);
		}
		if (terms.size() == 1 && notTerms.isEmpty()) {
			// Make sure "x OR y OR x" (a single-term conjunction) gets simplified.
			return simplify(terms.get(0));
		}
		// Try to simplify the expression
		final Multimap<String, Pair<String, RexNode>> equalityTerms = ArrayListMultimap.create();
		final Map<String, Pair<Range<C>, List<RexNode>>> rangeTerms =
				new HashMap<>();
		final Map<String, String> equalityConstantTerms = new HashMap<>();
		final Set<String> negatedTerms = new HashSet<>();
		final Set<String> nullOperands = new HashSet<>();
		final Set<RexNode> notNullOperands = new LinkedHashSet<>();
		final Set<String> comparedOperands = new HashSet<>();

		// Add the predicates from the source to the range terms.
		for (RexNode predicate : predicates.pulledUpPredicates) {
			final Comparison comparison = Comparison.of(predicate);
			if (comparison != null
					&& comparison.kind != SqlKind.NOT_EQUALS) { // not supported yet
				final C v0 = comparison.literal.getValueAs(clazz);
				if (v0 != null) {
					final RexNode result = processRange(rexBuilder, terms, rangeTerms,
							predicate, comparison.ref, v0, comparison.kind);
					if (result != null) {
						// Not satisfiable
						return result;
					}
				}
			}
		}

		for (int i = 0; i < terms.size(); i++) {
			RexNode term = terms.get(i);
			if (!RexUtil.isDeterministic(term)) {
				continue;
			}
			// Simplify BOOLEAN expressions if possible
			while (term.getKind() == SqlKind.EQUALS) {
				RexCall call = (RexCall) term;
				if (call.getOperands().get(0).isAlwaysTrue()) {
					term = call.getOperands().get(1);
					terms.set(i, term);
					continue;
				} else if (call.getOperands().get(1).isAlwaysTrue()) {
					term = call.getOperands().get(0);
					terms.set(i, term);
					continue;
				}
				break;
			}
			switch (term.getKind()) {
				case EQUALS:
				case NOT_EQUALS:
				case LESS_THAN:
				case GREATER_THAN:
				case LESS_THAN_OR_EQUAL:
				case GREATER_THAN_OR_EQUAL:
					RexCall call = (RexCall) term;
					RexNode left = call.getOperands().get(0);
					comparedOperands.add(left.toString());
					// if it is a cast, we include the inner reference
					if (left.getKind() == SqlKind.CAST) {
						RexCall leftCast = (RexCall) left;
						comparedOperands.add(leftCast.getOperands().get(0).toString());
					}
					RexNode right = call.getOperands().get(1);
					comparedOperands.add(right.toString());
					// if it is a cast, we include the inner reference
					if (right.getKind() == SqlKind.CAST) {
						RexCall rightCast = (RexCall) right;
						comparedOperands.add(rightCast.getOperands().get(0).toString());
					}
					final Comparison comparison = Comparison.of(term);
					// Check for comparison with null values
					if (comparison != null
							&& comparison.literal.getValue() == null) {
						return rexBuilder.makeLiteral(false);
					}
					// Check for equality on different constants. If the same ref or CAST(ref)
					// is equal to different constants, this condition cannot be satisfied,
					// and hence it can be evaluated to FALSE
					if (term.getKind() == SqlKind.EQUALS) {
						if (comparison != null) {
							final String literal = comparison.literal.toString();
							final String prevLiteral =
									equalityConstantTerms.put(comparison.ref.toString(), literal);
							if (prevLiteral != null && !literal.equals(prevLiteral)) {
								return rexBuilder.makeLiteral(false);
							}
						} else if (RexUtil.isReferenceOrAccess(left, true)
								&& RexUtil.isReferenceOrAccess(right, true)) {
							equalityTerms.put(left.toString(), Pair.of(right.toString(), term));
						}
					}
					// Assume the expression a > 5 is part of a Filter condition.
					// Then we can derive the negated term: a <= 5.
					// But as the comparison is string based and thus operands order dependent,
					// we should also add the inverted negated term: 5 >= a.
					// Observe that for creating the inverted term we invert the list of operands.
					RexNode negatedTerm = RexUtil.negate(rexBuilder, call);
					if (negatedTerm != null) {
						negatedTerms.add(negatedTerm.toString());
						RexNode invertNegatedTerm = RexUtil.invert(rexBuilder, (RexCall) negatedTerm);
						if (invertNegatedTerm != null) {
							negatedTerms.add(invertNegatedTerm.toString());
						}
					}
					// Remove terms that are implied by predicates on the input,
					// or weaken terms that are partially implied.
					// E.g. given predicate "x >= 5" and term "x between 3 and 10"
					// we weaken to term to "x between 5 and 10".
					final RexNode term2 = simplifyUsingPredicates(term, clazz);
					if (term2 != term) {
						terms.set(i, term = term2);
					}
					// Range
					if (comparison != null
							&& comparison.kind != SqlKind.NOT_EQUALS) { // not supported yet
						final C constant = comparison.literal.getValueAs(clazz);
						final RexNode result = processRange(rexBuilder, terms, rangeTerms,
								term, comparison.ref, constant, comparison.kind);
						if (result != null) {
							// Not satisfiable
							return result;
						}
					}
					break;
				case IN:
					comparedOperands.add(((RexCall) term).operands.get(0).toString());
					break;
				case BETWEEN:
					comparedOperands.add(((RexCall) term).operands.get(1).toString());
					break;
				case IS_NOT_NULL:
					notNullOperands.add(((RexCall) term).getOperands().get(0));
					terms.remove(i);
					--i;
					break;
				case IS_NULL:
					nullOperands.add(((RexCall) term).getOperands().get(0).toString());
			}
		}
		// If one column should be null and is in a comparison predicate,
		// it is not satisfiable.
		// Example. IS NULL(x) AND x < 5  - not satisfiable
		if (!Collections.disjoint(nullOperands, comparedOperands)) {
			return rexBuilder.makeLiteral(false);
		}
		// Check for equality of two refs wrt equality with constants
		// Example #1. x=5 AND y=5 AND x=y : x=5 AND y=5
		// Example #2. x=5 AND y=6 AND x=y - not satisfiable
		for (String ref1 : equalityTerms.keySet()) {
			final String literal1 = equalityConstantTerms.get(ref1);
			if (literal1 == null) {
				continue;
			}
			Collection<Pair<String, RexNode>> references = equalityTerms.get(ref1);
			for (Pair<String, RexNode> ref2 : references) {
				final String literal2 = equalityConstantTerms.get(ref2.left);
				if (literal2 == null) {
					continue;
				}
				if (!literal1.equals(literal2)) {
					// If an expression is equal to two different constants,
					// it is not satisfiable
					return rexBuilder.makeLiteral(false);
				}
				// Otherwise we can remove the term, as we already know that
				// the expression is equal to two constants
				terms.remove(ref2.right);
			}
		}
		// Remove not necessary IS NOT NULL expressions.
		//
		// Example. IS NOT NULL(x) AND x < 5  : x < 5
		for (RexNode operand : notNullOperands) {
			if (!comparedOperands.contains(operand.toString())) {
				terms.add(
						rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, operand));
			}
		}
		// If one of the not-disjunctions is a disjunction that is wholly
		// contained in the disjunctions list, the expression is not
		// satisfiable.
		//
		// Example #1. x AND y AND z AND NOT (x AND y)  - not satisfiable
		// Example #2. x AND y AND NOT (x AND y)        - not satisfiable
		// Example #3. x AND y AND NOT (x AND y AND z)  - may be satisfiable
		final Set<String> termsSet = new HashSet<>(RexUtil.strings(terms));
		for (RexNode notDisjunction : notTerms) {
			if (!RexUtil.isDeterministic(notDisjunction)) {
				continue;
			}
			final List<String> terms2Set = RexUtil.strings(RelOptUtil.conjunctions(notDisjunction));
			if (termsSet.containsAll(terms2Set)) {
				return rexBuilder.makeLiteral(false);
			}
		}
		// Add the NOT disjunctions back in.
		for (RexNode notDisjunction : notTerms) {
			final RexNode call =
					rexBuilder.makeCall(SqlStdOperatorTable.NOT, notDisjunction);
			terms.add(simplify(call));
		}
		// The negated terms: only deterministic expressions
		for (String negatedTerm : negatedTerms) {
			if (termsSet.contains(negatedTerm)) {
				return rexBuilder.makeLiteral(false);
			}
		}
		return RexUtil.composeConjunction(rexBuilder, terms, false);
	}

	private <C extends Comparable<C>> RexNode simplifyUsingPredicates(RexNode e,
	                                                                  Class<C> clazz) {
		final Comparison comparison = Comparison.of(e);
		// Check for comparison with null values
		if (comparison == null
				|| comparison.kind == SqlKind.NOT_EQUALS
				|| comparison.literal.getValue() == null) {
			return e;
		}
		final C v0 = comparison.literal.getValueAs(clazz);
		final Range<C> range = range(comparison.kind, v0);
		final Range<C> range2 =
				residue(comparison.ref, range, predicates.pulledUpPredicates,
						clazz);
		if (range2 == null) {
			// Term is impossible to satisfy given these predicates
			return rexBuilder.makeLiteral(false);
		} else if (range2.equals(range)) {
			// no change
			return e;
		} else if (range2.equals(Range.all())) {
			// Term is always satisfied given these predicates
			return rexBuilder.makeLiteral(true);
		} else if (range2.lowerEndpoint().equals(range2.upperEndpoint())) {
			if (range2.lowerBoundType() == BoundType.OPEN
					|| range2.upperBoundType() == BoundType.OPEN) {
				// range is a point, but does not include its endpoint, therefore is
				// effectively empty
				return rexBuilder.makeLiteral(false);
			}
			// range is now a point; it's worth simplifying
			return rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, comparison.ref,
					rexBuilder.makeLiteral(range2.lowerEndpoint(),
							comparison.literal.getType(), comparison.literal.getTypeName()));
		} else {
			// range has been reduced but it's not worth simplifying
			return e;
		}
	}

	/** Weakens a term so that it checks only what is not implied by predicates.
	 *
	 * <p>The term is broken into "ref comparison constant",
	 * for example "$0 &lt; 5".
	 *
	 * <p>Examples:
	 * <ul>
	 *
	 * <li>{@code residue($0 < 10, [$0 < 5])} returns {@code true}
	 *
	 * <li>{@code residue($0 < 10, [$0 < 20, $0 > 0])} returns {@code $0 < 10}
	 * </ul>
	 */
	private <C extends Comparable<C>> Range<C> residue(RexNode ref, Range<C> r0,
	                                                   List<RexNode> predicates, Class<C> clazz) {
		for (RexNode predicate : predicates) {
			switch (predicate.getKind()) {
				case EQUALS:
				case LESS_THAN:
				case LESS_THAN_OR_EQUAL:
				case GREATER_THAN:
				case GREATER_THAN_OR_EQUAL:
					final RexCall call = (RexCall) predicate;
					if (call.operands.get(0).equals(ref)
							&& call.operands.get(1) instanceof RexLiteral) {
						final RexLiteral literal = (RexLiteral) call.operands.get(1);
						final C c1 = literal.getValueAs(clazz);
						final Range<C> r1 = range(predicate.getKind(), c1);
						if (r0.encloses(r1)) {
							// Given these predicates, term is always satisfied.
							// e.g. r0 is "$0 < 10", r1 is "$0 < 5"
							return Range.all();
						}
						if (r0.isConnected(r1)) {
							return r0.intersection(r1);
						}
						// Ranges do not intersect. Return null meaning the empty range.
						return null;
					}
			}
		}
		return r0;
	}

	/** Simplifies OR(x, x) into x, and similar. */
	public RexNode simplifyOr(RexCall call) {
		assert call.getKind() == SqlKind.OR;
		final List<RexNode> terms = RelOptUtil.disjunctions(call);
		return simplifyOrs(terms);
	}

	/** Simplifies a list of terms and combines them into an OR.
	 * Modifies the list in place. */
	public RexNode simplifyOrs(List<RexNode> terms) {
		for (int i = 0; i < terms.size(); i++) {
			final RexNode term = simplify(terms.get(i));
			switch (term.getKind()) {
				case LITERAL:
					if (!RexLiteral.isNullLiteral(term)) {
						if (RexLiteral.booleanValue(term)) {
							return term; // true
						} else {
							terms.remove(i);
							--i;
							continue;
						}
					}
			}
			terms.set(i, term);
		}
		return RexUtil.composeDisjunction(rexBuilder, terms);
	}

	private RexNode simplifyCast(RexCall e) {
		final RexNode operand = e.getOperands().get(0);
		switch (operand.getKind()) {
			case LITERAL:
				final RexLiteral literal = (RexLiteral) operand;
				final Comparable value = literal.getValueAs(Comparable.class);
				final SqlTypeName typeName = literal.getTypeName();

				// First, try to remove the cast without changing the value.
				// makeCast and canRemoveCastFromLiteral have the same logic, so we are
				// sure to be able to remove the cast.
				if (rexBuilder.canRemoveCastFromLiteral(e.getType(), value, typeName)) {
					return rexBuilder.makeCast(e.getType(), operand);
				}

				// Next, try to convert the value to a different type,
				// e.g. CAST('123' as integer)
				switch (literal.getTypeName()) {
					case TIME:
						switch (e.getType().getSqlTypeName()) {
							case TIMESTAMP:
								return e;
						}
						break;
				}
				final List<RexNode> reducedValues = new ArrayList<>();
				executor.reduce(rexBuilder, ImmutableList.<RexNode>of(e), reducedValues);
				return Preconditions.checkNotNull(
						Iterables.getOnlyElement(reducedValues));
			default:
				return e;
		}
	}

	/** Removes any casts that change nullability but not type.
	 *
	 * <p>For example, {@code CAST(1 = 0 AS BOOLEAN)} becomes {@code 1 = 0}. */
	public RexNode removeNullabilityCast(RexNode e) {
		while (RexUtil.isNullabilityCast(rexBuilder.getTypeFactory(), e)) {
			e = ((RexCall) e).operands.get(0);
		}
		return e;
	}

	private static <C extends Comparable<C>> RexNode processRange(
			RexBuilder rexBuilder, List<RexNode> terms,
			Map<String, Pair<Range<C>, List<RexNode>>> rangeTerms, RexNode term,
			RexNode ref, C v0, SqlKind comparison) {
		Pair<Range<C>, List<RexNode>> p = rangeTerms.get(ref.toString());
		if (p == null) {
			rangeTerms.put(ref.toString(),
					Pair.of(range(comparison, v0),
							(List<RexNode>) ImmutableList.of(term)));
		} else {
			// Exists
			boolean removeUpperBound = false;
			boolean removeLowerBound = false;
			Range<C> r = p.left;
			switch (comparison) {
				case EQUALS:
					if (!r.contains(v0)) {
						// Range is empty, not satisfiable
						return rexBuilder.makeLiteral(false);
					}
					rangeTerms.put(ref.toString(),
							Pair.of(Range.singleton(v0),
									(List<RexNode>) ImmutableList.of(term)));
					// remove
					for (RexNode e : p.right) {
						Collections.replaceAll(terms, e, rexBuilder.makeLiteral(true));
					}
					break;
				case LESS_THAN: {
					int comparisonResult = 0;
					if (r.hasUpperBound()) {
						comparisonResult = v0.compareTo(r.upperEndpoint());
					}
					if (comparisonResult <= 0) {
						// 1) No upper bound, or
						// 2) We need to open the upper bound, or
						// 3) New upper bound is lower than old upper bound
						if (r.hasLowerBound()) {
							if (v0.compareTo(r.lowerEndpoint()) <= 0) {
								// Range is empty, not satisfiable
								return rexBuilder.makeLiteral(false);
							}
							// a <= x < b OR a < x < b
							r = Range.range(r.lowerEndpoint(), r.lowerBoundType(),
									v0, BoundType.OPEN);
						} else {
							// x < b
							r = Range.lessThan(v0);
						}

						if (r.isEmpty()) {
							// Range is empty, not satisfiable
							return rexBuilder.makeLiteral(false);
						}

						// remove prev upper bound
						removeUpperBound = true;
					} else {
						int termIndex = terms.indexOf(term);
						if (termIndex >= 0) {
							// Remove this term as it is contained in current upper bound
							terms.set(termIndex, rexBuilder.makeLiteral(true));
						}
					}
					break;
				}
				case LESS_THAN_OR_EQUAL: {
					int comparisonResult = -1;
					if (r.hasUpperBound()) {
						comparisonResult = v0.compareTo(r.upperEndpoint());
					}
					if (comparisonResult < 0) {
						// 1) No upper bound, or
						// 2) New upper bound is lower than old upper bound
						if (r.hasLowerBound()) {
							if (v0.compareTo(r.lowerEndpoint()) < 0) {
								// Range is empty, not satisfiable
								return rexBuilder.makeLiteral(false);
							}
							// a <= x <= b OR a < x <= b
							r = Range.range(r.lowerEndpoint(), r.lowerBoundType(),
									v0, BoundType.CLOSED);
						} else {
							// x <= b
							r = Range.atMost(v0);
						}

						if (r.isEmpty()) {
							// Range is empty, not satisfiable
							return rexBuilder.makeLiteral(false);
						}

						// remove prev upper bound
						removeUpperBound = true;
					} else {
						int termIndex = terms.indexOf(term);
						if (termIndex >= 0) {
							// Remove this term as it is contained in current upper bound
							terms.set(termIndex, rexBuilder.makeLiteral(true));
						}
					}
					break;
				}
				case GREATER_THAN: {
					int comparisonResult = 0;
					if (r.hasLowerBound()) {
						comparisonResult = v0.compareTo(r.lowerEndpoint());
					}
					if (comparisonResult >= 0) {
						// 1) No lower bound, or
						// 2) We need to open the lower bound, or
						// 3) New lower bound is greater than old lower bound
						if (r.hasUpperBound()) {
							if (v0.compareTo(r.upperEndpoint()) >= 0) {
								// Range is empty, not satisfiable
								return rexBuilder.makeLiteral(false);
							}
							// a < x <= b OR a < x < b
							r = Range.range(v0, BoundType.OPEN,
									r.upperEndpoint(), r.upperBoundType());
						} else {
							// x > a
							r = Range.greaterThan(v0);
						}

						if (r.isEmpty()) {
							// Range is empty, not satisfiable
							return rexBuilder.makeLiteral(false);
						}

						// remove prev lower bound
						removeLowerBound = true;
					} else {
						int termIndex = terms.indexOf(term);
						if (termIndex >= 0) {
							// Remove this term as it is contained in current lower bound
							terms.set(termIndex, rexBuilder.makeLiteral(true));
						}
					}
					break;
				}
				case GREATER_THAN_OR_EQUAL: {
					int comparisonResult = 1;
					if (r.hasLowerBound()) {
						comparisonResult = v0.compareTo(r.lowerEndpoint());
					}
					if (comparisonResult > 0) {
						// 1) No lower bound, or
						// 2) New lower bound is greater than old lower bound
						if (r.hasUpperBound()) {
							if (v0.compareTo(r.upperEndpoint()) > 0) {
								// Range is empty, not satisfiable
								return rexBuilder.makeLiteral(false);
							}
							// a <= x <= b OR a <= x < b
							r = Range.range(v0, BoundType.CLOSED,
									r.upperEndpoint(), r.upperBoundType());
						} else {
							// x >= a
							r = Range.atLeast(v0);
						}

						if (r.isEmpty()) {
							// Range is empty, not satisfiable
							return rexBuilder.makeLiteral(false);
						}

						// remove prev lower bound
						removeLowerBound = true;
					} else {
						int termIndex = terms.indexOf(term);
						if (termIndex >= 0) {
							// Remove this term as it is contained in current lower bound
							terms.set(termIndex, rexBuilder.makeLiteral(true));
						}
					}
					break;
				}
				default:
					throw new AssertionError();
			}
			if (removeUpperBound) {
				ImmutableList.Builder<RexNode> newBounds = ImmutableList.builder();
				for (RexNode e : p.right) {
					if (isUpperBound(e)) {
						Collections.replaceAll(terms, e, rexBuilder.makeLiteral(true));
					} else {
						newBounds.add(e);
					}
				}
				newBounds.add(term);
				rangeTerms.put(ref.toString(),
						Pair.of(r, (List<RexNode>) newBounds.build()));
			} else if (removeLowerBound) {
				ImmutableList.Builder<RexNode> newBounds = ImmutableList.builder();
				for (RexNode e : p.right) {
					if (isLowerBound(e)) {
						Collections.replaceAll(terms, e, rexBuilder.makeLiteral(true));
					} else {
						newBounds.add(e);
					}
				}
				newBounds.add(term);
				rangeTerms.put(ref.toString(),
						Pair.of(r, (List<RexNode>) newBounds.build()));
			}
		}
		// Default
		return null;
	}

	private static <C extends Comparable<C>> Range<C> range(SqlKind comparison,
	                                                        C c) {
		switch (comparison) {
			case EQUALS:
				return Range.singleton(c);
			case LESS_THAN:
				return Range.lessThan(c);
			case LESS_THAN_OR_EQUAL:
				return Range.atMost(c);
			case GREATER_THAN:
				return Range.greaterThan(c);
			case GREATER_THAN_OR_EQUAL:
				return Range.atLeast(c);
			default:
				throw new AssertionError();
		}
	}

	/** Comparison between a {@link RexInputRef} or {@link RexFieldAccess} and a
	 * literal. Literal may be on left or right side, and may be null. */
	private static class Comparison {
		final RexNode ref;
		final SqlKind kind;
		final RexLiteral literal;

		private Comparison(RexNode ref, SqlKind kind, RexLiteral literal) {
			this.ref = Preconditions.checkNotNull(ref);
			this.kind = Preconditions.checkNotNull(kind);
			this.literal = Preconditions.checkNotNull(literal);
		}

		/** Creates a comparison, or returns null. */
		static Comparison of(RexNode e) {
			switch (e.getKind()) {
				case EQUALS:
				case NOT_EQUALS:
				case LESS_THAN:
				case GREATER_THAN:
				case LESS_THAN_OR_EQUAL:
				case GREATER_THAN_OR_EQUAL:
					final RexCall call = (RexCall) e;
					final RexNode left = call.getOperands().get(0);
					final RexNode right = call.getOperands().get(1);
					switch (right.getKind()) {
						case LITERAL:
							if (RexUtil.isReferenceOrAccess(left, true)) {
								return new Comparison(left, e.getKind(), (RexLiteral) right);
							}
					}
					switch (left.getKind()) {
						case LITERAL:
							if (RexUtil.isReferenceOrAccess(right, true)) {
								return new Comparison(right, e.getKind().reverse(),
										(RexLiteral) left);
							}
					}
			}
			return null;
		}
	}

	private static boolean isUpperBound(final RexNode e) {
		final List<RexNode> operands;
		switch (e.getKind()) {
			case LESS_THAN:
			case LESS_THAN_OR_EQUAL:
				operands = ((RexCall) e).getOperands();
				return RexUtil.isReferenceOrAccess(operands.get(0), true)
						&& operands.get(1).isA(SqlKind.LITERAL);
			case GREATER_THAN:
			case GREATER_THAN_OR_EQUAL:
				operands = ((RexCall) e).getOperands();
				return RexUtil.isReferenceOrAccess(operands.get(1), true)
						&& operands.get(0).isA(SqlKind.LITERAL);
			default:
				return false;
		}
	}

	private static boolean isLowerBound(final RexNode e) {
		final List<RexNode> operands;
		switch (e.getKind()) {
			case LESS_THAN:
			case LESS_THAN_OR_EQUAL:
				operands = ((RexCall) e).getOperands();
				return RexUtil.isReferenceOrAccess(operands.get(1), true)
						&& operands.get(0).isA(SqlKind.LITERAL);
			case GREATER_THAN:
			case GREATER_THAN_OR_EQUAL:
				operands = ((RexCall) e).getOperands();
				return RexUtil.isReferenceOrAccess(operands.get(0), true)
						&& operands.get(1).isA(SqlKind.LITERAL);
			default:
				return false;
		}
	}
}

// End RexSimplify.java
