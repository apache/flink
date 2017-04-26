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

package org.apache.flink.cep.pattern;

import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.cep.nfa.NFA;
import org.apache.flink.cep.pattern.Quantifier.ConsumingStrategy;
import org.apache.flink.cep.pattern.conditions.AndCondition;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.OrCondition;
import org.apache.flink.cep.pattern.conditions.SubtypeCondition;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Preconditions;

/**
 * Base class for a pattern definition.
 * <p>
 * A pattern definition is used by {@link org.apache.flink.cep.nfa.compiler.NFACompiler} to create a {@link NFA}.
 *
 * <pre>{@code
 * Pattern<T, F> pattern = Pattern.<T>begin("start")
 *   .next("middle").subtype(F.class)
 *   .followedBy("end").where(new MyCondition());
 * }
 * </pre>
 *
 * @param <T> Base type of the elements appearing in the pattern
 * @param <F> Subtype of T to which the current pattern operator is constrained
 */
public class Pattern<T, F extends T> {

	/** Name of the pattern. */
	private final String name;

	/** Previous pattern. */
	private final Pattern<T, ? extends T> previous;

	/** The condition an event has to satisfy to be considered a matched. */
	private IterativeCondition<F> condition;

	/** Window length in which the pattern match has to occur. */
	private Time windowTime;

	/** A quantifier for the pattern. By default set to {@link Quantifier#ONE(ConsumingStrategy)}. */
	private Quantifier quantifier = Quantifier.ONE(ConsumingStrategy.STRICT);

	/**
	 * Applicable to a {@code times} pattern, and holds
	 * the number of times it has to appear.
	 */
	private int times;

	protected Pattern(final String name, final Pattern<T, ? extends T> previous) {
		this.name = name;
		this.previous = previous;
	}

	protected Pattern(
			final String name,
			final Pattern<T, ? extends T> previous,
			final ConsumingStrategy consumingStrategy) {
		this.name = name;
		this.previous = previous;
		this.quantifier = Quantifier.ONE(consumingStrategy);
	}

	public Pattern<T, ? extends T> getPrevious() {
		return previous;
	}

	public int getTimes() {
		return times;
	}

	public String getName() {
		return name;
	}

	public Time getWindowTime() {
		return windowTime;
	}

	public Quantifier getQuantifier() {
		return quantifier;
	}

	public IterativeCondition<F> getCondition() {
		return condition;
	}

	/**
	 * Starts a new pattern sequence. The provided name is the one of the initial pattern
	 * of the new sequence. Furthermore, the base type of the event sequence is set.
	 *
	 * @param name The name of starting pattern of the new pattern sequence
	 * @param <X> Base type of the event pattern
	 * @return The first pattern of a pattern sequence
	 */
	public static <X> Pattern<X, X> begin(final String name) {
		return new Pattern<X, X>(name, null);
	}

	/**
	 * Adds a condition that has to be satisfied by an event
	 * in order to be considered a match. If another condition has already been
	 * set, the new one is going to be combined with the previous with a
	 * logical {@code AND}. In other case, this is going to be the only
	 * condition.
	 *
	 * @param condition The condition as an {@link IterativeCondition}.
	 * @return The pattern with the new condition is set.
	 */
	public Pattern<T, F> where(IterativeCondition<F> condition) {
		ClosureCleaner.clean(condition, true);

		if (this.condition == null) {
			this.condition = condition;
		} else {
			this.condition = new AndCondition<>(this.condition, condition);
		}
		return this;
	}

	/**
	 * Adds a condition that has to be satisfied by an event
	 * in order to be considered a match. If another condition has already been
	 * set, the new one is going to be combined with the previous with a
	 * logical {@code OR}. In other case, this is going to be the only
	 * condition.
	 *
	 * @param condition The condition as an {@link IterativeCondition}.
	 * @return The pattern with the new condition is set.
	 */
	public Pattern<T, F> or(IterativeCondition<F> condition) {
		ClosureCleaner.clean(condition, true);

		if (this.condition == null) {
			this.condition = condition;
		} else {
			this.condition = new OrCondition<>(this.condition, condition);
		}
		return this;
	}

	/**
	 * Applies a subtype constraint on the current pattern. This means that an event has
	 * to be of the given subtype in order to be matched.
	 *
	 * @param subtypeClass Class of the subtype
	 * @param <S> Type of the subtype
	 * @return The same pattern with the new subtype constraint
	 */
	public <S extends F> Pattern<T, S> subtype(final Class<S> subtypeClass) {
		if (condition == null) {
			this.condition = new SubtypeCondition<F>(subtypeClass);
		} else {
			this.condition = new AndCondition<>(condition, new SubtypeCondition<F>(subtypeClass));
		}

		@SuppressWarnings("unchecked")
		Pattern<T, S> result = (Pattern<T, S>) this;

		return result;
	}

	/**
	 * Defines the maximum time interval in which a matching pattern has to be completed in
	 * order to be considered valid. This interval corresponds to the maximum time gap between first
	 * and the last event.
	 *
	 * @param windowTime Time of the matching window
	 * @return The same pattern operator with the new window length
	 */
	public Pattern<T, F> within(Time windowTime) {
		if (windowTime != null) {
			this.windowTime = windowTime;
		}

		return this;
	}

	/**
	 * Appends a new pattern to the existing one. The new pattern enforces strict
	 * temporal contiguity. This means that the whole pattern sequence matches only
	 * if an event which matches this pattern directly follows the preceding matching
	 * event. Thus, there cannot be any events in between two matching events.
	 *
	 * @param name Name of the new pattern
	 * @return A new pattern which is appended to this one
	 */
	public Pattern<T, T> next(final String name) {
		return new Pattern<T, T>(name, this, ConsumingStrategy.STRICT);
	}

	/**
	 * Appends a new pattern to the existing one. The new pattern enforces non-strict
	 * temporal contiguity. This means that a matching event of this pattern and the
	 * preceding matching event might be interleaved with other events which are ignored.
	 *
	 * @param name Name of the new pattern
	 * @return A new pattern which is appended to this one
	 */
	public Pattern<T, T> followedBy(final String name) {
		return new Pattern<>(name, this, ConsumingStrategy.SKIP_TILL_NEXT);
	}

	/**
	 * Appends a new pattern to the existing one. The new pattern enforces non-strict
	 * temporal contiguity. This means that a matching event of this pattern and the
	 * preceding matching event might be interleaved with other events which are ignored.
	 *
	 * @param name Name of the new pattern
	 * @return A new pattern which is appended to this one
	 */
	public Pattern<T, T> followedByAny(final String name) {
		return new Pattern<>(name, this, ConsumingStrategy.SKIP_TILL_ANY);
	}

	/**
	 * Specifies that this pattern is optional for a final match of the pattern
	 * sequence to happen.
	 *
	 * @return The same pattern as optional.
	 * @throws MalformedPatternException if the quantifier is not applicable to this pattern.
	 */
	public Pattern<T, F> optional() {
		quantifier.optional();
		return this;
	}

	/**
	 * Specifies that this pattern can occur {@code one or more} times.
	 * This means at least one and at most infinite number of events can
	 * be matched to this pattern.
	 *
	 * <p>If this quantifier is enabled for a
	 * pattern {@code A.oneOrMore().followedBy(B)} and a sequence of events
	 * {@code A1 A2 B} appears, this will generate patterns:
	 * {@code A1 B} and {@code A1 A2 B}. See also {@link #allowCombinations()}.
	 *
	 * @return The same pattern with a {@link Quantifier#ONE_OR_MORE(ConsumingStrategy)} quantifier applied.
	 * @throws MalformedPatternException if the quantifier is not applicable to this pattern.
	 */
	public Pattern<T, F> oneOrMore() {
		checkIfQuantifierApplied();
		this.quantifier = Quantifier.ONE_OR_MORE(quantifier.getConsumingStrategy());
		return this;
	}

	/**
	 * Specifies exact number of times that this pattern should be matched.
	 *
	 * @param times number of times matching event must appear
	 * @return The same pattern with number of times applied
	 *
	 * @throws MalformedPatternException if the quantifier is not applicable to this pattern.
	 */
	public Pattern<T, F> times(int times) {
		checkIfQuantifierApplied();
		Preconditions.checkArgument(times > 0, "You should give a positive number greater than 0.");
		this.quantifier = Quantifier.TIMES(quantifier.getConsumingStrategy());
		this.times = times;
		return this;
	}

	/**
	 * Applicable only to {@link Quantifier#ONE_OR_MORE(ConsumingStrategy)} and
	 * {@link Quantifier#TIMES(ConsumingStrategy)} patterns, this option allows more flexibility to the matching events.
	 *
	 * <p>If {@code allowCombinations()} is not applied for a
	 * pattern {@code A.oneOrMore().followedBy(B)} and a sequence of events
	 * {@code A1 A2 B} appears, this will generate patterns:
	 * {@code A1 B} and {@code A1 A2 B}. If this method is applied, we
	 * will have {@code A1 B}, {@code A2 B} and {@code A1 A2 B}.
	 *
	 * @return The same pattern with the updated quantifier.	 *
	 * @throws MalformedPatternException if the quantifier is not applicable to this pattern.
	 */
	public Pattern<T, F> allowCombinations() {
		quantifier.combinations();
		return this;
	}

	/**
	 * Works in conjunction with {@link Pattern#oneOrMore()} or {@link Pattern#times(int)}.
	 * Specifies that any not matching element breaks the loop.
	 *
	 * <p>E.g. a pattern like:
	 * <pre>{@code
	 * Pattern.<Event>begin("start").where(new SimpleCondition<Event>() {
	 *      @Override
	 *      public boolean filter(Event value) throws Exception {
	 *          return value.getName().equals("c");
	 *      }
	 * })
	 * .followedBy("middle").where(new SimpleCondition<Event>() {
	 *      @Override
	 *      public boolean filter(Event value) throws Exception {
	 *          return value.getName().equals("a");
	 *      }
	 * }).oneOrMore().consecutive()
	 * .followedBy("end1").where(new SimpleCondition<Event>() {
	 *      @Override
	 *      public boolean filter(Event value) throws Exception {
	 *          return value.getName().equals("b");
	 *      }
	 * });
	 * }</pre>
	 *
	 * <p>for a sequence: C D A1 A2 A3 D A4 B
	 *
	 * <p>will generate matches: {C A1 B}, {C A1 A2 B}, {C A1 A2 A3 B}
	 *
	 * <p>By default a relaxed continuity is applied.
	 *
	 * @return pattern with continuity changed to strict
	 */
	public Pattern<T, F> consecutive() {
		quantifier.consecutive();
		return this;
	}

	private void checkIfQuantifierApplied() {
		if (!quantifier.hasProperty(Quantifier.QuantifierProperty.SINGLE)) {
			throw new MalformedPatternException("Already applied quantifier to this Pattern. " +
					"Current quantifier is: " + quantifier);
		}
	}
}
