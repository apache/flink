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

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.cep.nfa.NFA;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Preconditions;

/**
 * Base class for a pattern definition.
 * <p>
 * A pattern definition is used by {@link org.apache.flink.cep.nfa.compiler.NFACompiler} to create
 * a {@link NFA}.
 *
 * <pre>{@code
 * Pattern<T, F> pattern = Pattern.<T>begin("start")
 *   .next("middle").subtype(F.class)
 *   .followedBy("end").where(new MyFilterFunction());
 * }
 * </pre>
 *
 * @param <T> Base type of the elements appearing in the pattern
 * @param <F> Subtype of T to which the current pattern operator is constrained
 */
public class Pattern<T, F extends T> {

	// name of the pattern operator
	private final String name;

	// previous pattern operator
	private final Pattern<T, ? extends T> previous;

	// filter condition for an event to be matched
	private FilterFunction<F> filterFunction;

	// window length in which the pattern match has to occur
	private Time windowTime;

	private Quantifier quantifier = Quantifier.ONE;

	private int times;

	protected Pattern(final String name, final Pattern<T, ? extends T> previous) {
		this.name = name;
		this.previous = previous;
	}

	public String getName() {
		return name;
	}

	public Pattern<T, ? extends T> getPrevious() {
		return previous;
	}

	public FilterFunction<F> getFilterFunction() {
		return filterFunction;
	}

	public Time getWindowTime() {
		return windowTime;
	}

	public Quantifier getQuantifier() {
		return quantifier;
	}

	public int getTimes() {
		return times;
	}

	/**
	 * Specifies a filter condition which has to be fulfilled by an event in order to be matched.
	 *
	 * @param newFilterFunction Filter condition
	 * @return The same pattern operator where the new filter condition is set
	 */
	public Pattern<T, F> where(FilterFunction<F> newFilterFunction) {
		ClosureCleaner.clean(newFilterFunction, true);

		if (this.filterFunction == null) {
			this.filterFunction = newFilterFunction;
		} else {
			this.filterFunction = new AndFilterFunction<F>(this.filterFunction, newFilterFunction);
		}

		return this;
	}

	/**
	 * Specifies a filter condition which is OR'ed with an existing filter function.
	 *
	 * @param orFilterFunction OR filter condition
	 * @return The same pattern operator where the new filter condition is set
	 */
	public Pattern<T, F> or(FilterFunction<F> orFilterFunction) {
		ClosureCleaner.clean(orFilterFunction, true);

		if (this.filterFunction == null) {
			this.filterFunction = orFilterFunction;
		} else {
			this.filterFunction = new OrFilterFunction<>(this.filterFunction, orFilterFunction);
		}

		return this;
	}

	/**
	 * Applies a subtype constraint on the current pattern operator. This means that an event has
	 * to be of the given subtype in order to be matched.
	 *
	 * @param subtypeClass Class of the subtype
	 * @param <S> Type of the subtype
	 * @return The same pattern operator with the new subtype constraint
	 */
	public <S extends F> Pattern<T, S> subtype(final Class<S> subtypeClass) {
		if (filterFunction == null) {
			this.filterFunction = new SubtypeFilterFunction<F>(subtypeClass);
		} else {
			this.filterFunction = new AndFilterFunction<F>(this.filterFunction, new SubtypeFilterFunction<F>(subtypeClass));
		}

		@SuppressWarnings("unchecked")
		Pattern<T, S> result = (Pattern<T, S>) this;

		return result;
	}

	/**
	 * Defines the maximum time interval for a matching pattern. This means that the time gap
	 * between first and the last event must not be longer than the window time.
	 *
	 * @param windowTime Time of the matching window
	 * @return The same pattenr operator with the new window length
	 */
	public Pattern<T, F> within(Time windowTime) {
		if (windowTime != null) {
			this.windowTime = windowTime;
		}

		return this;
	}

	/**
	 * Appends a new pattern operator to the existing one. The new pattern operator enforces strict
	 * temporal contiguity. This means that the whole pattern only matches if an event which matches
	 * this operator directly follows the preceding matching event. Thus, there cannot be any
	 * events in between two matching events.
	 *
	 * @param name Name of the new pattern operator
	 * @return A new pattern operator which is appended to this pattern operator
	 */
	public Pattern<T, T> next(final String name) {
		return new Pattern<T, T>(name, this);
	}

	/**
	 * Appends a new pattern operator to the existing one. The new pattern operator enforces
	 * non-strict temporal contiguity. This means that a matching event of this operator and the
	 * preceding matching event might be interleaved with other events which are ignored.
	 *
	 * @param name Name of the new pattern operator
	 * @return A new pattern operator which is appended to this pattern operator
	 */
	public FollowedByPattern<T, T> followedBy(final String name) {
		return new FollowedByPattern<T, T>(name, this);
	}

	/**
	 * Starts a new pattern with the initial pattern operator whose name is provided. Furthermore,
	 * the base type of the event sequence is set.
	 *
	 * @param name Name of the new pattern operator
	 * @param <X> Base type of the event pattern
	 * @return The first pattern operator of a pattern
	 */
	public static <X> Pattern<X, X> begin(final String name) {
		return new Pattern<X, X>(name, null);
	}

	/**
	 * Specifies that this pattern can occur zero or more times(kleene star).
	 * This means any number of events can be matched in this state.
	 *
	 * @return The same pattern with applied Kleene star operator
	 *
	 * @throws MalformedPatternException if quantifier already applied
	 */
	public Pattern<T, F> zeroOrMore() {
		return zeroOrMore(true);
	}

	/**
	 * Specifies that this pattern can occur zero or more times(kleene star).
	 * This means any number of events can be matched in this state.
	 *
	 * If eagerness is enabled for a pattern A*B and sequence A1 A2 B will generate patterns:
	 * B, A1 B and A1 A2 B. If disabled B, A1 B, A2 B and A1 A2 B.
	 *
	 * @param eager if true the pattern always consumes earlier events
	 * @return The same pattern with applied Kleene star operator
	 *
	 * @throws MalformedPatternException if quantifier already applied
	 */
	public Pattern<T, F> zeroOrMore(final boolean eager) {
		checkIfQuantifierApplied();
		if (eager) {
			this.quantifier = Quantifier.ZERO_OR_MORE_EAGER;
		} else {
			this.quantifier = Quantifier.ZERO_OR_MORE_COMBINATIONS;
		}
		return this;
	}

	/**
	 * Specifies that this pattern can occur one or more times(kleene star).
	 * This means at least one and at most infinite number of events can be matched in this state.
	 *
	 * @return The same pattern with applied Kleene plus operator
	 *
	 * @throws MalformedPatternException if quantifier already applied
	 */
	public Pattern<T, F> oneOrMore() {
		return oneOrMore(true);
	}

	/**
	 * Specifies that this pattern can occur one or more times(kleene star).
	 * This means at least one and at most infinite number of events can be matched in this state.
	 *
	 * If eagerness is enabled for a pattern A+B and sequence A1 A2 B will generate patterns:
	 * A1 B and A1 A2 B. If disabled A1 B, A2 B and A1 A2 B.
	 *
	 * @param eager if true the pattern always consumes earlier events
	 * @return The same pattern with applied Kleene plus operator
	 *
	 * @throws MalformedPatternException if quantifier already applied
	 */
	public Pattern<T, F> oneOrMore(final boolean eager) {
		checkIfQuantifierApplied();
		if (eager) {
			this.quantifier = Quantifier.ONE_OR_MORE_EAGER;
		} else {
			this.quantifier = Quantifier.ONE_OR_MORE_COMBINATIONS;
		}
		return this;
	}

	/**
	 * Specifies that this pattern can occur zero or once.
	 *
	 * @return The same pattern with applied Kleene ? operator
	 *
	 * @throws MalformedPatternException if quantifier already applied
	 */
	public Pattern<T, F> optional() {
		checkIfQuantifierApplied();
		this.quantifier = Quantifier.OPTIONAL;
		return this;
	}

	/**
	 * Specifies exact number of times that this pattern should be matched.
	 *
	 * @param times number of times matching event must appear
	 * @return The same pattern with number of times applied
	 *
	 * @throws MalformedPatternException if quantifier already applied
	 */
	public Pattern<T, F> times(int times) {
		checkIfQuantifierApplied();
		Preconditions.checkArgument(times > 0, "You should give a positive number greater than 0.");
		this.quantifier = Quantifier.TIMES;
		this.times = times;
		return this;
	}

	private void checkIfQuantifierApplied() {
		if (this.quantifier != Quantifier.ONE) {
			throw new MalformedPatternException("Already applied quantifier to this Pattern. Current quantifier is: " + this.quantifier);
		}
	}
}
