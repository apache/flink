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

import org.apache.flink.util.Preconditions;

import java.util.EnumSet;
import java.util.Objects;

/**
 * A quantifier describing the Pattern. There are three main groups of {@link Quantifier}.
 *
 * <p><ol>
 *     <li>Single</li>
 *     <li>Looping</li>
 *     <li>Times</li>
 * </ol>
 *
 * <p>Each {@link Pattern} can be optional and have a {@link ConsumingStrategy}. Looping and Times also hava an
 * additional inner consuming strategy that is applied between accepted events in the pattern.
 */
public class Quantifier {

	private final EnumSet<QuantifierProperty> properties;

	private final ConsumingStrategy consumingStrategy;

	private ConsumingStrategy innerConsumingStrategy = ConsumingStrategy.SKIP_TILL_NEXT;

	private Quantifier(
			final ConsumingStrategy consumingStrategy,
			final QuantifierProperty first,
			final QuantifierProperty... rest) {
		this.properties = EnumSet.of(first, rest);
		this.consumingStrategy = consumingStrategy;
	}

	public static Quantifier one(final ConsumingStrategy consumingStrategy) {
		return new Quantifier(consumingStrategy, QuantifierProperty.SINGLE);
	}

	public static Quantifier looping(final ConsumingStrategy consumingStrategy) {
		return new Quantifier(consumingStrategy, QuantifierProperty.LOOPING);
	}

	public static Quantifier times(final ConsumingStrategy consumingStrategy) {
		return new Quantifier(consumingStrategy, QuantifierProperty.TIMES);
	}

	public boolean hasProperty(QuantifierProperty property) {
		return properties.contains(property);
	}

	public ConsumingStrategy getInnerConsumingStrategy() {
		return innerConsumingStrategy;
	}

	public ConsumingStrategy getConsumingStrategy() {
		return consumingStrategy;
	}

	private static void checkPattern(boolean condition, Object errorMessage) {
		if (!condition) {
			throw new MalformedPatternException(String.valueOf(errorMessage));
		}
	}

	public void combinations() {
		checkPattern(!hasProperty(QuantifierProperty.SINGLE), "Combinations not applicable to " + this + "!");
		checkPattern(innerConsumingStrategy != ConsumingStrategy.STRICT, "You can apply either combinations or consecutive, not both!");
		checkPattern(innerConsumingStrategy != ConsumingStrategy.SKIP_TILL_ANY, "Combinations already applied!");

		innerConsumingStrategy = ConsumingStrategy.SKIP_TILL_ANY;
	}

	public void consecutive() {
		checkPattern(hasProperty(QuantifierProperty.LOOPING) || hasProperty(QuantifierProperty.TIMES), "Consecutive not applicable to " + this + "!");
		checkPattern(innerConsumingStrategy != ConsumingStrategy.SKIP_TILL_ANY, "You can apply either combinations or consecutive, not both!");
		checkPattern(innerConsumingStrategy != ConsumingStrategy.STRICT, "Consecutive already applied!");

		innerConsumingStrategy = ConsumingStrategy.STRICT;
	}

	public void optional() {
		checkPattern(!hasProperty(QuantifierProperty.OPTIONAL), "Optional already applied!");
		checkPattern(!(consumingStrategy == ConsumingStrategy.NOT_NEXT ||
					consumingStrategy == ConsumingStrategy.NOT_FOLLOW), "NOT pattern cannot be optional");

		properties.add(Quantifier.QuantifierProperty.OPTIONAL);
	}

	public void greedy() {
		checkPattern(!(innerConsumingStrategy == ConsumingStrategy.SKIP_TILL_ANY),
			"Option not applicable to FollowedByAny pattern");
		checkPattern(!hasProperty(Quantifier.QuantifierProperty.SINGLE),
			"Option not applicable to singleton quantifier");

		properties.add(QuantifierProperty.GREEDY);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		Quantifier that = (Quantifier) o;
		return Objects.equals(properties, that.properties) &&
				consumingStrategy == that.consumingStrategy &&
				innerConsumingStrategy == that.innerConsumingStrategy;
	}

	@Override
	public int hashCode() {
		return Objects.hash(properties, consumingStrategy, innerConsumingStrategy);
	}

	@Override
	public String toString() {
		return "Quantifier{" +
			"properties=" + properties +
			", consumingStrategy=" + consumingStrategy +
			", innerConsumingStrategy=" + innerConsumingStrategy +
			'}';
	}

	/**
	 * Properties that a {@link Quantifier} can have. Not all combinations are valid.
	 */
	public enum QuantifierProperty {
		SINGLE,
		LOOPING,
		TIMES,
		OPTIONAL,
		GREEDY
	}

	/**
	 * Describes strategy for which events are matched in this {@link Pattern}. See docs for more info.
	 */
	public enum ConsumingStrategy {
		STRICT,
		SKIP_TILL_NEXT,
		SKIP_TILL_ANY,

		NOT_FOLLOW,
		NOT_NEXT
	}

	/**
	 * Describe the times this {@link Pattern} can occur.
	 */
	public static class Times {
		private final int from;
		private final int to;

		private Times(int from, int to) {
			Preconditions.checkArgument(from > 0, "The from should be a positive number greater than 0.");
			Preconditions.checkArgument(to >= from, "The to should be a number greater than or equal to from: " + from + ".");
			this.from = from;
			this.to = to;
		}

		public int getFrom() {
			return from;
		}

		public int getTo() {
			return to;
		}

		public static Times of(int from, int to) {
			return new Times(from, to);
		}

		public static Times of(int times) {
			return new Times(times, times);
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			Times times = (Times) o;
			return from == times.from &&
				to == times.to;
		}

		@Override
		public int hashCode() {
			return Objects.hash(from, to);
		}
	}
}
