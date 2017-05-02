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

import java.util.EnumSet;
import java.util.Objects;

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

	public static Quantifier ONE(final ConsumingStrategy consumingStrategy) {
		return new Quantifier(consumingStrategy, QuantifierProperty.SINGLE);
	}

	public static Quantifier ONE_OR_MORE(final ConsumingStrategy consumingStrategy) {
		return new Quantifier(consumingStrategy, QuantifierProperty.LOOPING);
	}

	public static Quantifier TIMES(final ConsumingStrategy consumingStrategy) {
		return new Quantifier(consumingStrategy, QuantifierProperty.TIMES);
	}

	public boolean hasProperty(QuantifierProperty property) {
		return properties.contains(property);
	}

	public ConsumingStrategy getConsumingStrategy() {
		return consumingStrategy;
	}

	public ConsumingStrategy getInnerConsumingStrategy() {
		return innerConsumingStrategy;
	}

	private static void checkPattern(boolean condition, Object errorMessage) {
		if (!condition) {
			throw new MalformedPatternException(String.valueOf(errorMessage));
		}
	}

	public void combinations() {
		checkPattern(!hasProperty(QuantifierProperty.SINGLE), "Combinations not applicable to " + this + "!");
		checkPattern(innerConsumingStrategy != ConsumingStrategy.STRICT, "You can apply apply either combinations or consecutive, not both!");
		checkPattern(innerConsumingStrategy != ConsumingStrategy.SKIP_TILL_ANY, "Combinations already applied!");

		innerConsumingStrategy = ConsumingStrategy.SKIP_TILL_ANY;
	}

	public void consecutive() {
		checkPattern(hasProperty(QuantifierProperty.LOOPING) || hasProperty(QuantifierProperty.TIMES), "Combinations not applicable to " + this + "!");
		checkPattern(innerConsumingStrategy != ConsumingStrategy.SKIP_TILL_ANY, "You can apply apply either combinations or consecutive, not both!");
		checkPattern(innerConsumingStrategy != ConsumingStrategy.STRICT, "Combinations already applied!");

		innerConsumingStrategy = ConsumingStrategy.STRICT;
	}

	public void optional() {
		if (hasProperty(Quantifier.QuantifierProperty.OPTIONAL)) {
			throw new MalformedPatternException("Optional already applied!");
		}
		properties.add(Quantifier.QuantifierProperty.OPTIONAL);
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
				consumingStrategy == that.consumingStrategy;
	}

	@Override
	public int hashCode() {
		return Objects.hash(properties, consumingStrategy);
	}

	/**
	 * Properties that a {@link Quantifier} can have. Not all combinations are valid.
	 */
	public enum QuantifierProperty {
		SINGLE,
		LOOPING,
		TIMES,
		OPTIONAL
	}

	public enum ConsumingStrategy {
		STRICT,
		SKIP_TILL_NEXT,
		SKIP_TILL_ANY
	}

}
