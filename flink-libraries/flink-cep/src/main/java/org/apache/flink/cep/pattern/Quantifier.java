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
		return new Quantifier(consumingStrategy, QuantifierProperty.LOOPING, QuantifierProperty.EAGER);
	}

	public static Quantifier TIMES(final ConsumingStrategy consumingStrategy) {
		return new Quantifier(consumingStrategy, QuantifierProperty.TIMES, QuantifierProperty.EAGER);
	}

	public boolean hasProperty(QuantifierProperty property) {
		return properties.contains(property);
	}

	public ConsumingStrategy getConsumingStrategy() {
		return consumingStrategy;
	}

	public void combinations() {
		if (!hasProperty(QuantifierProperty.SINGLE) && !hasProperty(Quantifier.QuantifierProperty.EAGER)) {
			throw new MalformedPatternException("Combinations already allowed!");
		}

		if (hasProperty(Quantifier.QuantifierProperty.LOOPING) || hasProperty(Quantifier.QuantifierProperty.TIMES)) {
			properties.remove(Quantifier.QuantifierProperty.EAGER);
		} else {
			throw new MalformedPatternException("Combinations not applicable to " + this + "!");
		}
	}

	public void consecutive() {
		if (!hasProperty(QuantifierProperty.SINGLE) && hasProperty(Quantifier.QuantifierProperty.CONSECUTIVE)) {
			throw new MalformedPatternException("Strict continuity already applied!");
		}

		if (hasProperty(Quantifier.QuantifierProperty.LOOPING) || hasProperty(Quantifier.QuantifierProperty.TIMES)) {
			properties.add(Quantifier.QuantifierProperty.CONSECUTIVE);
		} else {
			throw new MalformedPatternException("Strict continuity not applicable to " + this + "!");
		}
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
		EAGER,
		CONSECUTIVE,
		OPTIONAL
	}

	public enum ConsumingStrategy {
		STRICT,
		SKIP_TILL_NEXT,
		SKIP_TILL_ANY
	}

}
