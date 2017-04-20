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

public enum Quantifier {
	ONE,
	ZERO_OR_MORE_EAGER(QuantifierProperty.LOOPING, QuantifierProperty.EAGER),
	ZERO_OR_MORE_COMBINATIONS(QuantifierProperty.LOOPING),
	ZERO_OR_MORE_EAGER_STRICT(QuantifierProperty.EAGER, QuantifierProperty.STRICT, QuantifierProperty.LOOPING),
	ZERO_OR_MORE_COMBINATIONS_STRICT(QuantifierProperty.STRICT, QuantifierProperty.LOOPING),
	ONE_OR_MORE_EAGER(
		QuantifierProperty.LOOPING,
		QuantifierProperty.EAGER,
		QuantifierProperty.AT_LEAST_ONE),
	ONE_OR_MORE_EAGER_STRICT(
		QuantifierProperty.STRICT,
		QuantifierProperty.LOOPING,
		QuantifierProperty.EAGER,
		QuantifierProperty.AT_LEAST_ONE),
	ONE_OR_MORE_COMBINATIONS(QuantifierProperty.LOOPING, QuantifierProperty.AT_LEAST_ONE),
	ONE_OR_MORE_COMBINATIONS_STRICT(
		QuantifierProperty.STRICT,
		QuantifierProperty.LOOPING,
		QuantifierProperty.AT_LEAST_ONE),
	TIMES(QuantifierProperty.TIMES),
	TIMES_STRICT(QuantifierProperty.TIMES, QuantifierProperty.STRICT),
	OPTIONAL;

	private final EnumSet<QuantifierProperty> properties;

	Quantifier(final QuantifierProperty first, final QuantifierProperty... rest) {
		this.properties = EnumSet.of(first, rest);
	}

	Quantifier() {
		this.properties = EnumSet.noneOf(QuantifierProperty.class);
	}

	public boolean hasProperty(QuantifierProperty property) {
		return properties.contains(property);
	}

	public enum QuantifierProperty {
		LOOPING,
		EAGER,
		AT_LEAST_ONE,
		STRICT,
		TIMES
	}

}
