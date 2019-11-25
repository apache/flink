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

package org.apache.flink.table.types.inference.strategies;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.InputTypeValidator;
import org.apache.flink.table.types.inference.TypeStrategy;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Type strategy that maps an {@link InputTypeValidator} to a {@link TypeStrategy} if the validator
 * matches.
 */
@Internal
public final class MatchingTypeStrategy implements TypeStrategy {

	private final Map<InputTypeValidator, TypeStrategy> matchers;

	public MatchingTypeStrategy(Map<InputTypeValidator, TypeStrategy> matchers) {
		this.matchers = matchers;
	}

	@Override
	public Optional<DataType> inferType(CallContext callContext) {
		for (Map.Entry<InputTypeValidator, TypeStrategy> matcher : matchers.entrySet()) {
			final InputTypeValidator validator = matcher.getKey();
			final TypeStrategy strategy = matcher.getValue();
			if (validator.getArgumentCount().isValidCount(callContext.getArgumentDataTypes().size()) &&
					validator.validate(callContext, false)) {
				return strategy.inferType(callContext);
			}
		}
		return Optional.empty();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		MatchingTypeStrategy that = (MatchingTypeStrategy) o;
		return matchers.equals(that.matchers);
	}

	@Override
	public int hashCode() {
		return Objects.hash(matchers);
	}
}
