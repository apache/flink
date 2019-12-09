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
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.ArgumentCount;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.ConstantArgumentCount;
import org.apache.flink.table.types.inference.InputTypeStrategy;
import org.apache.flink.table.types.inference.Signature;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.AbstractList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Strategy for inferring and validating the input using a disjunction of multiple {@link InputTypeStrategy}s
 * into one like {@code f(NUMERIC) || f(STRING)}.
 */
@Internal
public final class OrInputTypeStrategy implements InputTypeStrategy {

	private final List<? extends InputTypeStrategy> inputStrategies;

	public OrInputTypeStrategy(List<? extends InputTypeStrategy> inputStrategies) {
		Preconditions.checkArgument(inputStrategies.size() > 0);
		this.inputStrategies = inputStrategies;
	}

	@Override
	public ArgumentCount getArgumentCount() {
		final List<ArgumentCount> counts = new AbstractList<ArgumentCount>() {
			public ArgumentCount get(int index) {
				return inputStrategies.get(index).getArgumentCount();
			}

			public int size() {
				return inputStrategies.size();
			}
		};
		final Integer min = commonMin(counts);
		final Integer max = commonMax(counts);
		final ArgumentCount compositeCount = new ArgumentCount() {
			@Override
			public boolean isValidCount(int count) {
				for (ArgumentCount c : counts) {
					if (c.isValidCount(count)) {
						return true;
					}
				}
				return false;
			}

			@Override
			public Optional<Integer> getMinCount() {
				return Optional.ofNullable(min);
			}

			@Override
			public Optional<Integer> getMaxCount() {
				return Optional.ofNullable(max);
			}
		};

		// use constant count if applicable
		if (min == null || max == null) {
			// no boundaries
			return compositeCount;
		}
		for (int i = min; i <= max; i++) {
			if (!compositeCount.isValidCount(i)) {
				// not the full range
				return compositeCount;
			}
		}
		if (min.equals(max)) {
			return ConstantArgumentCount.of(min);
		}
		return ConstantArgumentCount.between(min, max);
	}

	@Override
	public Optional<List<DataType>> inferInputTypes(CallContext callContext, boolean throwOnFailure) {
		final List<LogicalType> actualTypes = callContext.getArgumentDataTypes().stream()
			.map(DataType::getLogicalType)
			.collect(Collectors.toList());

		Optional<List<DataType>> closestDataTypes = Optional.empty();
		for (InputTypeStrategy inputStrategy : inputStrategies) {
			final Optional<List<DataType>> inferredDataTypes = inputStrategy.inferInputTypes(
				callContext,
				false);
			// types do not match at all
			if (!inferredDataTypes.isPresent()) {
				continue;
			}
			final List<LogicalType> inferredTypes = inferredDataTypes.get().stream()
				.map(DataType::getLogicalType)
				.collect(Collectors.toList());
			// types match exactly
			if (actualTypes.equals(inferredTypes)) {
				return inferredDataTypes;
			}
			// type matches with some casting
			else if (!closestDataTypes.isPresent()) {
				closestDataTypes = inferredDataTypes;
			}
		}

		return closestDataTypes;
	}

	@Override
	public List<Signature> getExpectedSignatures(FunctionDefinition definition) {
		return inputStrategies.stream()
			.flatMap(v -> v.getExpectedSignatures(definition).stream())
			.collect(Collectors.toList());
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		OrInputTypeStrategy that = (OrInputTypeStrategy) o;
		return Objects.equals(inputStrategies, that.inputStrategies);
	}

	@Override
	public int hashCode() {
		return Objects.hash(inputStrategies);
	}

	// --------------------------------------------------------------------------------------------

	/**
	 * Returns the common minimum argument count or null if undefined.
	 */
	private @Nullable Integer commonMin(List<ArgumentCount> counts) {
		// min=5, min=3, min=0           -> min=0
		// min=5, min=3, min=0, min=null -> min=null
		int commonMin = Integer.MAX_VALUE;
		for (ArgumentCount count : counts) {
			final Optional<Integer> min = count.getMinCount();
			if (!min.isPresent()) {
				return null;
			}
			commonMin = Math.min(commonMin, min.get());
		}
		if (commonMin == Integer.MAX_VALUE) {
			return null;
		}
		return commonMin;
	}

	/**
	 * Returns the common maximum argument count or null if undefined.
	 */
	private @Nullable Integer commonMax(List<ArgumentCount> counts) {
		// max=5, max=3, max=0           -> max=5
		// max=5, max=3, max=0, max=null -> max=null
		int commonMax = Integer.MIN_VALUE;
		for (ArgumentCount count : counts) {
			final Optional<Integer> max = count.getMaxCount();
			if (!max.isPresent()) {
				return null;
			}
			commonMax = Math.max(commonMax, max.get());
		}
		if (commonMax == Integer.MIN_VALUE) {
			return null;
		}
		return commonMax;
	}
}
