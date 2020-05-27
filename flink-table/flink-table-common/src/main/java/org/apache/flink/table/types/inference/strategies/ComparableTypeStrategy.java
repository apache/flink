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
import org.apache.flink.table.types.inference.InputTypeStrategy;
import org.apache.flink.table.types.inference.Signature;
import org.apache.flink.table.types.logical.DistinctType;
import org.apache.flink.table.types.logical.LegacyTypeInformationType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RawType;
import org.apache.flink.table.types.logical.StructuredType;
import org.apache.flink.table.types.logical.StructuredType.StructuredComparision;
import org.apache.flink.util.Preconditions;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.hasFamily;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.hasRoot;

/**
 * An {@link InputTypeStrategy} that checks if all input arguments can be compared with each other with
 * the minimal provided comparision.
 *
 * <p>It requires at least two arguments.
 *
 * <p>For the rules which types are comparable with which types see {@link #areComparable(LogicalType, LogicalType)}.
 */
@Internal
public final class ComparableTypeStrategy implements InputTypeStrategy {
	private final StructuredComparision requiredComparision;
	private final ArgumentCount argumentCount;

	public ComparableTypeStrategy(
			ArgumentCount argumentCount,
			StructuredComparision requiredComparision) {
		Preconditions.checkArgument(
			argumentCount.getMinCount().map(c -> c >= 2).orElse(false),
			"Comparable type strategy requires no less than two arguments. Actual minimal argument count: %s",
			argumentCount.getMinCount().map(Objects::toString).orElse("<None>"));
		Preconditions.checkArgument(requiredComparision != StructuredComparision.NONE);
		this.requiredComparision = requiredComparision;
		this.argumentCount = argumentCount;
	}

	@Override
	public ArgumentCount getArgumentCount() {
		return argumentCount;
	}

	@Override
	public Optional<List<DataType>> inferInputTypes(CallContext callContext, boolean throwOnFailure) {
		List<DataType> argumentDataTypes = callContext.getArgumentDataTypes();
		for (int i = 0; i < argumentDataTypes.size() - 1; i++) {
			LogicalType firstLogicalType = argumentDataTypes.get(i).getLogicalType();
			LogicalType secondLogicalType = argumentDataTypes.get(i + 1).getLogicalType();

			if (!areComparable(firstLogicalType, secondLogicalType)) {
				if (throwOnFailure) {
					throw callContext.newValidationError(
						"All types in EQUALS should support %s comparison with each other. Can not compare %s with %s",
						requiredComparision == StructuredComparision.EQUALS ? "'EQUALS'" : "both 'EQUALS' and 'ORDER'",
						firstLogicalType,
						secondLogicalType
					);
				}

				return Optional.empty();
			}
		}

		return Optional.of(argumentDataTypes);
	}

	private boolean areComparable(LogicalType firstLogicalType, LogicalType secondLogicalType) {
		// A hack to support legacy types. To be removed when we drop the legacy types.
		if (firstLogicalType instanceof LegacyTypeInformationType ||
				secondLogicalType instanceof LegacyTypeInformationType) {
			return true;
		}

		// everything is comparable with null, it should return null in that case
		if (hasRoot(firstLogicalType, LogicalTypeRoot.NULL) || hasRoot(secondLogicalType, LogicalTypeRoot.NULL)) {
			return true;
		}

		if (hasRoot(firstLogicalType, LogicalTypeRoot.STRUCTURED_TYPE) &&
				hasRoot(secondLogicalType, LogicalTypeRoot.STRUCTURED_TYPE)) {
			return areStructuredTypesComparable(firstLogicalType, secondLogicalType);
		}

		if (hasRoot(firstLogicalType, LogicalTypeRoot.DISTINCT_TYPE) &&
				hasRoot(secondLogicalType, LogicalTypeRoot.DISTINCT_TYPE)) {
			return areDistinctTypesComparable(firstLogicalType, secondLogicalType);
		}

		if (hasRoot(firstLogicalType, LogicalTypeRoot.RAW) && hasRoot(secondLogicalType, LogicalTypeRoot.RAW)) {
			return areRawTypesComparable(firstLogicalType, secondLogicalType);
		}

		if (hasRoot(firstLogicalType, LogicalTypeRoot.ARRAY) && hasRoot(secondLogicalType, LogicalTypeRoot.ARRAY)) {
			return areCollectionsComparable(firstLogicalType, secondLogicalType);
		}

		if (hasRoot(firstLogicalType, LogicalTypeRoot.MULTISET) &&
			hasRoot(secondLogicalType, LogicalTypeRoot.MULTISET)) {
			return areCollectionsComparable(firstLogicalType, secondLogicalType);
		}

		if (hasRoot(firstLogicalType, LogicalTypeRoot.MAP) &&
				hasRoot(secondLogicalType, LogicalTypeRoot.MAP)) {
			return areCollectionsComparable(firstLogicalType, secondLogicalType);
		}

		if (hasRoot(firstLogicalType, LogicalTypeRoot.ROW) && hasRoot(secondLogicalType, LogicalTypeRoot.ROW)) {
			return areCollectionsComparable(firstLogicalType, secondLogicalType);
		}

		if (firstLogicalType.getTypeRoot() == secondLogicalType.getTypeRoot()) {
			return true;
		}

		if (hasFamily(firstLogicalType, LogicalTypeFamily.NUMERIC) &&
				hasFamily(secondLogicalType, LogicalTypeFamily.NUMERIC)) {
			return true;
		}

		// DATE + ALL TIMESTAMPS
		if (hasFamily(firstLogicalType, LogicalTypeFamily.DATETIME) &&
				hasFamily(secondLogicalType, LogicalTypeFamily.DATETIME)) {
			return true;
		}

		// VARCHAR + CHAR (we do not compare collations here)
		if (hasFamily(firstLogicalType, LogicalTypeFamily.CHARACTER_STRING) &&
				hasFamily(secondLogicalType, LogicalTypeFamily.CHARACTER_STRING)) {
			return true;
		}

		// VARBINARY + BINARY
		if (hasFamily(firstLogicalType, LogicalTypeFamily.BINARY_STRING) &&
				hasFamily(secondLogicalType, LogicalTypeFamily.BINARY_STRING)) {
			return true;
		}

		return false;
	}

	private boolean areRawTypesComparable(LogicalType firstLogicalType, LogicalType secondLogicalType) {
		return firstLogicalType.copy(true).equals(secondLogicalType.copy(true)) &&
			Comparable.class.isAssignableFrom(((RawType<?>) firstLogicalType).getOriginatingClass());
	}

	private boolean areDistinctTypesComparable(LogicalType firstLogicalType, LogicalType secondLogicalType) {
		DistinctType firstDistinctType = (DistinctType) firstLogicalType;
		DistinctType secondDistinctType = (DistinctType) secondLogicalType;
		return firstLogicalType.copy(true).equals(secondLogicalType.copy(true)) &&
			areComparable(firstDistinctType.getSourceType(), secondDistinctType.getSourceType());
	}

	private boolean areStructuredTypesComparable(LogicalType firstLogicalType, LogicalType secondLogicalType) {
		return firstLogicalType.copy(true).equals(secondLogicalType.copy(true)) &&
			hasRequiredComparision((StructuredType) firstLogicalType);
	}

	private boolean areCollectionsComparable(LogicalType firstLogicalType, LogicalType secondLogicalType) {
		List<LogicalType> firstChildren = firstLogicalType.getChildren();
		List<LogicalType> secondChildren = secondLogicalType.getChildren();

		if (firstChildren.size() != secondChildren.size()) {
			return false;
		}

		for (int i = 0; i < firstChildren.size(); i++) {
			if (!areComparable(firstChildren.get(i), secondChildren.get(i))) {
				return false;
			}
		}

		return true;
	}

	@Override
	public List<Signature> getExpectedSignatures(FunctionDefinition definition) {
		return Collections.singletonList(Signature.of(Signature.Argument.of("<COMPARABLE>...")));
	}

	private Boolean hasRequiredComparision(StructuredType structuredType) {
		switch (requiredComparision) {
			case EQUALS:
				return structuredType.getComparision().isEquality();
			case FULL:
				return structuredType.getComparision().isComparison();
			case NONE:
			default:
				// this is not important, required comparision will never be NONE
				return true;
		}
	}
}
