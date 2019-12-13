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
import org.apache.flink.table.types.inference.InputTypeStrategy;
import org.apache.flink.table.types.inference.MutableCallContext;
import org.apache.flink.table.types.logical.LogicalType;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Objects;

/**
 * Input type strategy for enriching data types with an expected conversion class. This is in particular
 * useful when a data type has been created out of a {@link LogicalType} but runtime hints are still missing.
 */
@Internal
public final class BridgingInputTypeStrategy implements InputTypeStrategy {

	private final List<BridgingSignature> bridgingSignatures;

	public BridgingInputTypeStrategy(List<BridgingSignature> bridgingSignatures) {
		this.bridgingSignatures = bridgingSignatures;
	}

	@Override
	public void inferInputTypes(MutableCallContext callContext) {
		final List<DataType> actualDataTypes = callContext.getArgumentDataTypes();
		for (BridgingSignature bridgingSignature : bridgingSignatures) {
			if (bridgingSignature.matches(actualDataTypes)) {
				bridgingSignature.enrich(actualDataTypes.size(), callContext);
				return; // there should be only one matching signature
			}
		}
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		BridgingInputTypeStrategy that = (BridgingInputTypeStrategy) o;
		return bridgingSignatures.equals(that.bridgingSignatures);
	}

	@Override
	public int hashCode() {
		return Objects.hash(bridgingSignatures);
	}

	/**
	 * Helper class that represents a signature of input arguments.
	 *
	 * <p>Note: Array elements can be null for skipping the enrichment of certain arguments. If the
	 * signature has varargs, the last data type represents the varying argument type.
	 */
	public static final class BridgingSignature {

		private final DataType[] expectedDataTypes;

		private final boolean isVarying;

		public BridgingSignature(DataType[] expectedDataTypes, boolean isVarying) {
			this.expectedDataTypes = expectedDataTypes;
			this.isVarying = isVarying;
		}

		public boolean matches(List<DataType> actualDataTypes) {
			if (!isValidCount(actualDataTypes.size())) {
				return false;
			}
			for (int i = 0; i < actualDataTypes.size(); i++) {
				final LogicalType actualType = actualDataTypes.get(i).getLogicalType();
				final DataType expectedType = getExpectedDataType(i);
				if (expectedType != null && !actualType.equals(expectedType.getLogicalType())) {
					return false;
				}
			}
			return true;
		}

		public void enrich(int argumentCount, MutableCallContext mutableCallContext) {
			for (int i = 0; i < argumentCount; i++) {
				mutableCallContext.mutateArgumentDataType(i, getExpectedDataType(i));
			}
		}

		private boolean isValidCount(int actualCount) {
			final int minCount;
			if (isVarying) {
				minCount = expectedDataTypes.length - 1;
			} else {
				minCount = expectedDataTypes.length;
			}
			final int maxCount;
			if (isVarying) {
				maxCount = Integer.MAX_VALUE;
			} else {
				maxCount = expectedDataTypes.length;
			}
			return actualCount >= minCount && actualCount <= maxCount;
		}

		private @Nullable DataType getExpectedDataType(int pos) {
			if (pos < expectedDataTypes.length) {
				return expectedDataTypes[pos];
			} else if (isVarying) {
				return expectedDataTypes[expectedDataTypes.length - 1];
			}
			throw new IllegalStateException("Argument count should have been validated before.");
		}
	}
}
