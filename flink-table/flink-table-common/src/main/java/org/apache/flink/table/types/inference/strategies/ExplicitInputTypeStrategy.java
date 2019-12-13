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

import java.util.List;
import java.util.Objects;

/**
 * Input type strategy that supplies a fixed {@link DataType} for each argument.
 */
@Internal
public final class ExplicitInputTypeStrategy implements InputTypeStrategy {

	private final List<DataType> dataTypes;

	public ExplicitInputTypeStrategy(List<DataType> dataTypes) {
		this.dataTypes = dataTypes;
	}

	@Override
	public void inferInputTypes(MutableCallContext callContext) {
		if (callContext.getArgumentDataTypes().size() != dataTypes.size()) {
			return;
		}
		for (int i = 0; i < dataTypes.size(); i++) {
			callContext.mutateArgumentDataType(i, dataTypes.get(i));
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
		ExplicitInputTypeStrategy that = (ExplicitInputTypeStrategy) o;
		return dataTypes.equals(that.dataTypes);
	}

	@Override
	public int hashCode() {
		return Objects.hash(dataTypes);
	}
}
