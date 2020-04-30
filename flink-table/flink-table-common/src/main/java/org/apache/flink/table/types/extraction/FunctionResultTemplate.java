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

package org.apache.flink.table.types.extraction;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.TypeStrategies;
import org.apache.flink.table.types.inference.TypeStrategy;

import java.util.Objects;

/**
 * Template of a function intermediate result (i.e. accumulator) or final result (i.e. output).
 */
@Internal
final class FunctionResultTemplate {

	final DataType dataType;

	private FunctionResultTemplate(DataType dataType) {
		this.dataType = dataType;
	}

	static FunctionResultTemplate of(DataType dataType) {
		return new FunctionResultTemplate(dataType);
	}

	TypeStrategy toTypeStrategy() {
		return TypeStrategies.explicit(dataType);
	}

	Class<?> toClass() {
		return dataType.getConversionClass();
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		FunctionResultTemplate that = (FunctionResultTemplate) o;
		return dataType.equals(that.dataType);
	}

	@Override
	public int hashCode() {
		return Objects.hash(dataType);
	}
}
