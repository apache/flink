/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import java.util.List;

import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.hasRoot;

/**
 * Input type strategy that supplies the function's output {@link DataType} for each unknown
 * argument if available.
 */
@Internal
public final class OutputTypeInputTypeStrategy implements InputTypeStrategy {

	@Override
	public void inferInputTypes(MutableCallContext callContext) {
		callContext.getOutputDataType().ifPresent(t -> {
			final List<DataType> dataTypes = callContext.getArgumentDataTypes();
			for (int i = 0; i < dataTypes.size(); i++) {
				if (hasRoot(dataTypes.get(i).getLogicalType(), LogicalTypeRoot.NULL)) {
					callContext.mutateArgumentDataType(i, t);
				}
			}
		});
	}

	@Override
	public boolean equals(Object o) {
		return this == o || o instanceof OutputTypeInputTypeStrategy;
	}

	@Override
	public int hashCode() {
		return OutputTypeInputTypeStrategy.class.hashCode();
	}
}
