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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.TypeStrategies;
import org.apache.flink.table.types.inference.TypeStrategy;
import org.apache.flink.table.types.inference.TypeTransformation;
import org.apache.flink.table.types.inference.utils.TestCallContext;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;

import org.hamcrest.CoreMatchers;
import org.junit.Test;

import java.util.Optional;

import static org.junit.Assert.assertThat;

/**
 * Tests for {@link CascadeTypeStrategy}.
 */
public class CascadeTypeStrategyTest {
	@Test
	public void testIdentityTransformation() {
		TypeStrategy typeStrategy = TypeStrategies.cascade(
			TypeStrategies.BOOLEAN_NOT_NULL,
			((callContext, typeToTransform) -> typeToTransform));

		Optional<DataType> dataType = typeStrategy.inferType(TestCallContext.forCall());

		assertThat(dataType.get(), CoreMatchers.equalTo(DataTypes.BOOLEAN().notNull()));
	}

	@Test
	public void testNoTransformations() {
		TypeStrategy typeStrategy = TypeStrategies.cascade(TypeStrategies.BOOLEAN_NOT_NULL);

		Optional<DataType> dataType = typeStrategy.inferType(TestCallContext.forCall());

		assertThat(dataType.get(), CoreMatchers.equalTo(DataTypes.BOOLEAN().notNull()));
	}

	@Test
	public void testAllTransformationsApplied() {
		TypeStrategy typeStrategy = TypeStrategies.cascade(
			TypeStrategies.BOOLEAN_NOT_NULL,
			(transformIf(LogicalTypeRoot.BOOLEAN, DataTypes.INT())),
			transformIf(LogicalTypeRoot.INTEGER, DataTypes.BIGINT())
		);

		Optional<DataType> dataType = typeStrategy.inferType(TestCallContext.forCall());

		assertThat(dataType.get(), CoreMatchers.equalTo(DataTypes.BIGINT()));
	}

	private TypeTransformation transformIf(LogicalTypeRoot typeRoot, DataType resultType) {
		return (callContext, typeToTransform) -> {
			if (LogicalTypeChecks.hasRoot(typeToTransform.getLogicalType(), typeRoot)) {
				return resultType;
			}
			return typeToTransform;
		};
	}
}
