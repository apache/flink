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

package org.apache.flink.table.types.inference.validators;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.inference.InputTypeValidator;
import org.apache.flink.table.types.inference.InputTypeValidators;
import org.apache.flink.table.types.inference.utils.TestCallContext;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * Tests for {@link TypeRootTypeValidator}.
 */
public class TypeRootTypeValidatorTest {
	@Test
	public void testNullableDataType() {
		InputTypeValidator validator = InputTypeValidators.typeRoot(LogicalTypeRoot.BOOLEAN);

		boolean nullableResult = validator.validate(TestCallContext.forCall(DataTypes.BOOLEAN().nullable()), false);

		assertThat(nullableResult, is(true));
	}

	@Test
	public void testNotNullDataType() {
		InputTypeValidator validator = InputTypeValidators.typeRoot(LogicalTypeRoot.BOOLEAN);

		boolean notNullResult = validator.validate(TestCallContext.forCall(DataTypes.BOOLEAN().notNull()), false);

		assertThat(notNullResult, is(true));
	}

	@Test
	public void testFailingValidation() {
		InputTypeValidator validator = InputTypeValidators.typeRoot(LogicalTypeRoot.VARCHAR);

		boolean validationResult = validator.validate(TestCallContext.forCall(DataTypes.BOOLEAN()), false);

		assertThat(validationResult, is(false));
	}
}
