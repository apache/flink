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

package org.apache.flink.table.types.inference;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.types.inference.validators.PassingTypeValidator;
import org.apache.flink.table.types.inference.validators.TypeFamilyTypeValidator;
import org.apache.flink.table.types.inference.validators.TypeRootTypeValidator;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import java.util.Arrays;

/**
 * Validators for checking the input data types of a function call.
 *
 * @see InputTypeValidator
 */
@Internal
public final class InputTypeValidators {

	// --------------------------------------------------------------------------------------------
	// factory methods for type strategies
	// --------------------------------------------------------------------------------------------

	/**
	 * Validator which checks if operands are of given type roots.
	 */
	public static InputTypeValidator typeRoot(LogicalTypeRoot... types) {
		return new TypeRootTypeValidator(Arrays.asList(types));
	}

	/**
	 * Validator which checks if operands are of given type families.
	 */
	public static InputTypeValidator family(LogicalTypeFamily... types) {
		return new TypeFamilyTypeValidator(Arrays.asList(types));
	}

	// --------------------------------------------------------------------------------------------
	// concrete, reusable type strategies
	// --------------------------------------------------------------------------------------------

	/**
	 * Validator that does not perform any validation and always passes.
	 */
	public static final InputTypeValidator PASSING = new PassingTypeValidator();

	// --------------------------------------------------------------------------------------------

	private InputTypeValidators() {
		// no instantiation
	}
}
