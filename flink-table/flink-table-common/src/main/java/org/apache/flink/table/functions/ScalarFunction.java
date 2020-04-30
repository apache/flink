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

package org.apache.flink.table.functions;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.functions.InvalidTypesException;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.types.extraction.TypeInferenceExtractor;
import org.apache.flink.table.types.inference.TypeInference;

/**
 * Base class for a user-defined scalar function. A user-defined scalar functions maps zero, one,
 * or multiple scalar values to a new scalar value.
 *
 * <p>The behavior of a {@link ScalarFunction} can be defined by implementing a custom evaluation
 * method. An evaluation method must be declared publicly and named <code>eval</code>. Evaluation
 * methods can also be overloaded by implementing multiple methods named <code>eval</code>.
 *
 * <p>By default, input and output data types are automatically extracted using reflection. If the
 * reflective information is not sufficient, it can be supported and enriched with {@link DataTypeHint}
 * and {@link FunctionHint} annotations.
 *
 * <p>The following examples show how to specify a scalar function:
 *
 * <pre>
 * {@code
 *   // a function that accepts two INT arguments and computes a sum
 *   class SumFunction extends ScalarFunction {
 *     public Integer eval(Integer a, Integer b) {
 *       return a + b;
 *     }
 *   }
 *
 *   // a function that accepts either INT NOT NULL or BOOLEAN NOT NULL and computes a STRING
 *   class StringifyFunction extends ScalarFunction {
 *     public String eval(int i) {
 *       return String.valueOf(i);
 *     }
 *     public String eval(boolean b) {
 *       return String.valueOf(b);
 *     }
 *   }
 *
 *   // a function that accepts either INT or BOOLEAN and computes a STRING using function hints
 *   @FunctionHint(input = [@DataTypeHint("INT")])
 *   @FunctionHint(input = [@DataTypeHint("BOOLEAN")])
 *   class StringifyFunction extends ScalarFunction {
 *     public String eval(Object o) {
 *       return o.toString();
 *     }
 *   }
 *
 *   // a function that accepts any data type as argument and computes a STRING
 *   class StringifyFunction extends ScalarFunction {
 *     public String eval(@DataTypeHint(inputGroup = InputGroup.ANY) Object o) {
 *       return o.toString();
 *     }
 *   }
 *
 *   // a function that accepts an arbitrary number of BIGINT values and computes a DECIMAL(10, 4)
 *   class SumFunction extends ScalarFunction {
 *     public @DataTypeHint("DECIMAL(10, 4)") BigDecimal eval(Long... values) {
 *       // ...
 *     }
 *   }
 * }
 * </pre>
 *
 * <p>For storing a user-defined function in a catalog, the class must have a default constructor and
 * must be instantiable during runtime.
 */
@PublicEvolving
public abstract class ScalarFunction extends UserDefinedFunction {

	/**
	 * Returns the result type of the evaluation method with a given signature.
	 *
	 * @deprecated This method uses the old type system and is based on the old reflective extraction
	 *             logic. The method will be removed in future versions and is only called when using
	 *             the deprecated {@code TableEnvironment.registerFunction(...)} method. The new reflective
	 *             extraction logic (possibly enriched with {@link DataTypeHint} and {@link FunctionHint})
	 *             should be powerful enough to cover most use cases. For advanced users, it is possible
	 *             to override {@link UserDefinedFunction#getTypeInference(DataTypeFactory)}.
	 */
	@Deprecated
	public TypeInformation<?> getResultType(Class<?>[] signature) {
		return null;
	}

	/**
	 * Returns {@link TypeInformation} about the operands of the evaluation method with a given
	 * signature.
	 *
	 * @deprecated This method uses the old type system and is based on the old reflective extraction
	 *             logic. The method will be removed in future versions and is only called when using
	 *             the deprecated {@code TableEnvironment.registerFunction(...)} method. The new reflective
	 *             extraction logic (possibly enriched with {@link DataTypeHint} and {@link FunctionHint})
	 *             should be powerful enough to cover most use cases. For advanced users, it is possible
	 *             to override {@link UserDefinedFunction#getTypeInference(DataTypeFactory)}.
	 */
	@Deprecated
	public TypeInformation<?>[] getParameterTypes(Class<?>[] signature) {
		final TypeInformation<?>[] types = new TypeInformation<?>[signature.length];
		for (int i = 0; i < signature.length; i++) {
			try {
				types[i] = TypeExtractor.getForClass(signature[i]);
			} catch (InvalidTypesException e) {
				throw new ValidationException(
					"Parameter types of scalar function " + this.getClass().getCanonicalName() +
					" cannot be automatically determined. Please provide type information manually.");
			}
		}
		return types;
	}

	@Override
	public final FunctionKind getKind() {
		return FunctionKind.SCALAR;
	}

	@Override
	public TypeInference getTypeInference(DataTypeFactory typeFactory) {
		return TypeInferenceExtractor.forScalarFunction(typeFactory, getClass());
	}
}
