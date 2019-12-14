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
import org.apache.flink.table.api.ValidationException;

/**
 * Base class for a user-defined scalar function. A user-defined scalar functions maps zero, one,
 * or multiple scalar values to a new scalar value.
 *
 * <p>The behavior of a {@link ScalarFunction} can be defined by implementing a custom evaluation
 * method. An evaluation method must be declared publicly and named <code>eval</code>. Evaluation
 * methods can also be overloaded by implementing multiple methods named <code>eval</code>.
 *
 * <p>User-defined functions must have a default constructor and must be instantiable during runtime.
 *
 * <p>By default the result type of an evaluation method is determined by Flink's type extraction
 * facilities. This is sufficient for basic types or simple POJOs but might be wrong for more
 * complex, custom, or composite types. In these cases {@link TypeInformation} of the result type
 * can be manually defined by overriding {@link ScalarFunction#getResultType}.
 *
 * <p>Internally, the Table/SQL API code generation works with primitive values as much as possible.
 * If a user-defined scalar function should not introduce much overhead during runtime, it is
 * recommended to declare parameters and result types as primitive types instead of their boxed
 * classes. <code>DATE/TIME</code> is equal to <code>int</code>, <code>TIMESTAMP</code> is equal
 * to <code>long</code>.
 */
@PublicEvolving
public abstract class ScalarFunction extends UserDefinedFunction {

	/**
	 * Returns the result type of the evaluation method with a given signature.
	 *
	 * <p>This method needs to be overridden in case Flink's type extraction facilities are not
	 * sufficient to extract the {@link TypeInformation} based on the return type of the evaluation
	 * method. Flink's type extraction facilities can handle basic types or
	 * simple POJOs but might be wrong for more complex, custom, or composite types.
	 *
	 * @param signature signature of the method the return type needs to be determined
	 * @return {@link TypeInformation} of result type or <code>null</code> if Flink should
	 *         determine the type
	 */
	public TypeInformation<?> getResultType(Class<?>[] signature) {
		return null;
	}

	/**
	 * Returns {@link TypeInformation} about the operands of the evaluation method with a given
	 * signature.
	 *
	 * <p>In order to perform operand type inference in SQL (especially when <code>NULL</code> is
	 * used) it might be necessary to determine the parameter {@link TypeInformation} of an
	 * evaluation method. By default Flink's type extraction facilities are used for this but might
	 * be wrong for more complex, custom, or composite types.
	 *
	 * @param signature signature of the method the operand types need to be determined
	 * @return {@link TypeInformation} of operand types
	 */
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
}
