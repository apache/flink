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

package org.apache.flink.table.api.functions;

import org.apache.flink.table.api.types.DataType;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ScalarFunctionCall;

import scala.collection.Seq;

/**
 * Base class for a user-defined scalar function. A user-defined scalar functions maps zero, one,
 * or multiple scalar values to a new scalar value.
 *
 * <p>The behavior of a {@link ScalarFunction} can be defined by implementing a custom evaluation
 * method. An evaluation method must be declared publicly and named "eval". Evaluation methods
 * can also be overloaded by implementing multiple methods named "eval".
 *
 * <p>User-defined functions must have a default constructor and must be instantiable during
 * runtime.
 *
 * <p>By default the result type of an evaluation method is determined by Flink's type extraction
 * facilities. This is sufficient for basic types or simple POJOs but might be wrong for more
 * complex, custom, or composite types. In these cases {@link DataType} of the result type
 * can be manually defined by overriding {@link #getResultType}.
 *
 * <p>Internally, the Table/SQL API code generation works with primitive values as much as possible.
 * If a user-defined scalar function should not introduce much overhead during runtime, it is
 * recommended to declare parameters and result types as primitive types instead of their boxed
 * classes. DATE/TIME is equal to int, TIMESTAMP is equal to long.
 */
public abstract class ScalarFunction extends CustomTypeDefinedFunction {

	/**
	 * Creates a call to a {@link ScalarFunction} in Scala Table API.
	 *
	 * @param params actual parameters of function
	 * @return {@link Expression} in form of a {@link ScalarFunctionCall}
	 */
	public final Expression apply(Expression... params) {
		return ScalarFunctionCall.apply(this, params);
	}

	public final Expression apply(Seq<Expression> params) {
		return new ScalarFunctionCall(this, params);
	}
}
