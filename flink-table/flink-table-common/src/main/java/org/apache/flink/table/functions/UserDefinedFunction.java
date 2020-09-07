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
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.utils.EncodingUtils;

import java.io.Serializable;

/**
 * Base class for all user-defined functions.
 *
 * <p>User-defined functions combine the logical definition of a function for validation and planning
 * (see {@link FunctionDefinition}) and contain a corresponding runtime implementation.
 *
 * <p>A runtime implementation might be called at two different stages:
 * <ul>
 *     <li>During planning (i.e. pre-flight phase): If a function is called with constant expressions
 *     or constant expressions can be derived from the given statement, a function is pre-evaluated
 *     for constant expression reduction and might not be executed on the cluster anymore. Use
 *     {@link #isDeterministic()} to disable constant expression reduction in this case. For example,
 *     the following calls to {@code ABS} are executed during planning:
 *     {@code SELECT ABS(-1) FROM t} and {@code SELECT ABS(field) FROM t WHERE field = -1}.
 *     <li>During runtime (i.e. cluster execution): If a function is called with non-constant expressions
 *     or {@link #isDeterministic()} returns false.
 * </ul>
 *
 * @see ScalarFunction
 * @see TableFunction
 * @see AsyncTableFunction
 * @see AggregateFunction
 * @see TableAggregateFunction
 */
@PublicEvolving
public abstract class UserDefinedFunction implements FunctionDefinition, Serializable {

	/**
	 * Returns a unique, serialized representation for this function.
	 */
	public final String functionIdentifier() {
		final String md5 = EncodingUtils.hex(EncodingUtils.md5(EncodingUtils.encodeObjectToString(this)));
		return getClass().getName().replace('.', '$').concat("$").concat(md5);
	}

	/**
	 * Setup method for user-defined function. It can be used for initialization work.
	 * By default, this method does nothing.
	 */
	public void open(FunctionContext context) throws Exception {
		// do nothing
	}

	/**
	 * Tear-down method for user-defined function. It can be used for clean up work.
	 * By default, this method does nothing.
	 */
	public void close() throws Exception {
		// do nothing
	}

	/**
	 * {@inheritDoc}
	 *
	 * <p>The type inference for user-defined functions is automatically extracted using reflection. It
	 * does this by analyzing implementation methods such as {@code eval() or accumulate()} and the generic
	 * parameters of a function class if present. If the reflective information is not sufficient, it can
	 * be supported and enriched with {@link DataTypeHint} and {@link FunctionHint} annotations.
	 *
	 * <p>Note: Overriding this method is only recommended for advanced users. If a custom type inference
	 * is specified, it is the responsibility of the implementer to make sure that the output of the type
	 * inference process matches with the implementation method:
	 *
	 * <p>The implementation method must comply with each {@link DataType#getConversionClass()} returned
	 * by the type inference. For example, if {@code DataTypes.TIMESTAMP(3).bridgedTo(java.sql.Timestamp.class)}
	 * is an expected argument type, the method must accept a call {@code eval(java.sql.Timestamp)}.
	 *
	 * <p>Regular Java calling semantics (including type widening and autoboxing) are applied when calling
	 * an implementation method which means that the signature can be {@code eval(java.lang.Object)}.
	 *
	 * <p>The runtime will take care of converting the data to the data format specified by the
	 * {@link DataType#getConversionClass()} coming from the type inference logic.
	 */
	@Override
	public abstract TypeInference getTypeInference(DataTypeFactory typeFactory);

	/**
	 * Returns the name of the UDF that is used for plan explanation and logging.
	 */
	@Override
	public String toString() {
		return getClass().getSimpleName();
	}
}
