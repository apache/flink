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
import org.apache.flink.util.Collector;

/**
 * Base class for a user-defined table function. A user-defined table function maps zero, one, or
 * multiple scalar values to zero, one, or multiple rows (or structured types). If an output record
 * consists of only one field, the structured record can be omitted, and a scalar value can be emitted
 * that will be implicitly wrapped into a row by the runtime.
 *
 * <p>The behavior of a {@link TableFunction} can be defined by implementing a custom evaluation
 * method. An evaluation method must be declared publicly, not static, and named <code>eval</code>.
 * Evaluation methods can also be overloaded by implementing multiple methods named <code>eval</code>.
 *
 * <p>By default, input and output data types are automatically extracted using reflection. This includes
 * the generic argument {@code T} of the class for determining an output data type. Input arguments are
 * derived from one or more {@code eval()} methods. If the reflective information is not sufficient, it
 * can be supported and enriched with {@link DataTypeHint} and {@link FunctionHint} annotations.
 *
 * <p>The following examples show how to specify a table function:
 *
 * <pre>
 * {@code
 *   // a function that accepts an arbitrary number of INT arguments and emits them as implicit ROW < INT >
 *   class FlattenFunction extends TableFunction<Integer> {
 *     public void eval(Integer... args) {
 *       for (Integer i : args) {
 *         collect(i);
 *       }
 *     }
 *   }
 *
 *   // a function that accepts either INT or STRING and emits them as implicit ROW < STRING >
 *   class DuplicatorFunction extends TableFunction<String> {
 *     public void eval(Integer i) {
 *       eval(String.valueOf(i));
 *     }
 *     public void eval(String s) {
 *       collect(s);
 *       collect(s);
 *     }
 *   }
 *
 *   // a function that produces a ROW < i INT, s STRING > from arguments, the function hint helps in
 *   // declaring the row's fields
 *   @FunctionHint(output = @DataTypeHint("ROW< i INT, s STRING >"))
 *   class DuplicatorFunction extends TableFunction<Row> {
 *     public void eval(Integer i, String s) {
 *       collect(Row.of(i, s));
 *       collect(Row.of(i, s));
 *     }
 *   }
 *
 *   // a function that accepts either INT or DECIMAL(10, 4) and emits them as implicit ROW < INT > or
 *   // ROW<DECIMAL(10, 4)> using function hints for declaring the output type
 *   class DuplicatorFunction extends TableFunction<Object> {
 *     @FunctionHint(output = @DataTypeHint("INT"))
 *     public void eval(Integer i) {
 *       collect(i);
 *       collect(i);
 *     }
 *     @FunctionHint(output = @DataTypeHint("DECIMAL(10, 4)"))
 *     public void eval(@DataTypeHint("DECIMAL(10, 4)") BigDecimal d) {
 *       collect(d);
 *       collect(d);
 *     }
 *   }
 * }
 * </pre>
 *
 * <p>For storing a user-defined function in a catalog, the class must have a default constructor and
 * must be instantiable during runtime.
 *
 * <p>In the API, a table function can be used as follows:
 *
 * <pre>
 * {@code
 *   public class Split extends TableFunction<String> {
 *
 *     // implement an "eval" method with as many parameters as you want
 *     public void eval(String str) {
 *       for (String s : str.split(" ")) {
 *         collect(s);   // use collect(...) to emit an output row
 *       }
 *     }
 *
 *     // you can overload the eval method here ...
 *   }
 *
 *   TableEnvironment tEnv = ...
 *   Table table = ...    // schema: ROW< a VARCHAR >
 *
 *   // for Scala users
 *   table.joinLateral(call(classOf[Split], $"a") as ("s")).select($"a", $"s")
 *
 *   // for Java users
 *   table.joinLateral(call(Split.class, $("a")).as("s")).select($("a"), $("s"));
 *
 *   // for SQL users
 *   tEnv.createTemporarySystemFunction("split", Split.class); // register table function first
 *   tEnv.sqlQuery("SELECT a, s FROM MyTable, LATERAL TABLE(split(a)) as T(s)");
 * }
 * </pre>
 *
 * @param <T> The type of the output row. Either an explicit composite type or an atomic type that is
 *            implicitly wrapped into a row consisting of one field.
 */
@PublicEvolving
public abstract class TableFunction<T> extends UserDefinedFunction {

	/**
	 * The code generated collector used to emit rows.
	 */
	private Collector<T> collector;

	/**
	 * Internal use. Sets the current collector.
	 */
	public final void setCollector(Collector<T> collector) {
		this.collector = collector;
	}

	/**
	 * Returns the result type of the evaluation method.
	 *
	 * @deprecated This method uses the old type system and is based on the old reflective extraction
	 *             logic. The method will be removed in future versions and is only called when using
	 *             the deprecated {@code TableEnvironment.registerFunction(...)} method. The new reflective
	 *             extraction logic (possibly enriched with {@link DataTypeHint} and {@link FunctionHint})
	 *             should be powerful enough to cover most use cases. For advanced users, it is possible
	 *             to override {@link UserDefinedFunction#getTypeInference(DataTypeFactory)}.
	 */
	@Deprecated
	public TypeInformation<T> getResultType() {
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
					"Parameter types of table function " + this.getClass().getCanonicalName() +
					" cannot be automatically determined. Please provide type information manually.");
			}
		}
		return types;
	}

	/**
	 * Emits an (implicit or explicit) output row.
	 *
	 * <p>If null is emitted as an explicit row, it will be skipped by the runtime. For implicit rows,
	 * the row's field will be null.
	 *
	 * @param row the output row
	 */
	protected final void collect(T row) {
		collector.collect(row);
	}

	@Override
	public final FunctionKind getKind() {
		return FunctionKind.TABLE;
	}

	@Override
	@SuppressWarnings({"unchecked", "rawtypes"})
	public TypeInference getTypeInference(DataTypeFactory typeFactory) {
		return TypeInferenceExtractor.forTableFunction(typeFactory, (Class) getClass());
	}
}
