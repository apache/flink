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
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.types.extraction.TypeInferenceExtractor;
import org.apache.flink.table.types.inference.TypeInference;

/**
 * Base class for a user-defined scalar function which returns results asynchronously. A
 * user-defined scalar function maps zero, one, or multiple scalar values to a new scalar value.
 *
 * <p>The behavior of a {@link AsyncScalarFunction} can be defined by implementing a custom
 * evaluation method. An evaluation method must be declared publicly and named <code>eval</code>.
 * Evaluation methods can also be overloaded by implementing multiple methods named <code>eval
 * </code>. The first argument of the method must be a {@link
 * java.util.concurrent.CompletableFuture} of the return type. The method body must complete the
 * future either if there is a result or if it completes exceptionally.
 *
 * <p>By default, input and output data types are automatically extracted using reflection. If the
 * reflective information is not sufficient, it can be supported and enriched with {@link
 * org.apache.flink.table.annotation.DataTypeHint} and {@link
 * org.apache.flink.table.annotation.FunctionHint} annotations.
 *
 * <p>The following examples show how to specify an async scalar function:
 *
 * <pre>{@code
 * // a function that accepts two INT arguments and computes a sum
 * class SumFunction extends AsyncScalarFunction {
 *   public void eval(CompletableFuture<Integer> future, Integer a, Integer b) {
 *     return future.complete(a + b);
 *   }
 * }
 *
 * // a function that accepts either INT NOT NULL or BOOLEAN NOT NULL and computes a STRING
 * class StringifyFunction extends AsyncScalarFunction {
 *   public void eval(CompletableFuture<String> future, int i) {
 *     return future.complete(String.valueOf(i));
 *   }
 *   public void eval(CompletableFuture<String> future, boolean b) {
 *     return future.complete(String.valueOf(b));
 *   }
 * }
 *
 * // a function that accepts either INT or BOOLEAN and computes a STRING using function hints
 * @FunctionHint(input = [@DataTypeHint("INT")])
 * @FunctionHint(input = [@DataTypeHint("BOOLEAN")])
 * class StringifyFunction extends AsyncScalarFunction {
 *   public void eval(CompletableFuture<String> future, Object o) {
 *     return future.complete(o.toString());
 *   }
 * }
 *
 * // a function that accepts any data type as argument and computes a STRING
 * class StringifyFunction extends AsyncScalarFunction {
 *   public void eval(CompletableFuture<String> future,
 *                    @DataTypeHint(inputGroup = InputGroup.ANY) Object o) {
 *     return future.complete(o.toString());
 *   }
 * }
 *
 * // a function that accepts an arbitrary number of BIGINT values and computes a DECIMAL(10, 4)
 * class SumFunction extends AsyncScalarFunction {
 *   public void eval(@DataTypeHint("DECIMAL(10, 4)") CompletableFuture<BigDecimal> future,
 *                    Long... values) {
 *     // ...
 *   }
 * }
 * }</pre>
 *
 * <p>For storing a user-defined function in a catalog, the class must have a default constructor
 * and must be instantiable during runtime. Anonymous functions in Table API can only be persisted
 * if the function is not stateful (i.e. containing only transient and static fields).
 */
@PublicEvolving
public class AsyncScalarFunction extends UserDefinedFunction {
    @Override
    public final FunctionKind getKind() {
        return FunctionKind.ASYNC_SCALAR;
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
        return TypeInferenceExtractor.forAsyncScalarFunction(typeFactory, getClass());
    }
}
