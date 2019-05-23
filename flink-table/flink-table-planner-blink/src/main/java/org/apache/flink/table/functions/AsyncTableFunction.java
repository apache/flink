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

import org.apache.flink.api.common.functions.InvalidTypesException;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.table.api.ValidationException;

/**
 * Base class for a user-defined asynchronously table function (UDTF). This is similar to
 * {@link TableFunction} but this function is asynchronously.
 *
 * <p>A user-defined table functions works on
 * zero, one, or multiple scalar values as input and returns multiple rows as output.
 *
 * <p>The behavior of a {@link AsyncTableFunction} can be defined by implementing a custom evaluation
 * method. An evaluation method must be declared publicly, not static and named "eval".
 * Evaluation methods can also be overloaded by implementing multiple methods named "eval".
 *
 * <p>The first parameter of evaluation method must be {@link ResultFuture}, and the others are user
 * defined input parameters like the "eval" method of {@link TableFunction}.
 *
 * <p>For each "eval", an async io operation can be triggered, and once it has been done,
 * the result can be collected by calling {@link ResultFuture#complete}. For each async
 * operation, its context is stored in the operator immediately after invoking "eval",
 * avoiding blocking for each stream input as long as the internal buffer is not full.
 *
 * <p>{@link ResultFuture} can be passed into callbacks or futures to collect the result data.
 * An error can also be propagate to the async IO operator by
 * {@link ResultFuture#completeExceptionally(Throwable)}.
 *
 * <p>User-defined functions must have a default constructor and must be instantiable during
 * runtime.
 *
 * <p>By default the result type of an evaluation method is determined by Flink's type extraction
 * facilities. This is sufficient for basic types or simple POJOs but might be wrong for more
 * complex, custom, or composite types. In these cases {@link TypeInformation} of the result type
 * can be manually defined by overriding {@link #getResultType}.
 *
 * <p>Internally, the Table/SQL API code generation works with primitive values as much as possible.
 * If a user-defined table function should not introduce much overhead during runtime, it is
 * recommended to declare parameters and result types as primitive types instead of their boxed
 * classes. DATE/TIME is equal to int, TIMESTAMP is equal to long.
 *
 * <p>Example:
 *
 * {@code
 *
 *   public class HBaseAsyncTableFunction extends AsyncTableFunction<String> {
 *
 *     // implement an "eval" method with as many parameters as you want
 *     public void eval(ResultFuture<String> result, String rowkey) {
 *       Get get = new Get(Bytes.toBytes(rowkey));
 *       ListenableFuture<Result> future = hbase.asyncGet(get);
 *       Futures.addCallback(future, new FutureCallback<Result>() {
 *         public void onSuccess(Result result) {
 *           List<String> ret = process(result);
 *           result.complete(ret);
 *         }
 *         public void onFailure(Throwable thrown) {
 *           result.completeExceptionally(thrown);
 *         }
 *       });
 *     }
 *
 *     // you can overload the eval method here ...
 *   }
 * }
 *
 * <p>NOTE: the {@link AsyncTableFunction} is can not used as UDTF currently. It only used in
 * temporal table join as a async lookup function
 *
 * @param <T> The type of the output row
 */
public abstract class AsyncTableFunction<T> extends UserDefinedFunction {

	/**
	 * Returns the result type of the evaluation method with a given signature.
	 *
	 * <p>This method needs to be overridden in case Flink's type extraction facilities are not
	 * sufficient to extract the {@link TypeInformation} based on the return type of the evaluation
	 * method. Flink's type extraction facilities can handle basic types or
	 * simple POJOs but might be wrong for more complex, custom, or composite types.
	 *
	 * @return {@link TypeInformation} of result type or <code>null</code> if Flink should determine the type
	 */
	public TypeInformation<T> getResultType() {
		return null;
	}

	/**
	 * Returns {@link TypeInformation} about the operands of the evaluation method with a given
	 * signature.
	 *
	 * <p>In order to perform operand type inference in SQL (especially when NULL is used) it might be
	 * necessary to determine the parameter {@link TypeInformation} of an evaluation method.
	 * By default Flink's type extraction facilities are used for this but might be wrong for
	 * more complex, custom, or composite types.
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
					"Parameter types of table function " + this.getClass().getCanonicalName() +
						" cannot be automatically determined. Please provide type information manually.");
			}
		}
		return types;
	}
}
