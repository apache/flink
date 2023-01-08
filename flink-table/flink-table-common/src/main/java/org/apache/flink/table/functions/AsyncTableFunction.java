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
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.extraction.TypeInferenceExtractor;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.types.Row;

import java.util.concurrent.CompletableFuture;

/**
 * Base class for a user-defined asynchronous table function. A user-defined asynchronous table
 * function maps zero, one, or multiple scalar values to zero, one, or multiple rows (or structured
 * types).
 *
 * <p>This kind of function is similar to {@link TableFunction} but is executed asynchronously.
 *
 * <p>The behavior of a {@link AsyncTableFunction} can be defined by implementing a custom
 * evaluation method. An evaluation method must be declared publicly, not static, and named <code>
 * eval</code>. Evaluation methods can also be overloaded by implementing multiple methods named
 * <code>eval</code>.
 *
 * <p>By default, input and output data types are automatically extracted using reflection. This
 * includes the generic argument {@code T} of the class for determining an output data type. Input
 * arguments are derived from one or more {@code eval()} methods. If the reflective information is
 * not sufficient, it can be supported and enriched with {@link DataTypeHint} and {@link
 * FunctionHint} annotations. See {@link TableFunction} for more examples how to annotate an
 * implementation class.
 *
 * <p>Note: Currently, asynchronous table functions are only supported as the runtime implementation
 * of {@link LookupTableSource}s for performing temporal joins. By default, input and output {@link
 * DataType}s of {@link AsyncTableFunction} are derived similar to other {@link
 * UserDefinedFunction}s using the logic above. However, for convenience, in a {@link
 * LookupTableSource} the output type can simply be a {@link Row} or {@link RowData} in which case
 * the input and output types are derived from the table's schema with default conversion.
 *
 * <p>The first parameter of the evaluation method must be a {@link CompletableFuture}. Other
 * parameters specify user-defined input parameters like the "eval" method of {@link TableFunction}.
 * The generic type of {@link CompletableFuture} must be {@link java.util.Collection} to collect
 * multiple possible result values.
 *
 * <p>For each call to <code>eval()</code>, an async IO operation can be triggered, and once the
 * operation has been done, the result can be collected by calling {@link
 * CompletableFuture#complete}. For each async operation, its context is stored in the operator
 * immediately after invoking <code>eval()</code>, avoiding blocking for each stream input as long
 * as the internal buffer is not full.
 *
 * <p>{@link CompletableFuture} can be passed into callbacks or futures to collect the result data.
 * An error can also be propagated to the async IO operator by calling {@link
 * CompletableFuture#completeExceptionally(Throwable)}.
 *
 * <p>For storing a user-defined function in a catalog, the class must have a default constructor
 * and must be instantiable during runtime. Anonymous functions in Table API can only be persisted
 * if the function is not stateful (i.e. containing only transient and static fields).
 *
 * <p>The following example shows how to perform an asynchronous request to Apache HBase:
 *
 * <pre>{@code
 * public class HBaseAsyncTableFunction extends AsyncTableFunction<Row> {
 *
 *   // implement an "eval" method that takes a CompletableFuture as the first parameter
 *   // and ends with as many parameters as you want
 *   public void eval(CompletableFuture<Collection<Row>> result, String rowkey) {
 *     Get get = new Get(Bytes.toBytes(rowkey));
 *     ListenableFuture<Result> future = hbase.asyncGet(get);
 *     Futures.addCallback(future, new FutureCallback<Result>() {
 *       public void onSuccess(Result hbaseResult) {
 *         List<Row> ret = process(hbaseResult);
 *         result.complete(ret);
 *       }
 *       public void onFailure(Throwable thrown) {
 *         result.completeExceptionally(thrown);
 *       }
 *     });
 *   }
 *
 *   // you can overload the eval method here ...
 * }
 * }</pre>
 *
 * @param <T> The type of the output row used during reflective extraction.
 */
@PublicEvolving
public abstract class AsyncTableFunction<T> extends UserDefinedFunction {

    @Override
    public final FunctionKind getKind() {
        return FunctionKind.ASYNC_TABLE;
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
        return TypeInferenceExtractor.forAsyncTableFunction(typeFactory, (Class) getClass());
    }
}
