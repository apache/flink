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
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.extraction.TypeInferenceExtractor;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.types.Row;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

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
 * <p>Optionally, a custom timeout handler can be defined by convention: when an async invocation
 * exceeds the configured timeout, the framework invokes the matching <code>timeout</code> method to
 * let the function supply a fallback result (via {@link CompletableFuture#complete}) or surface a
 * domain-specific error (via {@link CompletableFuture#completeExceptionally(Throwable)}) instead of
 * the default {@link TimeoutException}.
 *
 * <p>A <code>timeout</code> method must satisfy <em>all</em> of the following constraints:
 *
 * <ul>
 *   <li><b>Declaration.</b> Declared publicly, not static, and named <code>timeout</code>.
 *   <li><b>Signature parity with <code>eval</code>.</b> The parameter list mirrors the matching
 *       <code>eval</code>: the first parameter is a {@link CompletableFuture} with the
 *       <em>same</em> generic type as in <code>eval</code>; the remaining parameters are the lookup
 *       keys with the <em>same</em> types and order. Overloads are supported — declare one <code>
 *       timeout</code> per <code>eval</code> overload that needs a fallback.
 *   <li><b>Synchronous completion (enforced).</b> The handler must complete the future <em>before
 *       it returns</em>. It is invoked on the operator's mailbox thread, so blocking or scheduling
 *       additional async work there would stall the entire operator; the framework checks {@code
 *       future.isDone()} immediately after the call and, if it's not, forces the future to complete
 *       with an {@link IllegalStateException}. Concretely:
 *       <ul>
 *         <li>Do <em>not</em> issue another async call (e.g. a retry, a secondary lookup) and rely
 *             on its callback to complete the future — by the time the callback fires, the
 *             framework has already short-circuited this record with the IllegalStateException
 *             above.
 *         <li>Do <em>not</em> spawn a thread that completes the future asynchronously, for the same
 *             reason.
 *         <li>The body should be a pure, cheap fallback: a constant row, a NULL row, an empty
 *             collection, or {@code completeExceptionally(...)} with a user-defined exception.
 *       </ul>
 *   <li><b>Exception transparency.</b> Throwing synchronously from the body is safe: the throw
 *       propagates up the framework's outer catch around the {@code timeout(...)} dispatch and is
 *       forwarded to the downstream {@code ResultFuture}, equivalent in effect to calling {@code
 *       future.completeExceptionally(thrown)}. You do not need to wrap the body in try/catch
 *       yourself.
 * </ul>
 *
 * <p>Error and dispatch behavior the framework guarantees on top of the constraints above:
 *
 * <ul>
 *   <li><b>Absent handler →</b> the default {@link TimeoutException} fires; no codegen failure.
 *   <li><b>Incompatible signature →</b> if a <code>timeout</code> method exists with valid
 *       visibility but its parameter list is not assignable from the current call site's lookup-key
 *       types, the framework fails fast during planning (code generation) with a {@link
 *       org.apache.flink.table.api.ValidationException} whose message includes the function's fully
 *       qualified class name, the expected signature, and the actual signatures found.
 *   <li><b>Overload resolution →</b> when multiple <code>timeout</code> overloads are declared, the
 *       one matching the current call site's lookup-key types is dispatched (decoy overloads with
 *       incompatible types or arity are tolerated, never invoked).
 *   <li><b>Empty-collection fallback →</b> {@code future.complete(emptyList())} drops the row for
 *       an INNER lookup join and pads the right side with NULL for a LEFT OUTER lookup join.
 * </ul>
 *
 * <p>For storing a user-defined function in a catalog, the class must have a default constructor
 * and must be instantiable during runtime. Anonymous functions in Table API can only be persisted
 * if the function is not stateful (i.e. containing only transient and static fields).
 *
 * <p>The following example shows how to perform an asynchronous remote model invocation, with a
 * fallback handler provided via the <code>timeout</code> convention:
 *
 * <pre>{@code
 * public class RemoteModelAsyncTableFunction extends AsyncTableFunction<RowData> {
 *
 *   private transient RemoteModelClient client;
 *
 *   public void open(FunctionContext context) {
 *     client = new RemoteModelClient(...);
 *   }
 *
 *   // implement an "eval" method that takes a CompletableFuture as the first parameter
 *   // and ends with as many parameters as you want
 *   public void eval(CompletableFuture<Collection<RowData>> result, String prompt) {
 *     CompletableFuture<String> modelFuture = client.predictAsync(prompt);
 *     modelFuture.whenComplete((response, throwable) -> {
 *       if (throwable != null) {
 *         result.completeExceptionally(throwable);
 *       } else {
 *         result.complete(
 *             Collections.singletonList(GenericRowData.of(StringData.fromString(response))));
 *       }
 *     });
 *   }
 *
 *   // implement a "timeout" method whose parameter list mirrors the matching "eval" method.
 *   // The body MUST complete the future synchronously — do NOT call client.predictAsync(...)
 *   // again or spawn a thread to complete the future, because the operator has already
 *   // abandoned this record by the time this handler runs.
 *   public void timeout(CompletableFuture<Collection<RowData>> result, String prompt) {
 *     result.complete(
 *         Collections.singletonList(GenericRowData.of(StringData.fromString("FALLBACK"))));
 *   }
 *
 *   // you can overload the eval/timeout methods here ...
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
