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

package org.apache.flink.streaming.api.functions.async;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.streaming.api.operators.async.AsyncCollector;

import java.io.Serializable;

/**
 * A function to trigger Async I/O operation.
 * <p>
 * Typical usage for callback:
 * <pre>{@code
 * public class HBaseAsyncFunc implements AsyncFunction<String, String> {
 *   @Override
 *   public void asyncInvoke(String row, AsyncCollector<String> collector) throws Exception {
 *     HBaseCallback cb = new HBaseCallback(collector);
 *     Get get = new Get(Bytes.toBytes(row));
 *     hbase.asyncGet(get, cb);
 *   }
 * }
 * }
 * </pre>
 * <p>
 * Typical usage for {@link com.google.common.util.concurrent.ListenableFuture}
 * <pre>{@code
 * public class HBaseAsyncFunc implements AsyncFunction<String, String> {
 *   @Override
 *   public void asyncInvoke(String row, final AsyncCollector<String> collector) throws Exception {
 *     Get get = new Get(Bytes.toBytes(row));
 *     ListenableFuture<Result> future = hbase.asyncGet(get);
 *     Futures.addCallback(future, new FutureCallback<Result>() {
 *       public void onSuccess(Result result) {
 *         List<String> ret = process(result);
 *         collector.collect(ret);
 *       }
 *       public void onFailure(Throwable thrown) {
 *         collector.collect(thrown);
 *       }
 *     });
 *   }
 * }
 * }
 * </pre>
 *
 * @param <IN> The type of the input elements.
 * @param <OUT> The type of the returned elements.
 */
public interface AsyncFunction<IN, OUT> extends Function, Serializable {
	/**
	 * Trigger async operation for each stream input.
	 *
	 * @param input Stream Input
	 * @param collector AsyncCollector
	 * @exception Exception will make task fail and trigger fail-over process.
	 */
	void asyncInvoke(IN input, AsyncCollector<IN, OUT> collector) throws Exception;
}
