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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.common.functions.IterationRuntimeContext;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.functions.async.collector.AsyncCollector;

/**
 * Rich variant of the {@link AsyncFunction}. As a {@link RichFunction}, it gives access to the
 * {@link org.apache.flink.api.common.functions.RuntimeContext} and provides setup and teardown methods:
 * {@link RichFunction#open(org.apache.flink.configuration.Configuration)} and
 * {@link RichFunction#close()}.
 *
 * <p>
 * {@link RichAsyncFunction#getRuntimeContext()} and {@link RichAsyncFunction#getRuntimeContext()} are
 * not supported because the key may get changed while accessing states in the working thread.
 *
 * @param <IN> The type of the input elements.
 * @param <OUT> The type of the returned elements.
 */

@PublicEvolving
public abstract class RichAsyncFunction<IN, OUT> extends AbstractRichFunction
	implements AsyncFunction<IN, OUT> {

	@Override
	public abstract void asyncInvoke(IN input, AsyncCollector<OUT> collector) throws Exception;

	@Override
	public RuntimeContext getRuntimeContext() {
		throw new UnsupportedOperationException("Get runtime context is not supported in rich async function");
	}

	@Override
	public IterationRuntimeContext getIterationRuntimeContext() {
		throw new UnsupportedOperationException("Get iteration runtime context is not supported in rich async function");
	}
}
