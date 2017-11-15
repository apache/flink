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

package org.apache.flink.api.java.operators.translation;

import org.apache.flink.api.common.functions.CombineFunction;
import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.RichGroupCombineFunction;
import org.apache.flink.api.common.functions.RichGroupReduceFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

/**
 * A wrapper the wraps a function that implements both {@link CombineFunction} and {@link GroupReduceFunction} interfaces
 * and makes it look like a function that implements {@link GroupCombineFunction} and {@link GroupReduceFunction} to the runtime.
 */
public class RichCombineToGroupCombineWrapper<IN, OUT, F extends RichGroupReduceFunction<IN, OUT> & CombineFunction<IN, IN>>
	extends RichGroupCombineFunction<IN, IN> implements GroupReduceFunction<IN, OUT> {

	private final F wrappedFunction;

	public RichCombineToGroupCombineWrapper(F wrappedFunction) {
		this.wrappedFunction = Preconditions.checkNotNull(wrappedFunction);
	}

	@Override
	public void open(Configuration config) throws Exception {
		wrappedFunction.setRuntimeContext(getRuntimeContext());
		wrappedFunction.open(config);
	}

	@Override
	public void combine(Iterable<IN> values, Collector<IN> out) throws Exception {
		IN outValue = wrappedFunction.combine(values);
		out.collect(outValue);
	}

	@Override
	public void reduce(Iterable<IN> values, Collector<OUT> out) throws Exception {
		wrappedFunction.reduce(values, out);
	}
}
