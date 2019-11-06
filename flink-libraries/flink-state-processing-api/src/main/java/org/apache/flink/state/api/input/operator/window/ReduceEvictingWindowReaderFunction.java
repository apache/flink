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

package org.apache.flink.state.api.input.operator.window;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.state.api.functions.WindowReaderFunction;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.Collections;

/**
 * A wrapper function for reading state from an evicting window operator with a reduce function.
 */
@Internal
public class ReduceEvictingWindowReaderFunction<IN, OUT, KEY, W extends Window> extends EvictingWindowReaderFunction<IN, IN, OUT, KEY, W> {

	private final ReduceFunction<IN> reduceFunction;

	public ReduceEvictingWindowReaderFunction(WindowReaderFunction<IN, OUT, KEY, W> wrappedFunction, ReduceFunction<IN> reduceFunction) {
		super(wrappedFunction);
		this.reduceFunction = reduceFunction;
	}

	@Override
	public Iterable<IN> transform(Iterable<StreamRecord<IN>> elements) throws Exception {
		IN curr = null;

		for (StreamRecord<IN> element : elements) {
			if (curr == null) {
				curr = element.getValue();
			} else {
				curr = reduceFunction.reduce(curr, element.getValue());
			}
		}

		return Collections.singletonList(curr);
	}
}
