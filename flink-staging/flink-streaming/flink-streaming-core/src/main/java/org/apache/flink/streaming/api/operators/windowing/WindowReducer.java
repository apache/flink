/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.operators.windowing;

import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.streaming.api.datastream.WindowedDataStream;
import org.apache.flink.streaming.api.operators.StreamMap;
import org.apache.flink.streaming.api.windowing.StreamWindow;

/**
 * This operator is used to apply reduceWindow transformations on
 * {@link WindowedDataStream}s.
 */
public class WindowReducer<IN> extends StreamMap<StreamWindow<IN>, StreamWindow<IN>> {

	private static final long serialVersionUID = 1L;

	ReduceFunction<IN> reducer;

	public WindowReducer(ReduceFunction<IN> reducer) {
		super(new WindowReduceFunction<IN>(reducer));
		this.reducer = reducer;
	}

	private static class WindowReduceFunction<T> extends AbstractRichFunction implements
			MapFunction<StreamWindow<T>, StreamWindow<T>> {

		private static final long serialVersionUID = 1L;
		ReduceFunction<T> reducer;

		public WindowReduceFunction(ReduceFunction<T> reducer) {
			this.reducer = reducer;
		}

		@Override
		public StreamWindow<T> map(StreamWindow<T> window) throws Exception {
			StreamWindow<T> outputWindow = new StreamWindow<T>(window.windowID);
			outputWindow.numberOfParts = window.numberOfParts;

			if (!window.isEmpty()) {
				T reduced = window.get(0);
				for (int i = 1; i < window.size(); i++) {
					reduced = reducer.reduce(reduced, window.get(i));
				}
				outputWindow.add(reduced);
			}
			return outputWindow;
		}

		@Override
		public void setRuntimeContext(RuntimeContext t) {
			FunctionUtils.setFunctionRuntimeContext(reducer, t);
		}

	}

}
