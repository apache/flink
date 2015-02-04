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

package org.apache.flink.streaming.api.invokable.operator.windowing;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.invokable.operator.MapInvokable;

public class WindowMapper<IN, OUT> extends MapInvokable<StreamWindow<IN>, StreamWindow<OUT>> {

	private static final long serialVersionUID = 1L;

	GroupReduceFunction<IN, OUT> reducer;

	public WindowMapper(GroupReduceFunction<IN, OUT> reducer) {
		super(new WindowMapfunction<IN, OUT>(reducer));
		this.reducer = reducer;
	}

	private static class WindowMapfunction<T, R> implements
			MapFunction<StreamWindow<T>, StreamWindow<R>> {

		private static final long serialVersionUID = 1L;
		GroupReduceFunction<T, R> reducer;

		public WindowMapfunction(GroupReduceFunction<T, R> reducer) {
			this.reducer = reducer;
		}

		@Override
		public StreamWindow<R> map(StreamWindow<T> window) throws Exception {
			StreamWindow<R> outputWindow = new StreamWindow<R>(window.windowID);
			outputWindow.numberOfParts = window.numberOfParts;

			reducer.reduce(window, outputWindow);

			return outputWindow;
		}

	}

}