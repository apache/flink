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
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.streaming.api.datastream.WindowedDataStream;
import org.apache.flink.streaming.api.functions.WindowMapFunction;
import org.apache.flink.streaming.api.operators.StreamMap;
import org.apache.flink.streaming.api.windowing.StreamWindow;

/**
 * This operator is used to apply mapWindow transformations on
 * {@link WindowedDataStream}s.
 */
public class WindowMapper<IN, OUT> extends StreamMap<StreamWindow<IN>, StreamWindow<OUT>> {

	private static final long serialVersionUID = 1L;

	WindowMapFunction<IN, OUT> mapper;

	public WindowMapper(WindowMapFunction<IN, OUT> mapper) {
		super(new WindowMap<IN, OUT>(mapper));
		this.mapper = mapper;
	}

	private static class WindowMap<T, R> extends AbstractRichFunction
			implements MapFunction<StreamWindow<T>, StreamWindow<R>> {

		private static final long serialVersionUID = 1L;
		WindowMapFunction<T, R> mapper;

		public WindowMap(WindowMapFunction<T, R> mapper) {
			this.mapper = mapper;
		}

		@Override
		public StreamWindow<R> map(StreamWindow<T> window) throws Exception {
			StreamWindow<R> outputWindow = new StreamWindow<R>(window.windowID);

			outputWindow.numberOfParts = window.numberOfParts;

			mapper.mapWindow(window, outputWindow);

			return outputWindow;
		}

		@Override
		public void setRuntimeContext(RuntimeContext t) {
			FunctionUtils.setFunctionRuntimeContext(mapper, t);
		}

	}

}
