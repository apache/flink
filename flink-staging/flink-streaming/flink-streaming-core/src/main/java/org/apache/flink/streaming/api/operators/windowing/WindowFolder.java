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
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.streaming.api.datastream.WindowedDataStream;
import org.apache.flink.streaming.api.operators.StreamMap;
import org.apache.flink.streaming.api.windowing.StreamWindow;

/**
 * This operator is used to apply foldWindow transformations on
 * {@link WindowedDataStream}s.
 */
public class WindowFolder<IN, OUT> extends StreamMap<StreamWindow<IN>, StreamWindow<OUT>> {

	private static final long serialVersionUID = 1L;

	FoldFunction<IN, OUT> folder;

	public WindowFolder(FoldFunction<IN, OUT> folder, OUT initialValue) {
		super(new WindowFoldFunction<IN, OUT>(folder, initialValue));
		this.folder = folder;
	}

	private static class WindowFoldFunction<IN, OUT> extends AbstractRichFunction implements
			MapFunction<StreamWindow<IN>, StreamWindow<OUT>> {

		private static final long serialVersionUID = 1L;
		private OUT initialValue;
		FoldFunction<IN, OUT> folder;

		public WindowFoldFunction(FoldFunction<IN, OUT> folder, OUT initialValue) {
			this.folder = folder;
			this.initialValue = initialValue;
		}

		@Override
		public StreamWindow<OUT> map(StreamWindow<IN> window) throws Exception {
			StreamWindow<OUT> outputWindow = new StreamWindow<OUT>(window.windowID);
			outputWindow.numberOfParts = window.numberOfParts;

			if (!window.isEmpty()) {
				OUT accumulator = initialValue;
				for (int i = 0; i < window.size(); i++) {
					accumulator = folder.fold(accumulator, window.get(i));
				}
				outputWindow.add(accumulator);
			}
			return outputWindow;
		}

		@Override
		public void setRuntimeContext(RuntimeContext t) {
			FunctionUtils.setFunctionRuntimeContext(folder, t);
		}

	}

}
