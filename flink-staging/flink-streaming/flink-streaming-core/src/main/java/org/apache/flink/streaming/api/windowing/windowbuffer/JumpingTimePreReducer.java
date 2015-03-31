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

package org.apache.flink.streaming.api.windowing.windowbuffer;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.windowing.StreamWindow;
import org.apache.flink.streaming.api.windowing.helper.TimestampWrapper;
import org.apache.flink.util.Collector;

/**
 * Non-grouped pre-reducer for jumping time eviction policy
 * (the policies are based on time, and the slide size is larger than the window size).
 */
public class JumpingTimePreReducer<T> extends TumblingPreReducer<T> {

	private static final long serialVersionUID = 1L;

	private TimestampWrapper<T> timestampWrapper;
	protected long windowStartTime;
	private long slideSize;

	public JumpingTimePreReducer(ReduceFunction<T> reducer, TypeSerializer<T> serializer,
								long slideSize, long windowSize, TimestampWrapper<T> timestampWrapper){
		super(reducer, serializer);
		this.timestampWrapper = timestampWrapper;
		this.windowStartTime = timestampWrapper.getStartTime() + slideSize - windowSize;
		this.slideSize = slideSize;
	}

	@Override
	public void emitWindow(Collector<StreamWindow<T>> collector) {
		super.emitWindow(collector);
		windowStartTime += slideSize;
	}

	public void store(T element) throws Exception {
		if(timestampWrapper.getTimestamp(element) >= windowStartTime) {
			super.store(element);
		}
	}
}
