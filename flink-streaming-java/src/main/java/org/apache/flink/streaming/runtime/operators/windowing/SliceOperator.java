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

package org.apache.flink.streaming.runtime.operators.windowing;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.state.AppendingState;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SliceAssigner;
import org.apache.flink.streaming.api.windowing.slicing.Slice;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalSingleValueWindowFunction;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalWindowFunction;
import org.apache.flink.util.OutputTag;

/**
 * An operator that implements the logic for windowing based on a {@link SliceAssigner} and
 * {@link Trigger}.
 *
 * <p>Unlike {@link WindowOperator}, each element arrived at the slice operator only gets
 * assigned to zero or one unique window segments.
 *
 * @param <K> The type of key returned by the {@code KeySelector}.
 * @param <IN> The type of the incoming elements.
 * @param <ACC> The type of elements emitted by the window {@code StateDescriptor}
 * @param <W> The type of {@code Window} that the {@code WindowAssigner} assigns.
 */
@Internal
public class SliceOperator<K, IN, ACC, W extends Window>
	extends WindowOperator<K, IN, ACC, Slice<ACC, K, W>, W> {

	/**
	 * Creates a new {@code WindowOperator} based on the given policies and user functions.
	 */
	public SliceOperator(
		SliceAssigner<? super IN, W> sliceAssigner,
		TypeSerializer<W> windowSerializer,
		KeySelector<IN, K> keySelector,
		TypeSerializer<K> keySerializer,
		StateDescriptor<? extends AppendingState<IN, ACC>, ?> windowStateDescriptor,
		Trigger<? super IN, ? super W> trigger,
		long allowedLateness,
		OutputTag<IN> lateDataOutputTag) {
		this(sliceAssigner,
			windowSerializer,
			keySelector,
			keySerializer,
			windowStateDescriptor,
			new InternalSingleValueWindowFunction<>(
				(WindowFunction<ACC, Slice<ACC, K, W>, K, W>) (key, window, input, out) -> {
					for (ACC val : input) {
						out.collect(new Slice<>(val, key, window));
					}
				}),
			trigger,
			allowedLateness,
			lateDataOutputTag);
	}

	SliceOperator(
		SliceAssigner<? super IN, W> sliceAssigner,
		TypeSerializer<W> windowSerializer,
		KeySelector<IN, K> keySelector,
		TypeSerializer<K> keySerializer,
		StateDescriptor<? extends AppendingState<IN, ACC>, ?> windowStateDescriptor,
		InternalWindowFunction<ACC, Slice<ACC, K, W>, K, W> windowFunction,
		Trigger<? super IN, ? super W> trigger,
		long allowedLateness,
		OutputTag<IN> lateDataOutputTag) {
		super(sliceAssigner,
			windowSerializer,
			keySelector,
			keySerializer,
			windowStateDescriptor,
			windowFunction,
			trigger,
			allowedLateness,
			lateDataOutputTag);

	}
}
