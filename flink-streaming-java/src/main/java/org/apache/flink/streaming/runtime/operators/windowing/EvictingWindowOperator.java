/**
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package org.apache.flink.streaming.runtime.operators.windowing;

import com.google.common.annotations.VisibleForTesting;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.runtime.operators.windowing.buffers.EvictingWindowBuffer;
import org.apache.flink.streaming.runtime.operators.windowing.buffers.WindowBufferFactory;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.Window;

import static java.util.Objects.requireNonNull;

/**
 * A {@link WindowOperator} that also allows an {@link Evictor} to be used.
 *
 * <p>
 * The {@code Evictor} is used to evict elements from panes before processing a window and after
 * a {@link Trigger} has fired.
 *
 * @param <K> The type of key returned by the {@code KeySelector}.
 * @param <IN> The type of the incoming elements.
 * @param <OUT> The type of elements emitted by the {@code WindowFunction}.
 * @param <W> The type of {@code Window} that the {@code WindowAssigner} assigns.
 */
public class EvictingWindowOperator<K, IN, OUT, W extends Window> extends WindowOperator<K, IN, OUT, W> {

	private static final long serialVersionUID = 1L;

	private final Evictor<? super IN, ? super W> evictor;

	public EvictingWindowOperator(WindowAssigner<? super IN, W> windowAssigner,
			TypeSerializer<W> windowSerializer,
			KeySelector<IN, K> keySelector,
			TypeSerializer<K> keySerializer,
			WindowBufferFactory<? super IN, ? extends EvictingWindowBuffer<IN>> windowBufferFactory,
			WindowFunction<IN, OUT, K, W> windowFunction,
			Trigger<? super IN, ? super W> trigger,
			Evictor<? super IN, ? super W> evictor) {
		super(windowAssigner, windowSerializer, keySelector, keySerializer, windowBufferFactory, windowFunction, trigger);
		this.evictor = requireNonNull(evictor);
	}

	@Override
	@SuppressWarnings("unchecked, rawtypes")
	protected void emitWindow(Context context) throws Exception {
		timestampedCollector.setTimestamp(context.window.maxTimestamp());
		EvictingWindowBuffer<IN> windowBuffer = (EvictingWindowBuffer<IN>) context.windowBuffer;

		int toEvict = 0;
		if (windowBuffer.size() > 0) {
			// need some type trickery here...
			toEvict = evictor.evict((Iterable) windowBuffer.getElements(), windowBuffer.size(), context.window);
		}

		windowBuffer.removeElements(toEvict);

		userFunction.apply(context.key,
				context.window,
				context.windowBuffer.getUnpackedElements(),
				timestampedCollector);
	}

	@Override
	public EvictingWindowOperator<K, IN, OUT, W> enableSetProcessingTime(boolean setProcessingTime) {
		super.enableSetProcessingTime(setProcessingTime);
		return this;
	}


	// ------------------------------------------------------------------------
	// Getters for testing
	// ------------------------------------------------------------------------

	@VisibleForTesting
	public Evictor<? super IN, ? super W> getEvictor() {
		return evictor;
	}
}
