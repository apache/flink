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
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.operators.windowing.buffers.EvictingWindowBuffer;
import org.apache.flink.streaming.runtime.operators.windowing.buffers.WindowBufferFactory;

import static java.util.Objects.requireNonNull;

/**
 * Evicting window operator for non-keyed windows.
 *
 * @see org.apache.flink.streaming.runtime.operators.windowing.EvictingWindowOperator
 *
 * @param <IN> The type of the incoming elements.
 * @param <ACC> The type of elements stored in the window buffers.
 * @param <OUT> The type of elements emitted by the {@code WindowFunction}.
 * @param <W> The type of {@code Window} that the {@code WindowAssigner} assigns.
 */
@Internal
public class EvictingNonKeyedWindowOperator<IN, ACC, OUT, W extends Window> extends NonKeyedWindowOperator<IN, ACC, OUT, W> {

	private static final long serialVersionUID = 1L;

	private final Evictor<? super IN, ? super W> evictor;

	public EvictingNonKeyedWindowOperator(WindowAssigner<? super IN, W> windowAssigner,
			TypeSerializer<W> windowSerializer,
			WindowBufferFactory<? super IN, ACC, ? extends EvictingWindowBuffer<IN, ACC>> windowBufferFactory,
			AllWindowFunction<ACC, OUT, W> windowFunction,
			Trigger<? super IN, ? super W> trigger,
			Evictor<? super IN, ? super W> evictor) {
		super(windowAssigner, windowSerializer, windowBufferFactory, windowFunction, trigger);
		this.evictor = requireNonNull(evictor);
	}

	@Override
	@SuppressWarnings("unchecked, rawtypes")
	protected void emitWindow(Context context) throws Exception {
		timestampedCollector.setAbsoluteTimestamp(context.window.maxTimestamp());
		EvictingWindowBuffer<IN, ACC> windowBuffer = (EvictingWindowBuffer<IN, ACC>) context.windowBuffer;

		int toEvict = 0;
		if (windowBuffer.size() > 0) {
			// need some type trickery here...
			toEvict = evictor.evict((Iterable) windowBuffer.getElements(), windowBuffer.size(), context.window);
		}

		windowBuffer.removeElements(toEvict);

		userFunction.apply(
				context.window,
				context.windowBuffer.getUnpackedElements(),
				timestampedCollector);
	}

	// ------------------------------------------------------------------------
	// Getters for testing
	// ------------------------------------------------------------------------

	@VisibleForTesting
	public Evictor<? super IN, ? super W> getEvictor() {
		return evictor;
	}
}
