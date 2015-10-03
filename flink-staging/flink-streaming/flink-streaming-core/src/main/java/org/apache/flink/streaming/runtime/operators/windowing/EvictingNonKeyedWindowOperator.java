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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.operators.windowing.buffers.EvictingWindowBuffer;
import org.apache.flink.streaming.runtime.operators.windowing.buffers.WindowBuffer;
import org.apache.flink.streaming.runtime.operators.windowing.buffers.WindowBufferFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Evicting window operator for non-keyed windows.
 *
 * @see org.apache.flink.streaming.runtime.operators.windowing.EvictingWindowOperator
 *
 * @param <IN> The type of the incoming elements.
 * @param <OUT> The type of elements emitted by the {@code WindowFunction}.
 * @param <W> The type of {@code Window} that the {@code WindowAssigner} assigns.
 */
public class EvictingNonKeyedWindowOperator<IN, OUT, W extends Window> extends NonKeyedWindowOperator<IN, OUT, W> {

	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(EvictingNonKeyedWindowOperator.class);

	private final Evictor<? super IN, ? super W> evictor;

	public EvictingNonKeyedWindowOperator(WindowAssigner<? super IN, W> windowAssigner,
			WindowBufferFactory<? super IN, ? extends EvictingWindowBuffer<IN>> windowBufferFactory,
			AllWindowFunction<IN, OUT, W> windowFunction,
			Trigger<? super IN, ? super W> trigger,
			Evictor<? super IN, ? super W> evictor) {
		super(windowAssigner, windowBufferFactory, windowFunction, trigger);
		this.evictor = evictor;
	}

	@Override
	@SuppressWarnings("unchecked, rawtypes")
	protected void emitWindow(W window, boolean purge) throws Exception {

		timestampedCollector.setTimestamp(window.getEnd());

		Tuple2<WindowBuffer<IN>, TriggerContext> bufferAndTrigger;
		if (purge) {
			bufferAndTrigger = windows.remove(window);
		} else {
			bufferAndTrigger = windows.get(window);
		}

		if (bufferAndTrigger == null) {
			LOG.debug("Window {} already gone.", window);
			return;
		}


		EvictingWindowBuffer<IN> windowBuffer = (EvictingWindowBuffer<IN>) bufferAndTrigger.f0;

		int toEvict = 0;
		if (windowBuffer.size() > 0) {
			// need some type trickery here...
			toEvict = evictor.evict((Iterable) windowBuffer.getElements(), windowBuffer.size(), window);
		}

		windowBuffer.removeElements(toEvict);

		userFunction.apply(
				window,
				bufferAndTrigger.f0.getUnpackedElements(),
				timestampedCollector);
	}

	@Override
	public EvictingNonKeyedWindowOperator<IN, OUT, W> enableSetProcessingTime(boolean setProcessingTime) {
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
