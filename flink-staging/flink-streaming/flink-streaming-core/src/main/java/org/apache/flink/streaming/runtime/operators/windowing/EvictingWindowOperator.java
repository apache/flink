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
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.runtime.operators.windowing.buffers.EvictingWindowBuffer;
import org.apache.flink.streaming.runtime.operators.windowing.buffers.WindowBuffer;
import org.apache.flink.streaming.runtime.operators.windowing.buffers.WindowBufferFactory;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class EvictingWindowOperator<K, IN, OUT, W extends Window> extends WindowOperator<K, IN, OUT, W> {

	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(EvictingWindowOperator.class);

	private final Evictor<? super IN, ? super W> evictor;

	public EvictingWindowOperator(WindowAssigner<? super IN, W> windowAssigner,
			KeySelector<IN, K> keySelector,
			WindowBufferFactory<? super IN, ? extends EvictingWindowBuffer<IN>> windowBufferFactory,
			WindowFunction<IN, OUT, K, W> windowFunction,
			Trigger<? super IN, ? super W> trigger,
			Evictor<? super IN, ? super W> evictor) {
		super(windowAssigner, keySelector, windowBufferFactory, windowFunction, trigger);
		this.evictor = evictor;
	}

	@Override
	@SuppressWarnings("unchecked, rawtypes")
	protected void emitWindow(K key, W window, boolean purge) throws Exception {

		timestampedCollector.setTimestamp(window.getEnd());

		Map<W, Tuple2<WindowBuffer<IN>, TriggerContext>> keyWindows = windows.get(key);

		if (keyWindows == null) {
			LOG.debug("Window {} for key {} already gone.", window, key);
			return;
		}

		Tuple2<WindowBuffer<IN>, TriggerContext> bufferAndTrigger;
		if (purge) {
			bufferAndTrigger = keyWindows.remove(window);
		} else {
			bufferAndTrigger = keyWindows.get(window);
		}

		if (bufferAndTrigger == null) {
			LOG.debug("Window {} for key {} already gone.", window, key);
			return;
		}


		EvictingWindowBuffer<IN> windowBuffer = (EvictingWindowBuffer<IN>) bufferAndTrigger.f0;

		int toEvict = 0;
		if (windowBuffer.size() > 0) {
			// need some type trickery here...
			toEvict = evictor.evict((Iterable) windowBuffer.getElements(), windowBuffer.size(), window);
		}

		windowBuffer.removeElements(toEvict);

		userFunction.apply(key,
				window,
				bufferAndTrigger.f0.getUnpackedElements(),
				timestampedCollector);

		if (keyWindows.isEmpty()) {
			windows.remove(key);
		}
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
