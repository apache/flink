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
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.InputTypeConfigurable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.operators.Triggerable;
import org.apache.flink.streaming.runtime.operators.windowing.buffers.WindowBuffer;
import org.apache.flink.streaming.runtime.operators.windowing.buffers.WindowBufferFactory;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

public class WindowOperator<K, IN, OUT, W extends Window>
		extends AbstractUdfStreamOperator<OUT, WindowFunction<IN, OUT, K, W>>
		implements OneInputStreamOperator<IN, OUT>, Triggerable, InputTypeConfigurable {

	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(WindowOperator.class);


	private final WindowAssigner<? super IN, W> windowAssigner;
	private final KeySelector<IN, K> keySelector;

	private final Trigger<? super IN, ? super W> triggerTemplate;
	private final WindowBufferFactory<? super IN, ? extends WindowBuffer<IN>> windowBufferFactory;

	protected transient Map<K, Map<W, Tuple2<WindowBuffer<IN>, TriggerContext>>> windows;

	private transient Map<Long, Set<TriggerContext>> processingTimeTimers;
	private transient Map<Long, Set<TriggerContext>> watermarkTimers;

	protected transient TimestampedCollector<OUT> timestampedCollector;

	private boolean setProcessingTime = false;

	private TypeSerializer<IN> inputSerializer;

	public WindowOperator(WindowAssigner<? super IN, W> windowAssigner,
			KeySelector<IN, K> keySelector,
			WindowBufferFactory<? super IN, ? extends WindowBuffer<IN>> windowBufferFactory,
			WindowFunction<IN, OUT, K, W> windowFunction,
			Trigger<? super IN, ? super W> trigger) {

		super(windowFunction);

		this.windowAssigner = windowAssigner;
		this.keySelector = keySelector;

		this.windowBufferFactory = windowBufferFactory;
		this.triggerTemplate = trigger;

		setChainingStrategy(ChainingStrategy.ALWAYS);
//		forceInputCopy();
	}

	@Override
	@SuppressWarnings("unchecked")
	public void setInputType(TypeInformation<?> type, ExecutionConfig executionConfig) {
		inputSerializer = (TypeSerializer<IN>) type.createSerializer(executionConfig);
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		windows = Maps.newHashMap();
		watermarkTimers = Maps.newHashMap();
		processingTimeTimers = Maps.newHashMap();
		timestampedCollector = new TimestampedCollector<>(output);

		if (inputSerializer == null) {
			throw new IllegalStateException("Input serializer was not set.");
		}

		windowBufferFactory.setRuntimeContext(getRuntimeContext());
		windowBufferFactory.open(parameters);
	}

	@Override
	public void close() throws Exception {
		super.close();
		// emit the elements that we still keep
		for (Map.Entry<K, Map<W, Tuple2<WindowBuffer<IN>, TriggerContext>>> entry: windows.entrySet()) {
			K key = entry.getKey();
			Map<W, Tuple2<WindowBuffer<IN>, TriggerContext>> keyWindows = entry.getValue();
			for (W window: keyWindows.keySet()) {
				emitWindow(key, window, false);
			}
		}
		windows.clear();
		windowBufferFactory.close();
	}

	@Override
	@SuppressWarnings("unchecked")
	public void processElement(StreamRecord<IN> element) throws Exception {
		if (setProcessingTime) {
			element.replace(element.getValue(), System.currentTimeMillis());
		}
		Collection<W> elementWindows = windowAssigner.assignWindows(element.getValue(), element.getTimestamp());

		K key = keySelector.getKey(element.getValue());

		Map<W, Tuple2<WindowBuffer<IN>, TriggerContext>> keyWindows = windows.get(key);
		if (keyWindows == null) {
			keyWindows = Maps.newHashMap();
			windows.put(key, keyWindows);
		}

		for (W window: elementWindows) {
			Tuple2<WindowBuffer<IN>, TriggerContext> bufferAndTrigger = keyWindows.get(window);
			if (bufferAndTrigger == null) {
				bufferAndTrigger = new Tuple2<>();
				bufferAndTrigger.f0 = windowBufferFactory.create();
				bufferAndTrigger.f1 = new TriggerContext(key, window, triggerTemplate.duplicate());
				keyWindows.put(window, bufferAndTrigger);
			}
			StreamRecord<IN> elementCopy = new StreamRecord<>(inputSerializer.copy(element.getValue()), element.getTimestamp());
			bufferAndTrigger.f0.storeElement(elementCopy);
			Trigger.TriggerResult triggerResult = bufferAndTrigger.f1.trigger.onElement(elementCopy.getValue(), elementCopy.getTimestamp(), window, bufferAndTrigger.f1);
			processTriggerResult(triggerResult, key, window);
		}
	}

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


		userFunction.apply(key,
				window,
				bufferAndTrigger.f0.getUnpackedElements(),
				timestampedCollector);

		if (keyWindows.isEmpty()) {
			windows.remove(key);
		}
	}

	private void processTriggerResult(Trigger.TriggerResult triggerResult, K key, W window) throws Exception {
		switch (triggerResult) {
			case FIRE:
				emitWindow(key, window, false);
				break;

			case FIRE_AND_PURGE:
				emitWindow(key, window, true);
				break;

			case CONTINUE:
				// ingore
		}
	}

	@Override
	public void processWatermark(Watermark mark) throws Exception {
		Set<Long> toRemove = Sets.newHashSet();

		for (Map.Entry<Long, Set<TriggerContext>> triggers: watermarkTimers.entrySet()) {
			if (triggers.getKey() <= mark.getTimestamp()) {
				for (TriggerContext trigger: triggers.getValue()) {
					Trigger.TriggerResult triggerResult = trigger.trigger.onTime(mark.getTimestamp(), trigger);
					processTriggerResult(triggerResult, trigger.key, trigger.window);
				}
				toRemove.add(triggers.getKey());
			}
		}

		for (Long l: toRemove) {
			watermarkTimers.remove(l);
		}
		output.emitWatermark(mark);
	}

	@Override
	public void trigger(long time) throws Exception {
		Set<Long> toRemove = Sets.newHashSet();

		for (Map.Entry<Long, Set<TriggerContext>> triggers: processingTimeTimers.entrySet()) {
			if (triggers.getKey() < time) {
				for (TriggerContext trigger: triggers.getValue()) {
					Trigger.TriggerResult triggerResult = trigger.trigger.onTime(time, trigger);
					processTriggerResult(triggerResult, trigger.key, trigger.window);
				}
				toRemove.add(triggers.getKey());
			}
		}

		for (Long l: toRemove) {
			processingTimeTimers.remove(l);
		}
	}

	protected class TriggerContext implements Trigger.TriggerContext {
		Trigger<? super IN, ? super W> trigger;
		K key;
		W window;

		public TriggerContext(K key, W window, Trigger<? super IN, ? super W> trigger) {
			this.key = key;
			this.window = window;
			this.trigger = trigger;
		}

		@Override
		public void registerProcessingTimeTimer(long time) {
			Set<TriggerContext> triggers = processingTimeTimers.get(time);
			if (triggers == null) {
				getRuntimeContext().registerTimer(time, WindowOperator.this);
				triggers = Sets.newHashSet();
				processingTimeTimers.put(time, triggers);
			}
			triggers.add(this);
		}

		@Override
		public void registerWatermarkTimer(long time) {
			Set<TriggerContext> triggers = watermarkTimers.get(time);
			if (triggers == null) {
				triggers = Sets.newHashSet();
				watermarkTimers.put(time, triggers);
			}
			triggers.add(this);
		}
	}

	/**
	 * When this flag is enabled the current processing time is set as the timestamp of elements
	 * upon arrival. This must be used, for example, when using the
	 * {@link org.apache.flink.streaming.api.windowing.evictors.TimeEvictor} with processing
	 * time semantics.
	 */
	public WindowOperator<K, IN, OUT, W> enableSetProcessingTime(boolean setProcessingTime) {
		this.setProcessingTime = setProcessingTime;
		return this;
	}

	// ------------------------------------------------------------------------
	// Getters for testing
	// ------------------------------------------------------------------------

	@VisibleForTesting
	public Trigger<? super IN, ? super W> getTriggerTemplate() {
		return triggerTemplate;
	}

	@VisibleForTesting
	public KeySelector<IN, K> getKeySelector() {
		return keySelector;
	}

	@VisibleForTesting
	public WindowAssigner<? super IN, W> getWindowAssigner() {
		return windowAssigner;
	}

	@VisibleForTesting
	public WindowBufferFactory<? super IN, ? extends WindowBuffer<IN>> getWindowBufferFactory() {
		return windowBufferFactory;
	}

	@VisibleForTesting
	public boolean isSetProcessingTime() {
		return setProcessingTime;
	}
}
