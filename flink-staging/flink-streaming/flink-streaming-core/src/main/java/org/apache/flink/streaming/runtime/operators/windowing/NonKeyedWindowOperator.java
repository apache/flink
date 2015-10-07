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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.InputTypeConfigurable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.OutputTypeConfigurable;
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

/**
 * Window operator for non-keyed windows.
 *
 * @see org.apache.flink.streaming.runtime.operators.windowing.WindowOperator
 *
 * @param <IN> The type of the incoming elements.
 * @param <OUT> The type of elements emitted by the {@code WindowFunction}.
 * @param <W> The type of {@code Window} that the {@code WindowAssigner} assigns.
 */
public class NonKeyedWindowOperator<IN, OUT, W extends Window>
		extends AbstractUdfStreamOperator<OUT, AllWindowFunction<IN, OUT, W>>
		implements OneInputStreamOperator<IN, OUT>, Triggerable, InputTypeConfigurable, OutputTypeConfigurable<OUT> {

	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(NonKeyedWindowOperator.class);


	private final WindowAssigner<? super IN, W> windowAssigner;

	private final Trigger<? super IN, ? super W> triggerTemplate;
	private final WindowBufferFactory<? super IN, ? extends WindowBuffer<IN>> windowBufferFactory;

	protected transient Map<W, Tuple2<WindowBuffer<IN>, TriggerContext>> windows;

	private transient Map<Long, Set<TriggerContext>> processingTimeTimers;
	private transient Map<Long, Set<TriggerContext>> watermarkTimers;

	protected transient TimestampedCollector<OUT> timestampedCollector;

	private boolean setProcessingTime = false;

	private TypeSerializer<IN> inputSerializer;

	public NonKeyedWindowOperator(WindowAssigner<? super IN, W> windowAssigner,
			WindowBufferFactory<? super IN, ? extends WindowBuffer<IN>> windowBufferFactory,
			AllWindowFunction<IN, OUT, W> windowFunction,
			Trigger<? super IN, ? super W> trigger) {

		super(windowFunction);

		this.windowAssigner = windowAssigner;

		this.windowBufferFactory = windowBufferFactory;
		this.triggerTemplate = trigger;

		setChainingStrategy(ChainingStrategy.ALWAYS);
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
		for (W window: windows.keySet()) {
			emitWindow(window, false);
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

		for (W window: elementWindows) {
			Tuple2<WindowBuffer<IN>, TriggerContext> bufferAndTrigger = windows.get(window);
			if (bufferAndTrigger == null) {
				bufferAndTrigger = new Tuple2<>();
				bufferAndTrigger.f0 = windowBufferFactory.create();
				bufferAndTrigger.f1 = new TriggerContext(window, triggerTemplate.duplicate());
				windows.put(window, bufferAndTrigger);
			}
			StreamRecord<IN> elementCopy = new StreamRecord<>(inputSerializer.copy(element.getValue()), element.getTimestamp());
			bufferAndTrigger.f0.storeElement(elementCopy);
			Trigger.TriggerResult triggerResult = bufferAndTrigger.f1.trigger.onElement(elementCopy.getValue(), elementCopy.getTimestamp(), window, bufferAndTrigger.f1);
			processTriggerResult(triggerResult, window);
		}
	}

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


		userFunction.apply(
				window,
				bufferAndTrigger.f0.getUnpackedElements(),
				timestampedCollector);
	}

	private void processTriggerResult(Trigger.TriggerResult triggerResult, W window) throws Exception {
		switch (triggerResult) {
			case FIRE:
				emitWindow(window, false);
				break;

			case FIRE_AND_PURGE:
				emitWindow(window, true);
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
					processTriggerResult(triggerResult, trigger.window);
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
					processTriggerResult(triggerResult, trigger.window);
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
		W window;

		public TriggerContext(W window, Trigger<? super IN, ? super W> trigger) {
			this.window = window;
			this.trigger = trigger;
		}

		@Override
		public void registerProcessingTimeTimer(long time) {
			Set<TriggerContext> triggers = processingTimeTimers.get(time);
			if (triggers == null) {
				getRuntimeContext().registerTimer(time, NonKeyedWindowOperator.this);
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
	public NonKeyedWindowOperator<IN, OUT, W> enableSetProcessingTime(boolean setProcessingTime) {
		this.setProcessingTime = setProcessingTime;
		return this;
	}

	@Override
	public void setOutputType(TypeInformation<OUT> outTypeInfo, ExecutionConfig executionConfig) {
		if (userFunction instanceof OutputTypeConfigurable) {
			@SuppressWarnings("unchecked")
			OutputTypeConfigurable<OUT> typeConfigurable = (OutputTypeConfigurable<OUT>) userFunction;
			typeConfigurable.setOutputType(outTypeInfo, executionConfig);
		}
	}

	// ------------------------------------------------------------------------
	// Getters for testing
	// ------------------------------------------------------------------------

	@VisibleForTesting
	public Trigger<? super IN, ? super W> getTriggerTemplate() {
		return triggerTemplate;
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
