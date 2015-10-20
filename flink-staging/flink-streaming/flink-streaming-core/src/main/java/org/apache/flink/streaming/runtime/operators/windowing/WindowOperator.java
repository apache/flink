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
import org.apache.commons.lang.SerializationUtils;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.OperatorState;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.InputTypeConfigurable;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.StateHandle;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
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
import org.apache.flink.streaming.runtime.streamrecord.MultiplexingStreamRecordSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTaskState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * An operator that implements the logic for windowing based on a {@link WindowAssigner} and
 * {@link Trigger}.
 *
 * <p>
 * When an element arrives it gets assigned a key using a {@link KeySelector} and it get's
 * assigned to zero or more windows using a {@link WindowAssigner}. Based on this the element
 * is put into panes. A pane is the bucket of elements that have the same key and same
 * {@code Window}. An element can be in multiple panes of it was assigned to multiple windows by the
 * {@code WindowAssigner}.
 *
 * <p>
 * Each pane gets its own instance of the provided {@code Trigger}. This trigger determines when
 * the contents of the pane should be processed to emit results. When a trigger fires,
 * the given {@link WindowFunction} is invoked to produce the results that are emitted for
 * the pane to which the {@code Trigger} belongs.
 *
 * <p>
 * This operator also needs a {@link WindowBufferFactory} to create a buffer for storing the
 * elements of each pane.
 *
 * @param <K> The type of key returned by the {@code KeySelector}.
 * @param <IN> The type of the incoming elements.
 * @param <OUT> The type of elements emitted by the {@code WindowFunction}.
 * @param <W> The type of {@code Window} that the {@code WindowAssigner} assigns.
 */
public class WindowOperator<K, IN, OUT, W extends Window>
		extends AbstractUdfStreamOperator<OUT, WindowFunction<IN, OUT, K, W>>
		implements OneInputStreamOperator<IN, OUT>, Triggerable, InputTypeConfigurable, OutputTypeConfigurable<OUT> {

	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(WindowOperator.class);

	// ------------------------------------------------------------------------
	// Configuration values and user functions
	// ------------------------------------------------------------------------

	private final WindowAssigner<? super IN, W> windowAssigner;

	private final KeySelector<IN, K> keySelector;

	private final Trigger<? super IN, ? super W> trigger;

	private final WindowBufferFactory<? super IN, ? extends WindowBuffer<IN>> windowBufferFactory;

	/**
	 * If this is true. The current processing time is set as the timestamp of incoming elements.
	 * This for use with a {@link org.apache.flink.streaming.api.windowing.evictors.TimeEvictor}
	 * if eviction should happen based on processing time.
	 */
	private boolean setProcessingTime = false;

	/**
	 * This is used to copy the incoming element because it can be put into several window
	 * buffers.
	 */
	private TypeSerializer<IN> inputSerializer;

	/**
	 * For serializing the key in checkpoints.
	 */
	private final TypeSerializer<K> keySerializer;

	/**
	 * For serializing the window in checkpoints.
	 */
	private final TypeSerializer<W> windowSerializer;

	// ------------------------------------------------------------------------
	// State that is not checkpointed
	// ------------------------------------------------------------------------

	/**
	 * Processing time timers that are currently in-flight.
	 */
	private transient Map<Long, Set<Context>> processingTimeTimers;

	/**
	 * Current waiting watermark callbacks.
	 */
	private transient Map<Long, Set<Context>> watermarkTimers;

	/**
	 * This is given to the {@code WindowFunction} for emitting elements with a given timestamp.
	 */
	protected transient TimestampedCollector<OUT> timestampedCollector;

	// ------------------------------------------------------------------------
	// State that needs to be checkpointed
	// ------------------------------------------------------------------------

	/**
	 * The windows (panes) that are currently in-flight. Each pane has a {@code WindowBuffer}
	 * and a {@code TriggerContext} that stores the {@code Trigger} for that pane.
	 */
	protected transient Map<K, Map<W, Context>> windows;

	/**
	 * Creates a new {@code WindowOperator} based on the given policies and user functions.
	 */
	public WindowOperator(WindowAssigner<? super IN, W> windowAssigner,
			TypeSerializer<W> windowSerializer,
			KeySelector<IN, K> keySelector,
			TypeSerializer<K> keySerializer,
			WindowBufferFactory<? super IN, ? extends WindowBuffer<IN>> windowBufferFactory,
			WindowFunction<IN, OUT, K, W> windowFunction,
			Trigger<? super IN, ? super W> trigger) {

		super(windowFunction);

		this.windowAssigner = requireNonNull(windowAssigner);
		this.windowSerializer = windowSerializer;
		this.keySelector = requireNonNull(keySelector);
		this.keySerializer = requireNonNull(keySerializer);

		this.windowBufferFactory = requireNonNull(windowBufferFactory);
		this.trigger = requireNonNull(trigger);

		setChainingStrategy(ChainingStrategy.ALWAYS);
	}

	@Override
	@SuppressWarnings("unchecked")
	public final void setInputType(TypeInformation<?> type, ExecutionConfig executionConfig) {
		inputSerializer = (TypeSerializer<IN>) type.createSerializer(executionConfig);
	}

	@Override
	public final void open() throws Exception {
		super.open();

		timestampedCollector = new TimestampedCollector<>(output);

		if (inputSerializer == null) {
			throw new IllegalStateException("Input serializer was not set.");
		}

		windowBufferFactory.setRuntimeContext(getRuntimeContext());
		windowBufferFactory.open(getUserFunctionParameters());


		// these could already be initialized from restoreState()
		if (watermarkTimers == null) {
			watermarkTimers = new HashMap<>();
		}
		if (processingTimeTimers == null) {
			processingTimeTimers = new HashMap<>();
		}
		if (windows == null) {
			windows = new HashMap<>();
		}

		// re-register timers that this window context had set
		for (Map.Entry<K, Map<W, Context>> entry: windows.entrySet()) {
			Map<W, Context> keyWindows = entry.getValue();
			for (Context context: keyWindows.values()) {
				if (context.processingTimeTimer > 0) {
					Set<Context> triggers = processingTimeTimers.get(context.processingTimeTimer);
					if (triggers == null) {
						getRuntimeContext().registerTimer(context.processingTimeTimer, WindowOperator.this);
						triggers = new HashSet<>();
						processingTimeTimers.put(context.processingTimeTimer, triggers);
					}
					triggers.add(context);
				}
				if (context.watermarkTimer > 0) {
					Set<Context> triggers = watermarkTimers.get(context.watermarkTimer);
					if (triggers == null) {
						triggers = new HashSet<>();
						watermarkTimers.put(context.watermarkTimer, triggers);
					}
					triggers.add(context);
				}

			}
		}
	}

	@Override
	public final void close() throws Exception {
		super.close();
		// emit the elements that we still keep
		for (Map.Entry<K, Map<W, Context>> entry: windows.entrySet()) {
			Map<W, Context> keyWindows = entry.getValue();
			for (Context window: keyWindows.values()) {
				emitWindow(window);
			}
		}
		windows.clear();
		windowBufferFactory.close();
	}

	@Override
	@SuppressWarnings("unchecked")
	public final void processElement(StreamRecord<IN> element) throws Exception {
		if (setProcessingTime) {
			element.replace(element.getValue(), System.currentTimeMillis());
		}

		Collection<W> elementWindows = windowAssigner.assignWindows(element.getValue(), element.getTimestamp());

		K key = keySelector.getKey(element.getValue());

		Map<W, Context> keyWindows = windows.get(key);
		if (keyWindows == null) {
			keyWindows = new HashMap<>();
			windows.put(key, keyWindows);
		}

		for (W window: elementWindows) {
			Context context = keyWindows.get(window);
			if (context == null) {
				WindowBuffer<IN> windowBuffer = windowBufferFactory.create();
				context = new Context(key, window, windowBuffer);
				keyWindows.put(window, context);
			}
			StreamRecord<IN> elementCopy = new StreamRecord<>(inputSerializer.copy(element.getValue()), element.getTimestamp());
			context.windowBuffer.storeElement(elementCopy);
			Trigger.TriggerResult triggerResult = trigger.onElement(elementCopy.getValue(), elementCopy.getTimestamp(), window, context);
			processTriggerResult(triggerResult, key, window);
		}
	}

	protected void emitWindow(Context context) throws Exception {
		timestampedCollector.setTimestamp(context.window.maxTimestamp());

		userFunction.apply(context.key,
				context.window,
				context.windowBuffer.getUnpackedElements(),
				timestampedCollector);
	}

	private void processTriggerResult(Trigger.TriggerResult triggerResult, K key, W window) throws Exception {
		switch (triggerResult) {
			case FIRE: {
				Map<W, Context> keyWindows = windows.get(key);
				if (keyWindows == null) {
					LOG.debug("Window {} for key {} already gone.", window, key);
					return;
				}
				Context context = keyWindows.get(window);
				if (context == null) {
					LOG.debug("Window {} for key {} already gone.", window, key);
					return;
				}


				emitWindow(context);
				break;
			}

			case FIRE_AND_PURGE: {
				Map<W, Context> keyWindows = windows.get(key);
				if (keyWindows == null) {
					LOG.debug("Window {} for key {} already gone.", window, key);
					return;
				}
				Context context = keyWindows.remove(window);
				if (context == null) {
					LOG.debug("Window {} for key {} already gone.", window, key);
					return;
				}
				if (keyWindows.isEmpty()) {
					windows.remove(key);
				}

				emitWindow(context);
				break;
			}

			case CONTINUE:
				// ingore
		}
	}

	@Override
	public final void processWatermark(Watermark mark) throws Exception {
		Set<Long> toRemove = new HashSet<>();

		for (Map.Entry<Long, Set<Context>> triggers: watermarkTimers.entrySet()) {
			if (triggers.getKey() <= mark.getTimestamp()) {
				for (Context context: triggers.getValue()) {
					Trigger.TriggerResult triggerResult = context.onEventTime(triggers.getKey());
					processTriggerResult(triggerResult, context.key, context.window);
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
	public final void trigger(long time) throws Exception {
		Set<Long> toRemove = new HashSet<>();

		for (Map.Entry<Long, Set<Context>> triggers: processingTimeTimers.entrySet()) {
			if (triggers.getKey() < time) {
				for (Context context: triggers.getValue()) {
					Trigger.TriggerResult triggerResult = context.onProcessingTime(time);
					processTriggerResult(triggerResult, context.key, context.window);
				}
				toRemove.add(triggers.getKey());
			}
		}

		for (Long l: toRemove) {
			processingTimeTimers.remove(l);
		}
	}

	/**
	 * A context object that is given to {@code Trigger} functions to allow them to register
	 * timer/watermark callbacks.
	 */
	protected class Context implements Trigger.TriggerContext {
		protected K key;
		protected W window;

		protected WindowBuffer<IN> windowBuffer;

		protected HashMap<String, Serializable> state;

		// use these to only allow one timer in flight at a time of each type
		// if the trigger registers another timer this value here will be overwritten,
		// the timer is not removed from the set of in-flight timers to improve performance.
		// When a trigger fires it is just checked against the last timer that was set.
		protected long watermarkTimer;
		protected long processingTimeTimer;

		public Context(K key,
				W window,
				WindowBuffer<IN> windowBuffer) {
			this.key = key;
			this.window = window;
			this.windowBuffer = windowBuffer;
			state = new HashMap<>();

			this.watermarkTimer = -1;
			this.processingTimeTimer = -1;
		}

		/**
		 * Constructs a new {@code Context} by reading from a {@link DataInputView} that
		 * contains a serialized context that we wrote in
		 * {@link #writeToState(StateBackend.CheckpointStateOutputView)}
		 */
		@SuppressWarnings("unchecked")
		protected Context(DataInputView in) throws Exception {
			this.key = keySerializer.deserialize(in);
			this.window = windowSerializer.deserialize(in);
			this.watermarkTimer = in.readLong();
			this.processingTimeTimer = in.readLong();

			int stateSize = in.readInt();
			byte[] stateData = new byte[stateSize];
			in.read(stateData);
			ByteArrayInputStream bais = new ByteArrayInputStream(stateData);
			state = (HashMap<String, Serializable>) SerializationUtils.deserialize(bais);

			this.windowBuffer = windowBufferFactory.create();
			int numElements = in.readInt();
			MultiplexingStreamRecordSerializer<IN> recordSerializer = new MultiplexingStreamRecordSerializer<>(inputSerializer);
			for (int i = 0; i < numElements; i++) {
				windowBuffer.storeElement(recordSerializer.deserialize(in).<IN>asRecord());
			}
		}

		/**
		 * Writes the {@code Context} to the given state checkpoint output.
		 */
		protected void writeToState(StateBackend.CheckpointStateOutputView out) throws IOException {
			keySerializer.serialize(key, out);
			windowSerializer.serialize(window, out);
			out.writeLong(watermarkTimer);
			out.writeLong(processingTimeTimer);

			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			SerializationUtils.serialize(state, baos);
			out.writeInt(baos.size());
			out.write(baos.toByteArray(), 0, baos.size());

			MultiplexingStreamRecordSerializer<IN> recordSerializer = new MultiplexingStreamRecordSerializer<>(inputSerializer);
			out.writeInt(windowBuffer.size());
			for (StreamRecord<IN> element: windowBuffer.getElements()) {
				recordSerializer.serialize(element, out);
			}
		}

		@SuppressWarnings("unchecked")
		public <S extends Serializable> OperatorState<S> getKeyValueState(final String name, final S defaultState) {
			return new OperatorState<S>() {
				@Override
				public S value() throws IOException {
					Serializable value = state.get(name);
					if (value == null) {
						state.put(name, defaultState);
						value = defaultState;
					}
					return (S) value;
				}

				@Override
				public void update(S value) throws IOException {
					state.put(name, value);
				}
			};
		}

		@Override
		public void registerProcessingTimeTimer(long time) {
			if (this.processingTimeTimer == time) {
				// we already have set a trigger for that time
				return;
			}
			Set<Context> triggers = processingTimeTimers.get(time);
			if (triggers == null) {
				getRuntimeContext().registerTimer(time, WindowOperator.this);
				triggers = new HashSet<>();
				processingTimeTimers.put(time, triggers);
			}
			this.processingTimeTimer = time;
			triggers.add(this);
		}

		@Override
		public void registerWatermarkTimer(long time) {
			if (watermarkTimer == time) {
				// we already have set a trigger for that time
				return;
			}
			Set<Context> triggers = watermarkTimers.get(time);
			if (triggers == null) {
				triggers = new HashSet<>();
				watermarkTimers.put(time, triggers);
			}
			this.watermarkTimer = time;
			triggers.add(this);
		}

		public Trigger.TriggerResult onProcessingTime(long time) throws Exception {
			if (time == processingTimeTimer) {
				return trigger.onTime(time, this);
			} else {
				return Trigger.TriggerResult.CONTINUE;
			}
		}

		public Trigger.TriggerResult onEventTime(long time) throws Exception {
			if (time == watermarkTimer) {
				return trigger.onTime(time, this);
			} else {
				return Trigger.TriggerResult.CONTINUE;
			}
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

	@Override
	public final void setOutputType(TypeInformation<OUT> outTypeInfo, ExecutionConfig executionConfig) {
		if (userFunction instanceof OutputTypeConfigurable) {
			@SuppressWarnings("unchecked")
			OutputTypeConfigurable<OUT> typeConfigurable = (OutputTypeConfigurable<OUT>) userFunction;
			typeConfigurable.setOutputType(outTypeInfo, executionConfig);
		}
	}

	// ------------------------------------------------------------------------
	//  Checkpointing
	// ------------------------------------------------------------------------

	@Override
	public StreamTaskState snapshotOperatorState(long checkpointId, long timestamp) throws Exception {
		StreamTaskState taskState = super.snapshotOperatorState(checkpointId, timestamp);

		// we write the panes with the key/value maps into the stream
		StateBackend.CheckpointStateOutputView out = getStateBackend().createCheckpointStateOutputView(checkpointId, timestamp);

		int numKeys = windows.size();
		out.writeInt(numKeys);

		for (Map.Entry<K, Map<W, Context>> keyWindows: windows.entrySet()) {
			int numWindows = keyWindows.getValue().size();
			out.writeInt(numWindows);
			for (Context context: keyWindows.getValue().values()) {
				context.writeToState(out);
			}
		}

		taskState.setOperatorState(out.closeAndGetHandle());
		return taskState;
	}

	@Override
	public void restoreState(StreamTaskState taskState) throws Exception {
		super.restoreState(taskState);


		@SuppressWarnings("unchecked")
		StateHandle<DataInputView> inputState = (StateHandle<DataInputView>) taskState.getOperatorState();
		DataInputView in = inputState.getState(getUserCodeClassloader());

		int numKeys = in.readInt();
		this.windows = new HashMap<>(numKeys);
		this.processingTimeTimers = new HashMap<>();
		this.watermarkTimers = new HashMap<>();

		for (int i = 0; i < numKeys; i++) {
			int numWindows = in.readInt();
			for (int j = 0; j < numWindows; j++) {
				Context context = new Context(in);
				Map<W, Context> keyWindows = windows.get(context.key);
				if (keyWindows == null) {
					keyWindows = new HashMap<>(numWindows);
					windows.put(context.key, keyWindows);
				}
				keyWindows.put(context.window, context);
			}
		}
	}

	// ------------------------------------------------------------------------
	// Getters for testing
	// ------------------------------------------------------------------------

	@VisibleForTesting
	public boolean isSetProcessingTime() {
		return setProcessingTime;
	}

	@VisibleForTesting
	public Trigger<? super IN, ? super W> getTrigger() {
		return trigger;
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
}
