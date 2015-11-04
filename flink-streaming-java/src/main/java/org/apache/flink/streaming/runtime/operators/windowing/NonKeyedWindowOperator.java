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
import org.apache.flink.api.java.typeutils.InputTypeConfigurable;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.StateHandle;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
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
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static java.util.Objects.requireNonNull;

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
		implements OneInputStreamOperator<IN, OUT>, Triggerable, InputTypeConfigurable {

	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(WindowOperator.class);

	// ------------------------------------------------------------------------
	// Configuration values and stuff from the user
	// ------------------------------------------------------------------------

	private final WindowAssigner<? super IN, W> windowAssigner;

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

	/**
	 * To keep track of the current watermark so that we can immediately fire if a trigger
	 * registers an event time callback for a timestamp that lies in the past.
	 */
	protected transient long currentWatermark = -1L;

	// ------------------------------------------------------------------------
	// State that needs to be checkpointed
	// ------------------------------------------------------------------------

	/**
	 * The windows (panes) that are currently in-flight. Each pane has a {@code WindowBuffer}
	 * and a {@code TriggerContext} that stores the {@code Trigger} for that pane.
	 */
	protected transient Map<W, Context> windows;

	/**
	 * Creates a new {@code WindowOperator} based on the given policies and user functions.
	 */
	public NonKeyedWindowOperator(WindowAssigner<? super IN, W> windowAssigner,
			TypeSerializer<W> windowSerializer,
			WindowBufferFactory<? super IN, ? extends WindowBuffer<IN>> windowBufferFactory,
			AllWindowFunction<IN, OUT, W> windowFunction,
			Trigger<? super IN, ? super W> trigger) {

		super(windowFunction);

		this.windowAssigner = requireNonNull(windowAssigner);
		this.windowSerializer = windowSerializer;

		this.windowBufferFactory = requireNonNull(windowBufferFactory);
		this.trigger = requireNonNull(trigger);

		setChainingStrategy(ChainingStrategy.ALWAYS);
	}

	private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
		in.defaultReadObject();
		currentWatermark = -1;
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
		for (Context context: windows.values()) {
			if (context.processingTimeTimer > 0) {
				Set<Context> triggers = processingTimeTimers.get(context.processingTimeTimer);
				if (triggers == null) {
					getRuntimeContext().registerTimer(context.processingTimeTimer, NonKeyedWindowOperator.this);
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

	@Override
	public final void close() throws Exception {
		super.close();
		// emit the elements that we still keep
		for (Context window: windows.values()) {
			emitWindow(window);
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

		for (W window: elementWindows) {
			Context context = windows.get(window);
			if (context == null) {
				WindowBuffer<IN> windowBuffer = windowBufferFactory.create();
				context = new Context(window, windowBuffer);
				windows.put(window, context);
			}
			context.windowBuffer.storeElement(element);
			Trigger.TriggerResult triggerResult = context.onElement(element);
			processTriggerResult(triggerResult, window);
		}
	}

	protected void emitWindow(Context context) throws Exception {
		timestampedCollector.setTimestamp(context.window.maxTimestamp());

		if (context.windowBuffer.size() > 0) {
			userFunction.apply(
					context.window,
					context.windowBuffer.getUnpackedElements(),
					timestampedCollector);
		}
	}

	private void processTriggerResult(Trigger.TriggerResult triggerResult, W window) throws Exception {
		if (!triggerResult.isFire() && !triggerResult.isPurge()) {
			// do nothing
			return;
		}
		Context context;

		if (triggerResult.isPurge()) {
			context = windows.remove(window);
		} else {
			context = windows.get(window);
		}
		if (context == null) {
			LOG.debug("Window {} already gone.", window);
			return;
		}

		if (triggerResult.isFire()) {
			emitWindow(context);
		}
	}

	@Override
	public final void processWatermark(Watermark mark) throws Exception {
		Set<Long> toRemove = new HashSet<>();
		Set<Context> toTrigger = new HashSet<>();

		// we cannot call the Trigger in here because trigger methods might register new triggers.
		// that would lead to concurrent modification errors.
		for (Map.Entry<Long, Set<Context>> triggers: watermarkTimers.entrySet()) {
			if (triggers.getKey() <= mark.getTimestamp()) {
				for (Context context: triggers.getValue()) {
					toTrigger.add(context);
				}
				toRemove.add(triggers.getKey());
			}
		}

		for (Context context: toTrigger) {
			// double check the time. it can happen that the trigger registers a new timer,
			// in that case the entry is left in the watermarkTimers set for performance reasons.
			// We have to check here whether the entry in the set still reflects the
			// currently set timer in the Context.
			if (context.watermarkTimer <= mark.getTimestamp()) {
				Trigger.TriggerResult triggerResult = context.onEventTime(context.watermarkTimer);
				processTriggerResult(triggerResult, context.window);
			}
		}

		for (Long l: toRemove) {
			watermarkTimers.remove(l);
		}
		output.emitWatermark(mark);

		this.currentWatermark = mark.getTimestamp();
	}

	@Override
	public final void trigger(long time) throws Exception {
		Set<Long> toRemove = new HashSet<>();

		for (Map.Entry<Long, Set<Context>> triggers: processingTimeTimers.entrySet()) {
			long actualTime = triggers.getKey();
			if (actualTime <= time) {
				for (Context context: triggers.getValue()) {
					Trigger.TriggerResult triggerResult = context.onProcessingTime(actualTime);
					processTriggerResult(triggerResult, context.window);
				}
				toRemove.add(triggers.getKey());
			}
		}
		for (Long l: toRemove) {
			processingTimeTimers.remove(l);
		}
	}

	/**
	 * The {@code Context} is responsible for keeping track of the state of one pane.
	 *
	 * <p>
	 * A pane is the bucket of elements that have the same key (assigned by the
	 * {@link org.apache.flink.api.java.functions.KeySelector}) and same {@link Window}. An element can
	 * be in multiple panes of it was assigned to multiple windows by the
	 * {@link org.apache.flink.streaming.api.windowing.assigners.WindowAssigner}. These panes all
	 * have their own instance of the {@code Trigger}.
	 */
	protected class Context implements Trigger.TriggerContext {
		protected W window;

		protected WindowBuffer<IN> windowBuffer;

		protected HashMap<String, Serializable> state;

		// use these to only allow one timer in flight at a time of each type
		// if the trigger registers another timer this value here will be overwritten,
		// the timer is not removed from the set of in-flight timers to improve performance.
		// When a trigger fires it is just checked against the last timer that was set.
		protected long watermarkTimer;
		protected long processingTimeTimer;

		public Context(
				W window,
				WindowBuffer<IN> windowBuffer) {
			this.window = window;
			this.windowBuffer = windowBuffer;
			state = new HashMap<>();

			this.watermarkTimer = -1;
			this.processingTimeTimer = -1;
		}


		@SuppressWarnings("unchecked")
		protected Context(DataInputView in) throws Exception {
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

		protected void writeToState(StateBackend.CheckpointStateOutputView out) throws IOException {
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
				getRuntimeContext().registerTimer(time, NonKeyedWindowOperator.this);
				triggers = new HashSet<>();
				processingTimeTimers.put(time, triggers);
			}
			this.processingTimeTimer = time;
			triggers.add(this);
		}

		@Override
		public void registerEventTimeTimer(long time) {
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

		public Trigger.TriggerResult onElement(StreamRecord<IN> element) throws Exception {
			Trigger.TriggerResult onElementResult = trigger.onElement(element.getValue(), element.getTimestamp(), window, this);
			if (watermarkTimer > 0 && watermarkTimer <= currentWatermark) {
				// fire now and don't wait for the next watermark update
				Trigger.TriggerResult onEventTimeResult = onEventTime(watermarkTimer);
				return Trigger.TriggerResult.merge(onElementResult, onEventTimeResult);
			} else {
				return onElementResult;
			}
		}

		public Trigger.TriggerResult onProcessingTime(long time) throws Exception {
			if (time == processingTimeTimer) {
				processingTimeTimer = -1;
				return trigger.onProcessingTime(time, window, this);
			} else {
				return Trigger.TriggerResult.CONTINUE;
			}
		}

		public Trigger.TriggerResult onEventTime(long time) throws Exception {
			if (time == watermarkTimer) {
				watermarkTimer = -1;
				Trigger.TriggerResult firstTriggerResult = trigger.onEventTime(time, window, this);

				if (watermarkTimer > 0 && watermarkTimer <= currentWatermark) {
					// fire now and don't wait for the next watermark update
					Trigger.TriggerResult secondTriggerResult = onEventTime(watermarkTimer);
					return Trigger.TriggerResult.merge(firstTriggerResult, secondTriggerResult);
				} else {
					return firstTriggerResult;
				}

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
	public NonKeyedWindowOperator<IN, OUT, W> enableSetProcessingTime(boolean setProcessingTime) {
		this.setProcessingTime = setProcessingTime;
		return this;
	}

	// ------------------------------------------------------------------------
	//  Checkpointing
	// ------------------------------------------------------------------------

	@Override
	public StreamTaskState snapshotOperatorState(long checkpointId, long timestamp) throws Exception {
		StreamTaskState taskState = super.snapshotOperatorState(checkpointId, timestamp);

		// we write the panes with the key/value maps into the stream
		StateBackend.CheckpointStateOutputView out = getStateBackend().createCheckpointStateOutputView(checkpointId, timestamp);

		int numWindows = windows.size();
		out.writeInt(numWindows);
		for (Context context: windows.values()) {
			context.writeToState(out);
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

		int numWindows = in.readInt();
		this.windows = new HashMap<>(numWindows);
		this.processingTimeTimers = new HashMap<>();
		this.watermarkTimers = new HashMap<>();

		for (int j = 0; j < numWindows; j++) {
			Context context = new Context(in);
			windows.put(context.window, context);
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
	public WindowAssigner<? super IN, W> getWindowAssigner() {
		return windowAssigner;
	}

	@VisibleForTesting
	public WindowBufferFactory<? super IN, ? extends WindowBuffer<IN>> getWindowBufferFactory() {
		return windowBufferFactory;
	}
}
