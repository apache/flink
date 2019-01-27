/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.bundle;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.runtime.state.keyed.KeyedValueState;
import org.apache.flink.runtime.state.keyed.KeyedValueStateDescriptor;
import org.apache.flink.streaming.api.bundle.BundleTrigger;
import org.apache.flink.streaming.api.bundle.BundleTriggerCallback;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.table.runtime.functions.BundleFunction;
import org.apache.flink.table.runtime.functions.ExecutionContextImpl;
import org.apache.flink.table.runtime.util.StreamRecordCollector;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import java.util.HashMap;
import java.util.Map;

/**
 * The {@link KeyedBundleOperator} allows to process incoming stream records as a bundle on {@link
 * org.apache.flink.streaming.api.datastream.KeyedStream}.
 * For that the operator creates a bundle which is passed to an {@link BundleFunction}. <strong> In a bundle,
 * elements of the same key are in order, and elements of different key are out of order.</strong>
 * In case of chaining of this operator, it has to be made sure that the operators in the chain are
 * opened tail to head. The reason for this is that an opened {@link KeyedBundleOperator} starts
 * already emitting recovered {@link StreamElement} to downstream operators.
 *
 * @param <K> The type of the key in the Keyed Stream.
 * @param <V> The type of the value in the bundle buffer
 * @param <IN> Type of the input elements.
 * @param <OUT> Type of the returned elements.
 */
public class KeyedBundleOperator<K, V, IN, OUT>
	extends AbstractStreamOperator<OUT>
	implements OneInputStreamOperator<IN, OUT>, BundleTriggerCallback {
	private static final long serialVersionUID = 5081841938324118594L;

	private final boolean finishBundleBeforeSnapshot;

	/** The trigger that determines how many elementsMap should be put into a bundle. */
	private final BundleTrigger<IN> bundleTrigger;

	private final BundleFunction<K, V, IN, OUT> function;

	private final TypeInformation<V> valueType;

	private transient Object checkpointingLock;

	private transient Map<K, V> buffer;

	/** Output for stream records. */
	private transient Collector<OUT> collector;

	/** The state to store buffer to make it exactly once. */
	private transient KeyedValueState<K, V> bufferState;

	private transient int numOfElements = 0;

	private transient volatile boolean isInFinishingBundle = false;

	public KeyedBundleOperator(
		BundleFunction<K, V, IN, OUT> function,
		BundleTrigger<IN> bundleTrigger,
		TypeInformation<V> valueType,
		boolean finishBundleBeforeSnapshot) {
		this.finishBundleBeforeSnapshot = finishBundleBeforeSnapshot;
		chainingStrategy = ChainingStrategy.ALWAYS;

		this.function = function;
		this.bundleTrigger = Preconditions.checkNotNull(bundleTrigger, "bundleTrigger is null");
		this.valueType = Preconditions.checkNotNull(valueType, "valueType is null");
	}

	@Override
	public void setup(StreamTask<?, ?> containingTask, StreamConfig config, Output<StreamRecord<OUT>> output) {
		super.setup(containingTask, config, output);
		this.checkpointingLock = getContainingTask().getCheckpointLock();
	}

	@Override
	public void open() throws Exception {
		super.open();

		function.open(new ExecutionContextImpl(this, getRuntimeContext()));

		this.numOfElements = 0;
		this.collector = new StreamRecordCollector<>(output);
		this.buffer = new HashMap<>();

		// create & restore state
		if (!finishBundleBeforeSnapshot) {
			TypeSerializer<V> valueSer = valueType.createSerializer(getExecutionConfig());
			//noinspection unchecked
			KeyedValueStateDescriptor<K, V> bufferStateDesc = new KeyedValueStateDescriptor<>(
					"globalBufferState",
					(TypeSerializer<K>) getKeySerializer(),
					valueSer);
			this.bufferState = getKeyedState(bufferStateDesc);
			// recovering buffer
			buffer.putAll(bufferState.getAll());
			bufferState.removeAll();
		}

		// recovering number
		numOfElements = buffer.size();

		bundleTrigger.registerBundleTriggerCallback(this);
		 //reset trigger
		bundleTrigger.reset();
		LOG.info("KeyedBundleOperator's trigger info: " + bundleTrigger.explain());

		// counter metric to get the size of bundle
		getRuntimeContext().getMetricGroup().gauge("bundleSize", (Gauge<Integer>) () -> numOfElements);
		getRuntimeContext().getMetricGroup().gauge("bundleRatio", (Gauge<Double>) () -> {
			int numOfKeys = buffer.size();
			if (numOfKeys == 0) {
				return 0.0;
			} else {
				return 1.0 * numOfElements / numOfKeys;
			}
		});
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Override
	public void processElement(StreamRecord<IN> element) throws Exception {
		while (isInFinishingBundle) {
			checkpointingLock.wait();
		}
		K key = (K) getCurrentKey();
		V value = buffer.get(key);  // maybe null
		V newValue = function.addInput(value, element.getValue());
		buffer.put(key, newValue);
		numOfElements++;
		bundleTrigger.onElement(element.getValue());
	}

	/** finish bundle and invoke BundleFunction. */
	@Override
	public void finishBundle() throws Exception {
		assert(Thread.holdsLock(checkpointingLock));
		while (isInFinishingBundle) {
			checkpointingLock.wait();
		}
		isInFinishingBundle = true;
		if (!buffer.isEmpty()) {
			numOfElements = 0;
			function.finishBundle(buffer, collector);
			buffer.clear();
		}
		// reset trigger
		bundleTrigger.reset();
		checkpointingLock.notifyAll();
		isInFinishingBundle = false;
	}

	@Override
	public void processWatermark(Watermark mark) throws Exception {
		while (isInFinishingBundle) {
			checkpointingLock.wait();
		}
		// bundle operator only used in unbounded group by which not need to handle watermark
		finishBundle();
		super.processWatermark(mark);
	}

	@Override
	public void endInput() throws Exception {

	}

	@Override
	public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
		while (isInFinishingBundle) {
			checkpointingLock.wait();
		}
		if (finishBundleBeforeSnapshot) {
			finishBundle();
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public void snapshotState(StateSnapshotContext context) throws Exception {
		while (isInFinishingBundle) {
			checkpointingLock.wait();
		}
		super.snapshotState(context);

		if (!finishBundleBeforeSnapshot) {
			// clear state first
			bufferState.removeAll();

			// update state
			bufferState.putAll(buffer);
		}
	}

	@Override
	public void close() throws Exception {
		assert(Thread.holdsLock(checkpointingLock));
		while (isInFinishingBundle) {
			checkpointingLock.wait();
		}
		try {
			finishBundle();
			function.endInput(collector);
		} finally {
			Exception exception = null;

			try {
				super.close();
				if (function != null) {
					function.close();
				}
			} catch (InterruptedException interrupted) {
				exception = interrupted;

				Thread.currentThread().interrupt();
			} catch (Exception e) {
				exception = e;
			}

			if (exception != null) {
				LOG.warn("Errors occurred while closing the BundleOperator.", exception);
			}
		}
	}

	@Override
	public boolean requireState() {
		// always requireState
		return true;
	}
}
