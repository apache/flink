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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.runtime.state.keyed.KeyedValueState;
import org.apache.flink.runtime.state.keyed.KeyedValueStateDescriptor;
import org.apache.flink.streaming.api.bundle.BundleTriggerCallback;
import org.apache.flink.streaming.api.bundle.CoBundleTrigger;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.TwoInputSelection;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.runtime.util.StreamRecordCollector;
import org.apache.flink.table.typeutils.AbstractRowSerializer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Used for MiniBatch Join.
 */
public abstract class KeyedCoBundleOperator
	extends AbstractStreamOperator<BaseRow>
	implements TwoInputStreamOperator<BaseRow, BaseRow, BaseRow>, BundleTriggerCallback {

	private static final String LEFT_STATE_NAME = "_keyed_co_bundle_operator_left_state_";

	private static final String RIGHT_STATE_NAME = "_keyed_co_bundle_operator_right_state_";

	private final CoBundleTrigger<BaseRow, BaseRow> coBundleTrigger;

	private final boolean finishBundleBeforeSnapshot;

	private transient Object checkpointingLock;

	private transient StreamRecordCollector<BaseRow> collector;

	private transient Map<BaseRow, List<BaseRow>> leftBuffer;
	private transient Map<BaseRow, List<BaseRow>> rightBuffer;

	private transient KeyedValueState<BaseRow, List<BaseRow>> leftBufferState;
	private transient KeyedValueState<BaseRow, List<BaseRow>> rightBufferState;

	// We keep track of watermarks from both inputs, the combined input is the minimum
	// Once the minimum advances we emit a new watermark for downstream operators
	private long input1Watermark = Long.MIN_VALUE;
	private long input2Watermark = Long.MIN_VALUE;
	private long currentWatermark = Long.MIN_VALUE;

	private TypeSerializer<BaseRow> lTypeSerializer;
	private TypeSerializer<BaseRow> rTypeSerializer;
	private transient volatile boolean isInFinishingBundle = false;
	private AbstractRowSerializer<BaseRow> inputSer1;
	private AbstractRowSerializer<BaseRow> inputSer2;

	public KeyedCoBundleOperator(CoBundleTrigger<BaseRow, BaseRow> coBundleTrigger, boolean finishBundleBeforeSnapshot) {
		this.finishBundleBeforeSnapshot = finishBundleBeforeSnapshot;
		Preconditions.checkNotNull(coBundleTrigger, "coBundleTrigger is null");
		this.coBundleTrigger = coBundleTrigger;
	}

	@SuppressWarnings("unchecked")
	@Override
	public TwoInputSelection processElement1(StreamRecord<BaseRow> element) throws Exception {
		while (isInFinishingBundle) {
			checkpointingLock.wait();
		}
		BaseRow key = (BaseRow) getCurrentKey();
		BaseRow row = inputSer1.copy(element.getValue());
		List<BaseRow> records = leftBuffer.computeIfAbsent(key, k -> new ArrayList<>());
		records.add(row);
		coBundleTrigger.onLeftElement(row);
		return TwoInputSelection.ANY;
	}

	@SuppressWarnings("unchecked")
	@Override
	public TwoInputSelection processElement2(StreamRecord<BaseRow> element) throws Exception {
		while (isInFinishingBundle) {
			checkpointingLock.wait();
		}
		BaseRow key = (BaseRow) getCurrentKey();
		BaseRow row = inputSer2.copy(element.getValue());
		List<BaseRow> records = rightBuffer.computeIfAbsent(key, k -> new ArrayList<>());
		records.add(row);
		coBundleTrigger.onRightElement(row);
		return TwoInputSelection.ANY;
	}

	@Override
	public void processWatermark1(Watermark mark) throws Exception {
		while (isInFinishingBundle) {
			checkpointingLock.wait();
		}
		input1Watermark = mark.getTimestamp();
		long newMin = Math.min(input1Watermark, input2Watermark);
		if (newMin > currentWatermark) {
			currentWatermark = newMin;
			finishBundle();
			output.emitWatermark(new Watermark(newMin));
		}
	}

	@Override
	public void processWatermark2(Watermark mark) throws Exception {
		while (isInFinishingBundle) {
			checkpointingLock.wait();
		}
		input2Watermark = mark.getTimestamp();
		long newMin = Math.min(input1Watermark, input2Watermark);
		if (newMin > currentWatermark) {
			currentWatermark = newMin;
			finishBundle();
			output.emitWatermark(new Watermark(newMin));
		}
	}

	@Override
	public void endInput1() throws Exception {
		while (isInFinishingBundle) {
			checkpointingLock.wait();
		}
		finishBundle();
	}

	@Override
	public void endInput2() throws Exception {
		while (isInFinishingBundle) {
			checkpointingLock.wait();
		}
		finishBundle();
	}

	@Override
	public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
		if (finishBundleBeforeSnapshot) {
			finishBundle();
		}
	}

	@Override
	public void finishBundle() throws Exception {
		synchronized (checkpointingLock) {
			while (isInFinishingBundle) {
				checkpointingLock.wait();
			}
			isInFinishingBundle = true;
			if (!leftBuffer.isEmpty() || !rightBuffer.isEmpty()) {
				this.processBundles(leftBuffer, rightBuffer, collector);
				leftBuffer.clear();
				rightBuffer.clear();
			}
			coBundleTrigger.reset();

			checkpointingLock.notifyAll();
			isInFinishingBundle = false;
		}
	}

	protected abstract void processBundles(
		Map<BaseRow, List<BaseRow>> left,
		Map<BaseRow, List<BaseRow>> right,
		Collector<BaseRow> out) throws Exception;

	@Override
	public void setup(StreamTask<?, ?> containingTask, StreamConfig config, Output<StreamRecord<BaseRow>> output) {
		super.setup(containingTask, config, output);
		this.checkpointingLock = getContainingTask().getCheckpointLock();
	}

	@VisibleForTesting
	public void setupTypeSerializer(TypeSerializer<BaseRow> lSerializer, TypeSerializer<BaseRow> rSerializer) {
		Objects.requireNonNull(lSerializer);
		Objects.requireNonNull(rSerializer);
		this.lTypeSerializer = lSerializer;
		this.rTypeSerializer = rSerializer;
	}

	@Override
	public void open() throws Exception {
		super.open();
		this.checkpointingLock = getContainingTask().getCheckpointLock();
		this.collector = new StreamRecordCollector<>(output);
		this.leftBuffer = new HashMap<>();
		this.rightBuffer = new HashMap<>();
		inputSer1 = (AbstractRowSerializer) (lTypeSerializer == null ?
				config.getTypeSerializerIn1(getUserCodeClassloader()) : lTypeSerializer);
		inputSer2 = (AbstractRowSerializer) (rTypeSerializer == null ?
				config.getTypeSerializerIn2(getUserCodeClassloader()) : rTypeSerializer);

		// create & restore state
		if (!finishBundleBeforeSnapshot) {
			// create & restore state
			//noinspection unchecked
			KeyedValueStateDescriptor<BaseRow, List<BaseRow>> leftBufferStateDesc = new KeyedValueStateDescriptor<>(
					LEFT_STATE_NAME,
					(TypeSerializer) getKeySerializer(),
					new ListSerializer<>(inputSer1));
			this.leftBufferState = getKeyedState(leftBufferStateDesc);
			this.leftBuffer.putAll(leftBufferState.getAll());
			leftBufferState.removeAll();

			//noinspection unchecked
			KeyedValueStateDescriptor<BaseRow, List<BaseRow>> rightBufferStateDesc = new KeyedValueStateDescriptor<>(
					RIGHT_STATE_NAME,
					(TypeSerializer) getKeySerializer(),
					new ListSerializer<>(inputSer2));
			this.rightBufferState = getKeyedState(rightBufferStateDesc);
		}

		coBundleTrigger.registerBundleTriggerCallback(this);
		coBundleTrigger.reset();

		LOG.info("KeyedCoBundleOperator's trigger info: " + coBundleTrigger.explain());
	}

	@Override
	public void close() throws Exception {
		while (isInFinishingBundle) {
			checkpointingLock.wait();
		}

		try {
			finishBundle();

		} finally {
			Exception exception = null;

			try {
				super.close();
			} catch (InterruptedException interrupted) {
				exception = interrupted;

				Thread.currentThread().interrupt();
			} catch (Exception e) {
				exception = e;
			}

			if (exception != null) {
				LOG.warn("Errors occurred while closing the KeyedCoBundleOperator.", exception);
			}
		}
	}

	@Override
	public void snapshotState(StateSnapshotContext context) throws Exception {
		while (isInFinishingBundle) {
			checkpointingLock.wait();
		}
		super.snapshotState(context);

		if (!finishBundleBeforeSnapshot) {
			// clear state first
			leftBufferState.removeAll();
			rightBufferState.removeAll();

			// update state
			leftBufferState.putAll(leftBuffer);
			rightBufferState.putAll(rightBuffer);
		}
	}

	@Override
	public boolean requireState() {
		// always requireState
		return true;
	}
}
