/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.operators;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * A stream operator that extracts timestamps from stream elements and
 * generates watermarks based on punctuation elements.
 *
 * @param <T> The type of the input elements
 */
public class TimestampsAndPunctuatedWatermarksOperator<T>
		extends AbstractUdfStreamOperator<T, AssignerWithPunctuatedWatermarks<T>>
		implements OneInputStreamOperator<T, T> {

	private static final long serialVersionUID = 1L;

	private long currentWatermark = Long.MIN_VALUE;

	private static final String WATERMARK_STATE_NAME = "punctuated-watermark-states";

	private ListState<Long> restoreWatermarks;

	public TimestampsAndPunctuatedWatermarksOperator(AssignerWithPunctuatedWatermarks<T> assigner) {
		super(assigner);
		this.chainingStrategy = ChainingStrategy.ALWAYS;
	}

	@Override
	public void processElement(StreamRecord<T> element) throws Exception {
		final T value = element.getValue();
		final long newTimestamp = userFunction.extractTimestamp(value,
				element.hasTimestamp() ? element.getTimestamp() : Long.MIN_VALUE);

		output.collect(element.replace(element.getValue(), newTimestamp));

		final Watermark nextWatermark = userFunction.checkAndGetNextWatermark(value, newTimestamp);
		if (nextWatermark != null && nextWatermark.getTimestamp() > currentWatermark) {
			currentWatermark = nextWatermark.getTimestamp();
			output.emitWatermark(nextWatermark);
		}
	}

	/**
	 * Override the base implementation to completely ignore watermarks propagated from
	 * upstream (we rely only on the {@link AssignerWithPunctuatedWatermarks} to emit
	 * watermarks from here).
	 */
	@Override
	public void processWatermark(Watermark mark) throws Exception {
		// if we receive a Long.MAX_VALUE watermark we forward it since it is used
		// to signal the end of input and to not block watermark progress downstream
		if (mark.getTimestamp() == Long.MAX_VALUE && currentWatermark != Long.MAX_VALUE) {
			currentWatermark = Long.MAX_VALUE;
			output.emitWatermark(mark);
		}
	}

	@Override
	public void snapshotState(StateSnapshotContext context) throws Exception {
		super.snapshotState(context);
		restoreWatermarks.clear();
		restoreWatermarks.add(currentWatermark);
	}

	@Override
	public void initializeState(StateInitializationContext context) throws Exception {
		super.initializeState(context);

		OperatorStateStore operatorStateStore = context.getOperatorStateStore();
		restoreWatermarks = operatorStateStore.getUnionListState(new ListStateDescriptor<>(WATERMARK_STATE_NAME,
			BasicTypeInfo.LONG_TYPE_INFO));

		// find the lowest watermark
		if (context.isRestored()) {
			long lowestWatermark = Long.MIN_VALUE;
			for (Long watermark : restoreWatermarks.get()) {
				if (watermark > lowestWatermark) {
					lowestWatermark = watermark;
				}
			}
			currentWatermark = lowestWatermark;
			LOG.info("Find restore state for TimestampsAndPunctuatedWatermarksOperator.");
		} else {
			currentWatermark = Long.MIN_VALUE;
			LOG.info("No restore state for TimestampsAndPunctuatedWatermarksOperator.");
		}
		output.emitWatermark(new Watermark(currentWatermark));
	}
}
