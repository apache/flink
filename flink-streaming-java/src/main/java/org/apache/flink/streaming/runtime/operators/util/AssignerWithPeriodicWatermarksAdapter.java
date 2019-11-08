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

package org.apache.flink.streaming.runtime.operators.util;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An adapter that wraps a {@link AssignerWithPeriodicWatermarks} into a
 * {@link WatermarkGenerator}.
 */
@Internal
@SuppressWarnings("deprecation")
public final class AssignerWithPeriodicWatermarksAdapter<T> implements WatermarkGenerator<T> {

	private final AssignerWithPeriodicWatermarks<T> wms;

	public AssignerWithPeriodicWatermarksAdapter(AssignerWithPeriodicWatermarks<T> wms) {
		this.wms = checkNotNull(wms);
	}

	@Override
	public void onEvent(T event, long eventTimestamp, WatermarkOutput output) {}

	@Override
	public void onPeriodicEmit(WatermarkOutput output) {
		final org.apache.flink.streaming.api.watermark.Watermark next = wms.getCurrentWatermark();
		if (next != null) {
			output.emitWatermark(new Watermark(next.getTimestamp()));
		}
	}

	// ------------------------------------------------------------------------

	/**
	 * A WatermarkStrategy that returns an {@link AssignerWithPeriodicWatermarks} wrapped as a
	 * {@link WatermarkGenerator}.
	 */
	public static final class Strategy<T> implements WatermarkStrategy<T> {
		private static final long serialVersionUID = 1L;

		private final AssignerWithPeriodicWatermarks<T> wms;

		public Strategy(AssignerWithPeriodicWatermarks<T> wms) {
			this.wms = checkNotNull(wms);
		}

		@Override
		public WatermarkGenerator<T> createWatermarkGenerator() {
			return new AssignerWithPeriodicWatermarksAdapter<>(wms);
		}
	}
}
