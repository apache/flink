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

package org.apache.flink.streaming.api.operators.source;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

import javax.annotation.Nullable;

final class OnPeriodicTestWatermarkGenerator<T> implements WatermarkGenerator<T> {

	@Nullable
	private Long lastTimestamp;

	@Override
	public void onEvent(T event, long eventTimestamp, WatermarkOutput output) {
		lastTimestamp = eventTimestamp;
	}

	@Override
	public void onPeriodicEmit(WatermarkOutput output) {
		if (lastTimestamp != null) {
			output.emitWatermark(new Watermark(lastTimestamp));
		}
	}
}
