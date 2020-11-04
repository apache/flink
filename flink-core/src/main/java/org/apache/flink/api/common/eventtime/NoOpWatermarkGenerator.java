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

package org.apache.flink.api.common.eventtime;

import org.apache.flink.annotation.Public;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An implementation of a {@link WatermarkGenerator} that periodically emits watermarks
 * or generates only one final watermark at the end of input.
 */
@Public
public final class NoOpWatermarkGenerator<E> implements WatermarkGenerator<E> {

	private final WatermarkGenerator<E> watermarkGenerator;

	private final boolean emitProgressiveWatermarks;

	/**
	 * Creates a new watermark generator with the given watermark generator and emit progressive watermarks flag.
	 *
	 * @param watermarkGenerator        The watermark generator of the operator.
	 * @param emitProgressiveWatermarks Whether to periodically emit watermarks or only one final watermark at the end of input.
	 */
	public NoOpWatermarkGenerator(WatermarkGenerator<E> watermarkGenerator,
			boolean emitProgressiveWatermarks) {
		checkNotNull(watermarkGenerator, "watermarkGenerator");

		this.watermarkGenerator = watermarkGenerator;
		this.emitProgressiveWatermarks = emitProgressiveWatermarks;
	}

	@Override
	public void onEvent(E event, long eventTimestamp, WatermarkOutput output) {
		if (emitProgressiveWatermarks) {
			watermarkGenerator.onEvent(event, eventTimestamp, output);
		}
	}

	@Override
	public void onPeriodicEmit(WatermarkOutput output) {
		if (emitProgressiveWatermarks) {
			watermarkGenerator.onPeriodicEmit(output);
		}
	}
}
