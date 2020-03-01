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

package org.apache.flink.streaming.api.operators.async.queue;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;

import java.util.Collection;

/**
 * {@link StreamElementQueueEntry} implementation for the {@link Watermark}.
 */
@Internal
class WatermarkQueueEntry<OUT> implements StreamElementQueueEntry<OUT> {
	@Nonnull
	private final Watermark watermark;

	WatermarkQueueEntry(Watermark watermark) {
		this.watermark = Preconditions.checkNotNull(watermark);
	}

	@Override
	public void emitResult(TimestampedCollector<OUT> output) {
		output.emitWatermark(watermark);
	}

	@Nonnull
	@Override
	public Watermark getInputElement() {
		return watermark;
	}

	@Override
	public boolean isDone() {
		return true;
	}

	@Override
	public void complete(Collection result) {
		throw new IllegalStateException("Cannot complete a watermark.");
	}
}
