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
import org.apache.flink.streaming.api.watermark.Watermark;

import java.util.concurrent.CompletableFuture;

/**
 * {@link StreamElementQueueEntry} implementation for the {@link Watermark}.
 */
@Internal
public class WatermarkQueueEntry extends StreamElementQueueEntry<Watermark> implements AsyncWatermarkResult {

	private final CompletableFuture<Watermark> future;

	public WatermarkQueueEntry(Watermark watermark) {
		super(watermark);

		this.future = CompletableFuture.completedFuture(watermark);
	}

	@Override
	public Watermark getWatermark() {
		return (Watermark) getStreamElement();
	}

	@Override
	protected CompletableFuture<Watermark> getFuture() {
		return future;
	}
}
