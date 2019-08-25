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

package org.apache.flink.streaming.connectors.kafka.testutils;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * Test {@link org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext}.
 */
public class TestSourceContext<T> implements SourceFunction.SourceContext<T> {

	private final Object checkpointLock = new Object();
	private final Object watermarkLock = new Object();

	private volatile StreamRecord<T> latestElement;
	private volatile Watermark currentWatermark;

	@Override
	public void collect(T element) {
		this.latestElement = new StreamRecord<>(element);
	}

	@Override
	public void collectWithTimestamp(T element, long timestamp) {
		this.latestElement = new StreamRecord<>(element, timestamp);
	}

	@Override
	public void emitWatermark(Watermark mark) {
		synchronized (watermarkLock) {
			currentWatermark = mark;
			watermarkLock.notifyAll();
		}
	}

	@Override
	public void markAsTemporarilyIdle() {
		// do nothing
	}

	@Override
	public Object getCheckpointLock() {
		return checkpointLock;
	}

	@Override
	public void close() {
		// do nothing
	}

	public StreamRecord<T> getLatestElement() {
		return latestElement;
	}

	public boolean hasWatermark() {
		return currentWatermark != null;
	}

	public Watermark getLatestWatermark() throws InterruptedException {
		synchronized (watermarkLock) {
			while (currentWatermark == null) {
				watermarkLock.wait();
			}
			Watermark wm = currentWatermark;
			currentWatermark = null;
			return wm;
		}
	}
}
