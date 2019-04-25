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

package org.apache.flink.runtime.io.network.metrics;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.io.network.partition.consumer.LocalInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;
import org.apache.flink.runtime.metrics.MetricNames;

/**
 * Collects metrics for {@link RemoteInputChannel} and {@link LocalInputChannel}.
 */
public class InputChannelMetrics {

	private static final String IO_NUM_BYTES_IN_LOCAL = MetricNames.IO_NUM_BYTES_IN + "Local";
	private static final String IO_NUM_BYTES_IN_REMOTE = MetricNames.IO_NUM_BYTES_IN + "Remote";
	private static final String IO_NUM_BYTES_IN_LOCAL_RATE = IO_NUM_BYTES_IN_LOCAL + MetricNames.SUFFIX_RATE;
	private static final String IO_NUM_BYTES_IN_REMOTE_RATE = IO_NUM_BYTES_IN_REMOTE + MetricNames.SUFFIX_RATE;

	private static final String IO_NUM_BUFFERS_IN_LOCAL = MetricNames.IO_NUM_BUFFERS_IN + "Local";
	private static final String IO_NUM_BUFFERS_IN_REMOTE = MetricNames.IO_NUM_BUFFERS_IN + "Remote";
	private static final String IO_NUM_BUFFERS_IN_LOCAL_RATE = IO_NUM_BUFFERS_IN_LOCAL + MetricNames.SUFFIX_RATE;
	private static final String IO_NUM_BUFFERS_IN_REMOTE_RATE = IO_NUM_BUFFERS_IN_REMOTE + MetricNames.SUFFIX_RATE;

	private final Counter numBytesInLocal;
	private final Counter numBytesInRemote;
	private final Counter numBuffersInLocal;
	private final Counter numBuffersInRemote;

	public InputChannelMetrics(MetricGroup parent) {
		this.numBytesInLocal = parent.counter(IO_NUM_BYTES_IN_LOCAL);
		this.numBytesInRemote = parent.counter(IO_NUM_BYTES_IN_REMOTE);
		parent.meter(IO_NUM_BYTES_IN_LOCAL_RATE, new MeterView(numBytesInLocal, 60));
		parent.meter(IO_NUM_BYTES_IN_REMOTE_RATE, new MeterView(numBytesInRemote, 60));

		this.numBuffersInLocal = parent.counter(IO_NUM_BUFFERS_IN_LOCAL);
		this.numBuffersInRemote = parent.counter(IO_NUM_BUFFERS_IN_REMOTE);
		parent.meter(IO_NUM_BUFFERS_IN_LOCAL_RATE, new MeterView(numBuffersInLocal, 60));
		parent.meter(IO_NUM_BUFFERS_IN_REMOTE_RATE, new MeterView(numBuffersInRemote, 60));
	}

	public Counter getNumBytesInLocalCounter() {
		return numBytesInLocal;
	}

	public Counter getNumBytesInRemoteCounter() {
		return numBytesInRemote;
	}

	public Counter getNumBuffersInLocalCounter() {
		return numBuffersInLocal;
	}

	public Counter getNumBuffersInRemoteCounter() {
		return numBuffersInRemote;
	}
}
