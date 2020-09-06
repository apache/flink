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
import org.apache.flink.util.Preconditions;

/**
 * Collects metrics for {@link RemoteInputChannel} and {@link LocalInputChannel}.
 */
public class InputChannelMetrics {

	private static final String IO_NUM_BYTES_IN_LOCAL = MetricNames.IO_NUM_BYTES_IN + "Local";
	private static final String IO_NUM_BYTES_IN_REMOTE = MetricNames.IO_NUM_BYTES_IN + "Remote";
	private static final String IO_NUM_BUFFERS_IN_LOCAL = MetricNames.IO_NUM_BUFFERS_IN + "Local";
	private static final String IO_NUM_BUFFERS_IN_REMOTE = MetricNames.IO_NUM_BUFFERS_IN + "Remote";

	private final Counter numBytesInLocal;
	private final Counter numBytesInRemote;
	private final Counter numBuffersInLocal;
	private final Counter numBuffersInRemote;

	public InputChannelMetrics(MetricGroup ... parents) {
		this.numBytesInLocal = createCounter(IO_NUM_BYTES_IN_LOCAL, parents);
		this.numBytesInRemote = createCounter(IO_NUM_BYTES_IN_REMOTE, parents);
		this.numBuffersInLocal = createCounter(IO_NUM_BUFFERS_IN_LOCAL, parents);
		this.numBuffersInRemote = createCounter(IO_NUM_BUFFERS_IN_REMOTE, parents);
	}

	private static Counter createCounter(String name, MetricGroup ... parents) {
		Counter[] counters = new Counter[parents.length];
		for (int i = 0; i < parents.length; i++) {
			counters[i] = parents[i].counter(name);
			parents[i].meter(name + MetricNames.SUFFIX_RATE, new MeterView(counters[i]));
		}
		return new MultiCounterWrapper(counters);
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

	private static class MultiCounterWrapper implements Counter {
		private final Counter[] counters;

		private MultiCounterWrapper(Counter ... counters) {
			Preconditions.checkArgument(counters.length > 0);
			this.counters = counters;
		}

		@Override
		public void inc() {
			for (Counter c : counters) {
				c.inc();
			}
		}

		@Override
		public void inc(long n) {
			for (Counter c : counters) {
				c.inc(n);
			}
		}

		@Override
		public void dec() {
			for (Counter c : counters) {
				c.dec();
			}
		}

		@Override
		public void dec(long n) {
			for (Counter c : counters) {
				c.dec(n);
			}
		}

		@Override
		public long getCount() {
			// assume that the counters are not accessed directly elsewhere
			return counters[0].getCount();
		}
	}
}
