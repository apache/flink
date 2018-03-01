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

package org.apache.flink.storm.metric;

import org.apache.flink.metrics.Counter;

import org.apache.storm.metric.api.CountMetric;

/**
 * An adapter to compose the counter metric between flink counter and storm
 * counter metric.
 * @since Mar 5, 2017
 */
public class CounterMetricAdapter extends CountMetric {

	private Counter delegate;

	public CounterMetricAdapter(final Counter counter) {
		this.delegate = counter;
	}

	public void incr() {
		delegate.inc();
	}

	public void incrBy(final long incrementBy) {
		delegate.inc(incrementBy);
	}

	public Long getValueAndReset() {
		long count = delegate.getCount();
		delegate.dec(delegate.getCount());
		return count;
	}

}
