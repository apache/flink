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

package org.apache.flink.metrics.groups;

import org.apache.flink.annotation.Internal;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;

/**
 * A special {@link MetricGroup} that does not register any metrics at the metrics registry
 * and any reporters.
 * 
 * <p>This metrics group appears always closed ({@link #isClosed()}).
 */
@Internal
public class UnregisteredMetricsGroup implements MetricGroup {

	@Override
	public void close() {}

	@Override
	public boolean isClosed() {
		return true;
	}

	@Override
	public Counter counter(int name) {
		return new Counter();
	}

	@Override
	public Counter counter(String name) {
		return new Counter();
	}

	@Override
	public <T> Gauge<T> gauge(int name, Gauge<T> gauge) {
		return gauge;
	}

	@Override
	public <T> Gauge<T> gauge(String name, Gauge<T> gauge) {
		return gauge;
	}

	
	@Override
	public MetricGroup addGroup(int name) {
		return addGroup(String.valueOf(name));
	}

	@Override
	public MetricGroup addGroup(String name) {
		return new UnregisteredMetricsGroup();
	}
}
