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

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricRegistry;

import java.util.List;

/**
 * Special {@link org.apache.flink.metrics.MetricGroup} that contains shareable pre-defined IO-related metrics.
 */
public class IOMetricGroup extends AbstractMetricGroup {
	
	private final TaskMetricGroup parent;

	private final Counter numBytesIn;
	private final Counter numBytesOut;
	private final Counter numRecordsIn;
	private final Counter numRecordsOut;

	public IOMetricGroup(MetricRegistry registry, TaskMetricGroup parent) {
		super(registry);
		this.parent = parent;
		this.numBytesIn = parent.counter("numBytesIn");
		this.numBytesOut = parent.counter("numBytesOut");
		this.numRecordsIn = parent.counter("numRecordsIn");
		this.numRecordsOut = parent.counter("numRecordsOut");
	}

	@Override
	public List<String> generateScope() {
		return parent.generateScope();
	}

	@Override
	public List<String> generateScope(Scope.ScopeFormat format) {
		return parent.generateScope(format);
	}

	public Counter getBytesInCounter() {
		return this.numBytesIn;
	}

	public Counter getBytesOutCounter() {
		return this.numBytesOut;
	}

	public Counter getRecordsInCounter() {
		return this.numRecordsIn;
	}

	public Counter getRecordsOutCounter() {
		return this.numRecordsOut;
	}
}
