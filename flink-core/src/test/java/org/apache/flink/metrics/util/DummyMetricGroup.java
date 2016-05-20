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
package org.apache.flink.metrics.util;

import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.AbstractMetricGroup;
import org.apache.flink.metrics.groups.Scope;

import java.util.ArrayList;
import java.util.List;

public class DummyMetricGroup extends AbstractMetricGroup {
	public DummyMetricGroup() {
		super(new DummyMetricRegistry());
	}

	@Override
	public List<String> generateScope() {
		return new ArrayList<>();
	}

	@Override
	public List<String> generateScope(Scope.ScopeFormat format) {
		return new ArrayList<>();
	}

	@Override
	protected MetricGroup addMetric(String name, Metric metric) {
		return this;
	}

	@Override
	public MetricGroup addGroup(int name) {
		return addGroup("" + name);
	}

	@Override
	public MetricGroup addGroup(String name) {
		return new DummyMetricGroup();
	}
}
