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
import org.apache.flink.metrics.MetricRegistry;

import java.util.List;

/**
 * A simple named {@link org.apache.flink.metrics.MetricGroup} with no special properties.
 */
@Internal
public class GenericMetricGroup extends AbstractMetricGroup {
	
	private final AbstractMetricGroup parent;

	private final String name;

	protected GenericMetricGroup(MetricRegistry registry, AbstractMetricGroup parent, int name) {
		this(registry, parent, String.valueOf(name));
	}

	protected GenericMetricGroup(MetricRegistry registry, AbstractMetricGroup parent, String name) {
		super(registry);
		this.parent = parent;
		this.name = name;
	}

	@Override
	public List<String> generateScope() {
		List<String> scope = parent.generateScope();
		scope.add(name);
		return scope;
	}

	@Override
	public List<String> generateScope(Scope.ScopeFormat format) {
		List<String> scope = parent.generateScope(format);
		scope.add(name);
		return scope;
	}
}
