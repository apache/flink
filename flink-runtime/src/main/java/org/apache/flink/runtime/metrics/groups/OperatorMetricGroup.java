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

package org.apache.flink.runtime.metrics.groups;

import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.scope.OperatorScopeFormat;

import java.util.Collections;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Special {@link org.apache.flink.metrics.MetricGroup} representing an Operator.
 */
public class OperatorMetricGroup extends ComponentMetricGroup {

	/** The task metric group that contains this operator metric groups */
	private final TaskMetricGroup parent;

	public OperatorMetricGroup(MetricRegistry registry, TaskMetricGroup parent, String operatorName) {
		this(registry, parent, registry.getScopeFormats().getOperatorFormat(), operatorName);
	}

	public OperatorMetricGroup(
			MetricRegistry registry,
			TaskMetricGroup parent,
			OperatorScopeFormat scopeFormat,
			String operatorName) {

		super(registry, scopeFormat.formatScope(parent, operatorName));
		this.parent = checkNotNull(parent);
	}

	// ------------------------------------------------------------------------
	
	public final TaskMetricGroup parent() {
		return parent;
	}
	
	// ------------------------------------------------------------------------
	//  Component Metric Group Specifics
	// ------------------------------------------------------------------------

	@Override
	protected Iterable<? extends ComponentMetricGroup> subComponents() {
		return Collections.emptyList();
	}
}
