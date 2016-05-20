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

import java.util.Collections;

import static org.apache.flink.metrics.groups.JobMetricGroup.DEFAULT_SCOPE_JOB;

/**
 * Special {@link org.apache.flink.metrics.MetricGroup} representing an Operator.
 */
@Internal
public class OperatorMetricGroup extends ComponentMetricGroup {

	public static final String SCOPE_OPERATOR_DESCRIPTOR = "operator";
	public static final String SCOPE_OPERATOR_NAME = Scope.format("operator_name");
	public static final String SCOPE_OPERATOR_SUBTASK_INDEX = Scope.format("subtask_index");
	public static final String DEFAULT_SCOPE_OPERATOR_COMPONENT = Scope.concat(SCOPE_OPERATOR_NAME, SCOPE_OPERATOR_SUBTASK_INDEX);
	public static final String DEFAULT_SCOPE_OPERATOR = Scope.concat(DEFAULT_SCOPE_JOB, DEFAULT_SCOPE_OPERATOR_COMPONENT);

	protected OperatorMetricGroup(MetricRegistry registry, TaskMetricGroup task, String name, int subTaskIndex) {
		super(registry, task, registry.getScopeConfig().getOperatorFormat());

		this.formats.put(SCOPE_OPERATOR_NAME, name);
		this.formats.put(SCOPE_OPERATOR_SUBTASK_INDEX, String.valueOf(subTaskIndex));
	}

	// ------------------------------------------------------------------------

	@Override
	protected String getScopeFormat(Scope.ScopeFormat format) {
		return format.getOperatorFormat();
	}

	@Override
	protected Iterable<? extends ComponentMetricGroup> subComponents() {
		return Collections.emptyList();
	}
}
