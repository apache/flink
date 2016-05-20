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
import org.apache.flink.util.AbstractID;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.metrics.groups.JobMetricGroup.DEFAULT_SCOPE_JOB;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Special {@link org.apache.flink.metrics.MetricGroup} representing a Flink runtime Task.
 * 
 * <p>Contains extra logic for adding operators.
 */
@Internal
public class TaskMetricGroup extends ComponentMetricGroup {
	
	public static final String SCOPE_TASK_DESCRIPTOR = "task";
	public static final String SCOPE_TASK_ID = Scope.format("task_id");
	public static final String SCOPE_TASK_NAME = Scope.format("task_name");
	public static final String SCOPE_TASK_ATTEMPT = Scope.format("task_attempt");
	public static final String SCOPE_TASK_SUBTASK_INDEX = Scope.format("subtask_index");
	public static final String DEFAULT_SCOPE_TASK_COMPONENT = SCOPE_TASK_NAME;
	public static final String DEFAULT_SCOPE_TASK = Scope.concat(DEFAULT_SCOPE_JOB, DEFAULT_SCOPE_TASK_COMPONENT);


	private final Map<String, OperatorMetricGroup> operators = new HashMap<>();

	private final IOMetricGroup ioMetrics;

	private final AbstractID executionId;
	
	private final int subtaskIndex;

	protected TaskMetricGroup(
			MetricRegistry registry,
			JobMetricGroup parent,
			AbstractID taskId,
			AbstractID executionId,
			int subtaskIndex,
			String name) {

		super(registry, parent, registry.getScopeConfig().getTaskFormat());

		this.executionId = executionId;
		this.subtaskIndex = subtaskIndex;
		this.ioMetrics = new IOMetricGroup(registry, this);
		
		this.formats.put(SCOPE_TASK_ID, taskId.toString());
		this.formats.put(SCOPE_TASK_ATTEMPT, executionId.toString());
		this.formats.put(SCOPE_TASK_NAME, checkNotNull(name));
		this.formats.put(SCOPE_TASK_SUBTASK_INDEX, String.valueOf(subtaskIndex));
	}

	public OperatorMetricGroup addOperator(String name) {
		OperatorMetricGroup operator = new OperatorMetricGroup(this.registry, this, name, this.subtaskIndex);

		synchronized (this) {
			OperatorMetricGroup previous = operators.put(name, operator);
			if (previous == null) {
				// no operator group so far
				return operator;
			} else {
				// already had an operator group. restore that one.
				operators.put(name, previous);
				return previous;
			}
		}
	}

	@Override
	public void close() {
		super.close();
		parent().removeTaskMetricGroup(executionId);
	}

	/**
	 * Returns the IOMetricGroup for this task.
	 *
	 * @return IOMetricGroup for this task.
	 */
	public IOMetricGroup getIOMetricGroup() {
		return this.ioMetrics;
	}

	@Override
	protected JobMetricGroup parent() {
		return (JobMetricGroup) super.parent();
	}

	@Override
	protected String getScopeFormat(Scope.ScopeFormat format) {
		return format.getTaskFormat();
	}

	@Override
	protected Iterable<? extends ComponentMetricGroup> subComponents() {
		return operators.values();
	}
}
