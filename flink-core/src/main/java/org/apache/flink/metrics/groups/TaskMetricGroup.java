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
import org.apache.flink.metrics.groups.scope.ScopeFormat.TaskScopeFormat;
import org.apache.flink.util.AbstractID;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Special {@link org.apache.flink.metrics.MetricGroup} representing a Flink runtime Task.
 * 
 * <p>Contains extra logic for adding operators.
 */
@Internal
public class TaskMetricGroup extends ComponentMetricGroup {

	/** The job metrics group containing this task metrics group */
	private final TaskManagerJobMetricGroup parent;

	private final Map<String, OperatorMetricGroup> operators = new HashMap<>();

	private final IOMetricGroup ioMetrics;
	
	/** The execution Id uniquely identifying the executed task represented by this metrics group */
	private final AbstractID executionId;

	@Nullable
	private final AbstractID vertexId;
	
	@Nullable
	private final String taskName;

	private final int subtaskIndex;

	private final int attemptNumber;

	// ------------------------------------------------------------------------

	public TaskMetricGroup(
			MetricRegistry registry,
			TaskManagerJobMetricGroup parent,
			@Nullable AbstractID vertexId,
			AbstractID executionId,
			@Nullable String taskName,
			int subtaskIndex,
			int attemptNumber) {
		
		this(registry, parent, registry.getScopeFormats().getTaskFormat(),
				vertexId, executionId, taskName, subtaskIndex, attemptNumber);
	}

	public TaskMetricGroup(
			MetricRegistry registry,
			TaskManagerJobMetricGroup parent,
			TaskScopeFormat scopeFormat, 
			@Nullable AbstractID vertexId,
			AbstractID executionId,
			@Nullable String taskName,
			int subtaskIndex,
			int attemptNumber) {

		super(registry, scopeFormat.formatScope(
				parent, vertexId, executionId, taskName, subtaskIndex, attemptNumber));

		this.parent = checkNotNull(parent);
		this.executionId = checkNotNull(executionId);
		this.vertexId = vertexId;
		this.taskName = taskName;
		this.subtaskIndex = subtaskIndex;
		this.attemptNumber = attemptNumber;

		this.ioMetrics = new IOMetricGroup(registry, this);
	}

	// ------------------------------------------------------------------------
	//  properties
	// ------------------------------------------------------------------------

	public final TaskManagerJobMetricGroup parent() {
		return parent;
	}

	public AbstractID executionId() {
		return executionId;
	}

	@Nullable
	public AbstractID vertexId() {
		return vertexId;
	}

	@Nullable
	public String taskName() {
		return taskName;
	}

	public int subtaskIndex() {
		return subtaskIndex;
	}

	public int attemptNumber() {
		return attemptNumber;
	}

	/**
	 * Returns the IOMetricGroup for this task.
	 *
	 * @return IOMetricGroup for this task.
	 */
	public IOMetricGroup getIOMetricGroup() {
		return ioMetrics;
	}

	// ------------------------------------------------------------------------
	//  operators and cleanup
	// ------------------------------------------------------------------------
	public OperatorMetricGroup addOperator(String name) {
		OperatorMetricGroup operator = new OperatorMetricGroup(this.registry, this, name);

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
		parent.removeTaskMetricGroup(executionId);
	}

	// ------------------------------------------------------------------------

	@Override
	protected Iterable<? extends ComponentMetricGroup> subComponents() {
		return operators.values();
	}
}
