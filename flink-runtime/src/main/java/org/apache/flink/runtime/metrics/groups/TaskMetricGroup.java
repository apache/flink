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

import org.apache.flink.annotation.Internal;
import org.apache.flink.metrics.CharacterFilter;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.dump.QueryScopeInfo;
import org.apache.flink.runtime.metrics.scope.ScopeFormat;
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
public class TaskMetricGroup extends ComponentMetricGroup<TaskManagerJobMetricGroup> {

	private final Map<OperatorID, OperatorMetricGroup> operators = new HashMap<>();

	static final int METRICS_OPERATOR_NAME_MAX_LENGTH = 80;

	private final TaskIOMetricGroup ioMetrics;

	/** The execution Id uniquely identifying the executed task represented by this metrics group. */
	private final AbstractID executionId;

	@Nullable
	protected final JobVertexID vertexId;

	@Nullable
	private final String taskName;

	protected final int subtaskIndex;

	private final int attemptNumber;

	// ------------------------------------------------------------------------

	public TaskMetricGroup(
			MetricRegistry registry,
			TaskManagerJobMetricGroup parent,
			@Nullable JobVertexID vertexId,
			AbstractID executionId,
			@Nullable String taskName,
			int subtaskIndex,
			int attemptNumber) {
		super(registry, registry.getScopeFormats().getTaskFormat().formatScope(
			checkNotNull(parent), vertexId, checkNotNull(executionId), taskName, subtaskIndex, attemptNumber), parent);

		this.executionId = checkNotNull(executionId);
		this.vertexId = vertexId;
		this.taskName = taskName;
		this.subtaskIndex = subtaskIndex;
		this.attemptNumber = attemptNumber;

		this.ioMetrics = new TaskIOMetricGroup(this);
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
	 * Returns the TaskIOMetricGroup for this task.
	 *
	 * @return TaskIOMetricGroup for this task.
	 */
	public TaskIOMetricGroup getIOMetricGroup() {
		return ioMetrics;
	}

	@Override
	protected QueryScopeInfo.TaskQueryScopeInfo createQueryServiceMetricInfo(CharacterFilter filter) {
		return new QueryScopeInfo.TaskQueryScopeInfo(
			this.parent.jobId.toString(),
			String.valueOf(this.vertexId),
			this.subtaskIndex);
	}

	// ------------------------------------------------------------------------
	//  operators and cleanup
	// ------------------------------------------------------------------------

	public OperatorMetricGroup addOperator(String name) {
		return addOperator(OperatorID.fromJobVertexID(vertexId), name);
	}

	public OperatorMetricGroup addOperator(OperatorID operatorID, String name) {
		if (name != null && name.length() > METRICS_OPERATOR_NAME_MAX_LENGTH) {
			LOG.warn("The operator name {} exceeded the {} characters length limit and was truncated.", name, METRICS_OPERATOR_NAME_MAX_LENGTH);
			name = name.substring(0, METRICS_OPERATOR_NAME_MAX_LENGTH);
		}
		OperatorMetricGroup operator = new OperatorMetricGroup(this.registry, this, operatorID, name);

		synchronized (this) {
			OperatorMetricGroup previous = operators.put(operatorID, operator);
			if (previous == null) {
				// no operator group so far
				return operator;
			} else {
				// already had an operator group. restore that one.
				operators.put(operatorID, previous);
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
	//  Component Metric Group Specifics
	// ------------------------------------------------------------------------

	@Override
	protected void putVariables(Map<String, String> variables) {
		variables.put(ScopeFormat.SCOPE_TASK_VERTEX_ID, vertexId.toString());
		variables.put(ScopeFormat.SCOPE_TASK_NAME, taskName);
		variables.put(ScopeFormat.SCOPE_TASK_ATTEMPT_ID, executionId.toString());
		variables.put(ScopeFormat.SCOPE_TASK_ATTEMPT_NUM, String.valueOf(attemptNumber));
		variables.put(ScopeFormat.SCOPE_TASK_SUBTASK_INDEX, String.valueOf(subtaskIndex));
	}

	@Override
	protected Iterable<? extends ComponentMetricGroup> subComponents() {
		return operators.values();
	}

	@Override
	protected String getGroupName(CharacterFilter filter) {
		return "task";
	}
}
