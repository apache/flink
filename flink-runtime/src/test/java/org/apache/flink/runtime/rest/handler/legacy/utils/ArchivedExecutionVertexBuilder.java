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

package org.apache.flink.runtime.rest.handler.legacy.utils;

import org.apache.flink.runtime.executiongraph.ArchivedExecution;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionVertex;
import org.apache.flink.runtime.util.EvictingBoundedList;
import org.apache.flink.util.Preconditions;

import java.util.List;
import java.util.Random;

/**
 * Utility class for constructing an ArchivedExecutionVertex.
 */
public class ArchivedExecutionVertexBuilder {

	private static final Random RANDOM = new Random();

	private int subtaskIndex;
	private EvictingBoundedList<ArchivedExecution> priorExecutions;
	private String taskNameWithSubtask;
	private ArchivedExecution currentExecution;

	public ArchivedExecutionVertexBuilder setSubtaskIndex(int subtaskIndex) {
		this.subtaskIndex = subtaskIndex;
		return this;
	}

	public ArchivedExecutionVertexBuilder setPriorExecutions(List<ArchivedExecution> priorExecutions) {
		this.priorExecutions = new EvictingBoundedList<>(priorExecutions.size());
		for (ArchivedExecution execution : priorExecutions) {
			this.priorExecutions.add(execution);
		}
		return this;
	}

	public ArchivedExecutionVertexBuilder setTaskNameWithSubtask(String taskNameWithSubtask) {
		this.taskNameWithSubtask = taskNameWithSubtask;
		return this;
	}

	public ArchivedExecutionVertexBuilder setCurrentExecution(ArchivedExecution currentExecution) {
		this.currentExecution = currentExecution;
		return this;
	}

	public ArchivedExecutionVertex build() {
		Preconditions.checkNotNull(currentExecution);
		return new ArchivedExecutionVertex(
			subtaskIndex,
			taskNameWithSubtask != null ? taskNameWithSubtask : "task_" + RANDOM.nextInt() + "_" + subtaskIndex,
			currentExecution,
			priorExecutions != null ? priorExecutions : new EvictingBoundedList<ArchivedExecution>(0)
		);
	}
}
