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

package org.apache.flink.runtime.metrics.scope;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.metrics.groups.TaskManagerMetricGroup;

/**
 * The scope format for the {@link org.apache.flink.runtime.metrics.groups.JobMetricGroup}.
 */
public class TaskManagerJobScopeFormat extends ScopeFormat {

	public TaskManagerJobScopeFormat(String format, TaskManagerScopeFormat parentFormat) {
		super(format, parentFormat, new String[] {
				SCOPE_ACTOR_HOST,
				SCOPE_TASKMANAGER_ID,
				SCOPE_JOB_ID,
				SCOPE_JOB_NAME
		});
	}

	public String[] formatScope(TaskManagerMetricGroup parent, JobID jid, String jobName) {
		final String[] template = copyTemplate();
		final String[] values = {
				parent.hostname(),
				parent.taskManagerId(),
				valueOrNull(jid),
				valueOrNull(jobName)
		};
		return bindVariables(template, values);
	}
}
