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

package org.apache.flink.metrics.groups.scope;

import org.apache.flink.api.common.JobID;
import org.apache.flink.metrics.CharacterFilter;
import org.apache.flink.metrics.groups.JobManagerMetricGroup;
import org.apache.flink.metrics.groups.JobMetricGroup;
import org.apache.flink.metrics.groups.TaskManagerJobMetricGroup;
import org.apache.flink.metrics.groups.TaskManagerMetricGroup;
import org.apache.flink.metrics.groups.TaskMetricGroup;
import org.apache.flink.util.AbstractID;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This class represents the format after which the "scope" (or namespace) of the various
 * component metric groups is built. Component metric groups
 * ({@link org.apache.flink.metrics.groups.ComponentMetricGroup}), are for example
 * "TaskManager", "Task", or "Operator".
 *
 * <p>User defined scope formats allow users to include or exclude
 * certain identifiers from the scope. The scope for metrics belonging to the "Task"
 * group could for example include the task attempt number (more fine grained identification), or
 * exclude it (continuity of the namespace across failure and recovery).
 */
public abstract class ScopeFormat {

	private static CharacterFilter defaultFilter = new CharacterFilter() {
		@Override
		public String filterCharacters(String input) {
			return input;
		}
	};

	// ------------------------------------------------------------------------
	//  Scope Format Special Characters
	// ------------------------------------------------------------------------

	/**
	 * If the scope format starts with this character, then the parent components scope
	 * format will be used as a prefix.
	 * 
	 * <p>For example, if the {@link JobMetricGroup} format is {@code "*.<job_name>"}, and the
	 * {@link TaskManagerMetricGroup} format is {@code "<host>"}, then the job's metrics
	 * will have {@code "<host>.<job_name>"} as their scope.
	 */
	public static final String SCOPE_INHERIT_PARENT = "*";

	public static final String SCOPE_SEPARATOR = ".";

	private static final String SCOPE_VARIABLE_PREFIX = "<";
	private static final String SCOPE_VARIABLE_SUFFIX = ">";

	// ------------------------------------------------------------------------
	//  Scope Variables
	// ------------------------------------------------------------------------

	public static final String SCOPE_ACTOR_HOST = asVariable("host");

	// ----- Job Manager ----

	/** The default scope format of the JobManager component: {@code "<host>.jobmanager"} */
	public static final String DEFAULT_SCOPE_JOBMANAGER_COMPONENT =
		concat(SCOPE_ACTOR_HOST, "jobmanager");

	/** The default scope format of JobManager metrics: {@code "<host>.jobmanager"} */
	public static final String DEFAULT_SCOPE_JOBMANAGER_GROUP = DEFAULT_SCOPE_JOBMANAGER_COMPONENT;

	// ----- Task Manager ----

	public static final String SCOPE_TASKMANAGER_ID = asVariable("tm_id");

	/** The default scope format of the TaskManager component: {@code "<host>.taskmanager.<tm_id>"} */
	public static final String DEFAULT_SCOPE_TASKMANAGER_COMPONENT =
			concat(SCOPE_ACTOR_HOST, "taskmanager", SCOPE_TASKMANAGER_ID);

	/** The default scope format of TaskManager metrics: {@code "<host>.taskmanager.<tm_id>"} */
	public static final String DEFAULT_SCOPE_TASKMANAGER_GROUP = DEFAULT_SCOPE_TASKMANAGER_COMPONENT;

	// ----- Job -----

	public static final String SCOPE_JOB_ID = asVariable("job_id");
	public static final String SCOPE_JOB_NAME = asVariable("job_name");

	/** The default scope format for the job component: {@code "<job_name>"} */
	public static final String DEFAULT_SCOPE_JOB_COMPONENT = SCOPE_JOB_NAME;

	// ----- Job on Job Manager ----

	/** The default scope format for all job metrics on a jobmanager: {@code "<host>.jobmanager.<job_name>"} */
	public static final String DEFAULT_SCOPE_JOBMANAGER_JOB_GROUP =
		concat(DEFAULT_SCOPE_JOBMANAGER_COMPONENT, DEFAULT_SCOPE_JOB_COMPONENT);

	// ----- Job on Task Manager ----

	/** The default scope format for all job metrics on a taskmanager: {@code "<host>.taskmanager.<tm_id>.<job_name>"} */
	public static final String DEFAULT_SCOPE_TASKMANAGER_JOB_GROUP =
			concat(DEFAULT_SCOPE_TASKMANAGER_COMPONENT, DEFAULT_SCOPE_JOB_COMPONENT);

	// ----- Task ----

	public static final String SCOPE_TASK_VERTEX_ID = asVariable("task_id");
	public static final String SCOPE_TASK_NAME = asVariable("task_name");
	public static final String SCOPE_TASK_ATTEMPT_ID = asVariable("task_attempt_id");
	public static final String SCOPE_TASK_ATTEMPT_NUM = asVariable("task_attempt_num");
	public static final String SCOPE_TASK_SUBTASK_INDEX = asVariable("subtask_index");

	/** Default scope of the task component: {@code "<task_name>.<subtask_index>"} */
	public static final String DEFAULT_SCOPE_TASK_COMPONENT =
			concat(SCOPE_TASK_NAME, SCOPE_TASK_SUBTASK_INDEX);

	/** The default scope format for all task metrics:
	 * {@code "<host>.taskmanager.<tm_id>.<job_name>.<task_name>.<subtask_index>"} */
	public static final String DEFAULT_SCOPE_TASK_GROUP =
			concat(DEFAULT_SCOPE_TASKMANAGER_JOB_GROUP, DEFAULT_SCOPE_TASK_COMPONENT);

	// ----- Operator ----

	public static final String SCOPE_OPERATOR_NAME = asVariable("operator_name");

	/** The default scope added by the operator component: "<operator_name>.<subtask_index>" */
	public static final String DEFAULT_SCOPE_OPERATOR_COMPONENT =
			concat(SCOPE_OPERATOR_NAME, SCOPE_TASK_SUBTASK_INDEX);

	/** The default scope format for all operator metrics:
	 * {@code "<host>.taskmanager.<tm_id>.<job_name>.<operator_name>.<subtask_index>"} */
	public static final String DEFAULT_SCOPE_OPERATOR_GROUP =
			concat(DEFAULT_SCOPE_TASKMANAGER_JOB_GROUP, DEFAULT_SCOPE_OPERATOR_COMPONENT);

	// ------------------------------------------------------------------------
	//  Formatters form the individual component types
	// ------------------------------------------------------------------------

	/**
	 * The scope format for the {@link JobManagerMetricGroup}.
	 */
	public static class JobManagerScopeFormat extends ScopeFormat {

		public JobManagerScopeFormat(String format) {
			super(format, null, new String[] {
				SCOPE_ACTOR_HOST
			});
		}

		public String[] formatScope(String hostname) {
			final String[] template = copyTemplate();
			final String[] values = { hostname };
			return bindVariables(template, values);
		}
	}

	/**
	 * The scope format for the {@link TaskManagerMetricGroup}.
	 */
	public static class TaskManagerScopeFormat extends ScopeFormat {

		public TaskManagerScopeFormat(String format) {
			super(format, null, new String[] {
					SCOPE_ACTOR_HOST,
					SCOPE_TASKMANAGER_ID
			});
		}

		public String[] formatScope(String hostname, String taskManagerId) {
			final String[] template = copyTemplate();
			final String[] values = { hostname, taskManagerId };
			return bindVariables(template, values);
		}
	}

	// ------------------------------------------------------------------------

	/**
	 * The scope format for the {@link JobMetricGroup}.
	 */
	public static class JobManagerJobScopeFormat extends ScopeFormat {

		public JobManagerJobScopeFormat(String format, JobManagerScopeFormat parentFormat) {
			super(format, parentFormat, new String[] {
				SCOPE_ACTOR_HOST,
				SCOPE_JOB_ID,
				SCOPE_JOB_NAME
			});
		}

		public String[] formatScope(JobManagerMetricGroup parent, JobID jid, String jobName) {
			final String[] template = copyTemplate();
			final String[] values = {
				parent.hostname(),
				valueOrNull(jid),
				valueOrNull(jobName)
			};
			return bindVariables(template, values);
		}
	}

	/**
	 * The scope format for the {@link JobMetricGroup}.
	 */
	public static class TaskManagerJobScopeFormat extends ScopeFormat {

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

	// ------------------------------------------------------------------------

	/**
	 * The scope format for the {@link TaskMetricGroup}.
	 */
	public static class TaskScopeFormat extends ScopeFormat {

		public TaskScopeFormat(String format, TaskManagerJobScopeFormat parentFormat) {
			super(format, parentFormat, new String[] {
					SCOPE_ACTOR_HOST,
					SCOPE_TASKMANAGER_ID,
					SCOPE_JOB_ID,
					SCOPE_JOB_NAME,
					SCOPE_TASK_VERTEX_ID,
					SCOPE_TASK_ATTEMPT_ID,
					SCOPE_TASK_NAME,
					SCOPE_TASK_SUBTASK_INDEX,
					SCOPE_TASK_ATTEMPT_NUM
			});
		}

		public String[] formatScope(
				TaskManagerJobMetricGroup parent,
				AbstractID vertexId, AbstractID attemptId,
				String taskName, int subtask, int attemptNumber) {

			final String[] template = copyTemplate();
			final String[] values = {
					parent.parent().hostname(),
					parent.parent().taskManagerId(),
					valueOrNull(parent.jobId()),
					valueOrNull(parent.jobName()),
					valueOrNull(vertexId),
					valueOrNull(attemptId),
					valueOrNull(taskName),
					String.valueOf(subtask),
					String.valueOf(attemptNumber)
			};
			return bindVariables(template, values);
		}
	}

	// ------------------------------------------------------------------------

	/**
	 * The scope format for the {@link org.apache.flink.metrics.groups.OperatorMetricGroup}.
	 */
	public static class OperatorScopeFormat extends ScopeFormat {

		public OperatorScopeFormat(String format, TaskScopeFormat parentFormat) {
			super(format, parentFormat, new String[] {
					SCOPE_ACTOR_HOST,
					SCOPE_TASKMANAGER_ID,
					SCOPE_JOB_ID,
					SCOPE_JOB_NAME,
					SCOPE_TASK_VERTEX_ID,
					SCOPE_TASK_ATTEMPT_ID,
					SCOPE_TASK_NAME,
					SCOPE_TASK_SUBTASK_INDEX,
					SCOPE_TASK_ATTEMPT_NUM,
					SCOPE_OPERATOR_NAME
			});
		}

		public String[] formatScope(TaskMetricGroup parent, String operatorName) {

			final String[] template = copyTemplate();
			final String[] values = {
					parent.parent().parent().hostname(),
					parent.parent().parent().taskManagerId(),
					valueOrNull(parent.parent().jobId()),
					valueOrNull(parent.parent().jobName()),
					valueOrNull(parent.vertexId()),
					valueOrNull(parent.executionId()),
					valueOrNull(parent.taskName()),
					String.valueOf(parent.subtaskIndex()),
					String.valueOf(parent.attemptNumber()),
					valueOrNull(operatorName)
			};
			return bindVariables(template, values);
		}
	}

	// ------------------------------------------------------------------------
	//  Scope Format Base
	// ------------------------------------------------------------------------

	/** The scope format */
	private final String format;

	/** The format, split into components */
	private final String[] template;

	private final int[] templatePos;

	private final int[] valuePos;

	// ------------------------------------------------------------------------

	protected ScopeFormat(String format, ScopeFormat parent, String[] variables) {
		checkNotNull(format, "format is null");

		final String[] rawComponents = format.split("\\" + SCOPE_SEPARATOR);

		// compute the template array
		final boolean parentAsPrefix = rawComponents.length > 0 && rawComponents[0].equals(SCOPE_INHERIT_PARENT);
		if (parentAsPrefix) {
			if (parent == null) {
				throw new IllegalArgumentException("Component scope format requires parent prefix (starts with '"
					+ SCOPE_INHERIT_PARENT + "'), but this component has no parent (is root component).");
			}

			this.format = format.length() > 2 ? format.substring(2) : "<empty>";

			String[] parentTemplate = parent.template;
			int parentLen = parentTemplate.length;
			
			this.template = new String[parentLen + rawComponents.length - 1];
			System.arraycopy(parentTemplate, 0, this.template, 0, parentLen);
			System.arraycopy(rawComponents, 1, this.template, parentLen, rawComponents.length - 1);
		}
		else {
			this.format = format.isEmpty() ? "<empty>" : format;
			this.template = rawComponents;
		}

		// --- compute the replacement matrix ---
		// a bit of clumsy Java collections code ;-)
		
		HashMap<String, Integer> varToValuePos = arrayToMap(variables);
		List<Integer> templatePos = new ArrayList<>();
		List<Integer> valuePos = new ArrayList<>();

		for (int i = 0; i < template.length; i++) {
			final String component = template[i];
			
			// check if that is a variable
			if (component != null && component.length() >= 3 &&
					component.charAt(0) == '<' && component.charAt(component.length() - 1) == '>') {

				// this is a variable
				Integer replacementPos = varToValuePos.get(component);
				if (replacementPos != null) {
					templatePos.add(i);
					valuePos.add(replacementPos);
				}
			}
		}

		this.templatePos = integerListToArray(templatePos);
		this.valuePos = integerListToArray(valuePos);
	}

	// ------------------------------------------------------------------------

	public String format() {
		return format;
	}

	protected final String[] copyTemplate() {
		String[] copy = new String[template.length];
		System.arraycopy(template, 0, copy, 0, template.length);
		return copy;
	}

	protected final String[] bindVariables(String[] template, String[] values) {
		final int len = templatePos.length;
		for (int i = 0; i < len; i++) {
			template[templatePos[i]] = values[valuePos[i]];
		}
		return template;
	}

	// ------------------------------------------------------------------------

	@Override
	public String toString() {
		return "ScopeFormat '" + format + '\'';
	}
	
	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	/**
	 * Formats the given string to resemble a scope variable.
	 *
	 * @param scope The string to format
	 * @return The formatted string
	 */
	public static String asVariable(String scope) {
		return SCOPE_VARIABLE_PREFIX + scope + SCOPE_VARIABLE_SUFFIX;
	}

	public static String concat(String... components) {
		return concat(defaultFilter, '.', components);
	}

	public static String concat(CharacterFilter filter, String... components) {
		return concat(filter, '.', components);
	}

	public static String concat(Character delimiter, String... components) {
		return concat(defaultFilter, delimiter, components);
	}

	/**
	 * Concatenates the given component names separated by the delimiter character. Additionally
	 * the character filter is applied to all component names.
	 *
	 * @param filter Character filter to be applied to the component names
	 * @param delimiter Delimiter to separate component names
	 * @param components Array of component names
	 * @return The concatenated component name
	 */
	public static String concat(CharacterFilter filter, Character delimiter, String... components) {
		StringBuilder sb = new StringBuilder();
		sb.append(filter.filterCharacters(components[0]));
		for (int x = 1; x < components.length; x++) {
			sb.append(delimiter);
			sb.append(filter.filterCharacters(components[x]));
		}
		return sb.toString();
	}
	
	static String valueOrNull(Object value) {
		return (value == null || (value instanceof String && ((String) value).isEmpty())) ?
				"null" : value.toString();
	}

	static HashMap<String, Integer> arrayToMap(String[] array) {
		HashMap<String, Integer> map = new HashMap<>(array.length);
		for (int i = 0; i < array.length; i++) {
			map.put(array[i], i);
		}
		return map;
	}

	private static int[] integerListToArray(List<Integer> list) {
		int[] array = new int[list.size()];
		int pos = 0;
		for (Integer i : list) {
			array[pos++] = i;
		}
		return array;
	}
}
