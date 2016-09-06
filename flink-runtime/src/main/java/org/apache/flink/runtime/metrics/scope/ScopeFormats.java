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

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A container for component scope formats.
 */
public class ScopeFormats {

	private final JobManagerScopeFormat jobManagerFormat;
	private final JobManagerJobScopeFormat jobManagerJobFormat;
	private final TaskManagerScopeFormat taskManagerFormat;
	private final TaskManagerJobScopeFormat taskManagerJobFormat;
	private final TaskScopeFormat taskFormat;
	private final OperatorScopeFormat operatorFormat;

	// ------------------------------------------------------------------------

	/**
	 * Creates all default scope formats.
	 */
	public ScopeFormats() {
		this.jobManagerFormat = new JobManagerScopeFormat(ScopeFormat.DEFAULT_SCOPE_JOBMANAGER_COMPONENT);

		this.jobManagerJobFormat = new JobManagerJobScopeFormat(
				ScopeFormat.DEFAULT_SCOPE_JOBMANAGER_JOB_GROUP, this.jobManagerFormat);

		this.taskManagerFormat = new TaskManagerScopeFormat(ScopeFormat.DEFAULT_SCOPE_TASKMANAGER_COMPONENT);

		this.taskManagerJobFormat = new TaskManagerJobScopeFormat(
				ScopeFormat.DEFAULT_SCOPE_TASKMANAGER_JOB_GROUP, this.taskManagerFormat);

		this.taskFormat = new TaskScopeFormat(
				ScopeFormat.DEFAULT_SCOPE_TASK_GROUP, this.taskManagerJobFormat);

		this.operatorFormat = new OperatorScopeFormat(
				ScopeFormat.DEFAULT_SCOPE_OPERATOR_GROUP, this.taskFormat);
	}

	/**
	 * Creates all scope formats, based on the given scope format strings.
	 */
	public ScopeFormats(
			String jobManagerFormat,
			String jobManagerJobFormat,
			String taskManagerFormat,
			String taskManagerJobFormat,
			String taskFormat,
			String operatorFormat)
	{
		this.jobManagerFormat = new JobManagerScopeFormat(jobManagerFormat);
		this.jobManagerJobFormat = new JobManagerJobScopeFormat(jobManagerJobFormat, this.jobManagerFormat);
		this.taskManagerFormat = new TaskManagerScopeFormat(taskManagerFormat);
		this.taskManagerJobFormat = new TaskManagerJobScopeFormat(taskManagerJobFormat, this.taskManagerFormat);
		this.taskFormat = new TaskScopeFormat(taskFormat, this.taskManagerJobFormat);
		this.operatorFormat = new OperatorScopeFormat(operatorFormat, this.taskFormat);
	}

	/**
	 * Creates a {@code ScopeFormats} with the given scope formats.
	 */
	public ScopeFormats(
			JobManagerScopeFormat jobManagerFormat,
			JobManagerJobScopeFormat jobManagerJobFormat,
			TaskManagerScopeFormat taskManagerFormat,
			TaskManagerJobScopeFormat taskManagerJobFormat,
			TaskScopeFormat taskFormat,
			OperatorScopeFormat operatorFormat)
	{
		this.jobManagerFormat = checkNotNull(jobManagerFormat);
		this.jobManagerJobFormat = checkNotNull(jobManagerJobFormat);
		this.taskManagerFormat = checkNotNull(taskManagerFormat);
		this.taskManagerJobFormat = checkNotNull(taskManagerJobFormat);
		this.taskFormat = checkNotNull(taskFormat);
		this.operatorFormat = checkNotNull(operatorFormat);
	}

	// ------------------------------------------------------------------------
	//  Accessors
	// ------------------------------------------------------------------------

	public JobManagerScopeFormat getJobManagerFormat() {
		return this.jobManagerFormat;
	}

	public TaskManagerScopeFormat getTaskManagerFormat() {
		return this.taskManagerFormat;
	}

	public TaskManagerJobScopeFormat getTaskManagerJobFormat() {
		return this.taskManagerJobFormat;
	}

	public JobManagerJobScopeFormat getJobManagerJobFormat() {
		return this.jobManagerJobFormat;
	}

	public TaskScopeFormat getTaskFormat() {
		return this.taskFormat;
	}

	public OperatorScopeFormat getOperatorFormat() {
		return this.operatorFormat;
	}

	// ------------------------------------------------------------------------
	//  Parsing from Config
	// ------------------------------------------------------------------------

	/**
	 * Creates the scope formats as defined in the given configuration
	 * 
	 * @param config The configuration that defines the formats
	 * @return The ScopeFormats parsed from the configuration
	 */
	public static ScopeFormats fromConfig(Configuration config) {
		String jmFormat = config.getString(
				ConfigConstants.METRICS_SCOPE_NAMING_JM, ScopeFormat.DEFAULT_SCOPE_JOBMANAGER_GROUP);
		String jmJobFormat = config.getString(
				ConfigConstants.METRICS_SCOPE_NAMING_JM_JOB, ScopeFormat.DEFAULT_SCOPE_JOBMANAGER_JOB_GROUP);
		String tmFormat = config.getString(
				ConfigConstants.METRICS_SCOPE_NAMING_TM, ScopeFormat.DEFAULT_SCOPE_TASKMANAGER_GROUP);
		String tmJobFormat = config.getString(
				ConfigConstants.METRICS_SCOPE_NAMING_TM_JOB, ScopeFormat.DEFAULT_SCOPE_TASKMANAGER_JOB_GROUP);
		String taskFormat = config.getString(
				ConfigConstants.METRICS_SCOPE_NAMING_TASK, ScopeFormat.DEFAULT_SCOPE_TASK_GROUP);
		String operatorFormat = config.getString(
				ConfigConstants.METRICS_SCOPE_NAMING_OPERATOR, ScopeFormat.DEFAULT_SCOPE_OPERATOR_GROUP);

		return new ScopeFormats(jmFormat, jmJobFormat, tmFormat, tmJobFormat, taskFormat, operatorFormat);
	}
}
