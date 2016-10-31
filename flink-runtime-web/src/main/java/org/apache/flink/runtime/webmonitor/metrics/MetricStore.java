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
package org.apache.flink.runtime.webmonitor.metrics;

import org.apache.flink.runtime.metrics.dump.MetricDump;
import org.apache.flink.runtime.metrics.dump.QueryScopeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.runtime.metrics.dump.MetricDump.METRIC_CATEGORY_COUNTER;
import static org.apache.flink.runtime.metrics.dump.MetricDump.METRIC_CATEGORY_GAUGE;
import static org.apache.flink.runtime.metrics.dump.MetricDump.METRIC_CATEGORY_HISTOGRAM;
import static org.apache.flink.runtime.metrics.dump.MetricDump.METRIC_CATEGORY_METER;
import static org.apache.flink.runtime.metrics.dump.QueryScopeInfo.INFO_CATEGORY_JM;
import static org.apache.flink.runtime.metrics.dump.QueryScopeInfo.INFO_CATEGORY_JOB;
import static org.apache.flink.runtime.metrics.dump.QueryScopeInfo.INFO_CATEGORY_OPERATOR;
import static org.apache.flink.runtime.metrics.dump.QueryScopeInfo.INFO_CATEGORY_TASK;
import static org.apache.flink.runtime.metrics.dump.QueryScopeInfo.INFO_CATEGORY_TM;

/**
 * Nested data-structure to store metrics.
 *
 * This structure is not thread-safe.
 */
public class MetricStore {
	private static final Logger LOG = LoggerFactory.getLogger(MetricStore.class);

	final JobManagerMetricStore jobManager = new JobManagerMetricStore();
	final Map<String, TaskManagerMetricStore> taskManagers = new HashMap<>();
	final Map<String, JobMetricStore> jobs = new HashMap<>();

	// -----------------------------------------------------------------------------------------------------------------
	// Adding metrics
	// -----------------------------------------------------------------------------------------------------------------
	public void add(MetricDump metric) {
		try {
			QueryScopeInfo info = metric.scopeInfo;
			TaskManagerMetricStore tm;
			JobMetricStore job;
			TaskMetricStore task;
			SubtaskMetricStore subtask;

			String name = info.scope.isEmpty()
				? metric.name
				: info.scope + "." + metric.name;

			if (name.isEmpty()) { // malformed transmission
				return;
			}

			switch (info.getCategory()) {
				case INFO_CATEGORY_JM:
					addMetric(jobManager.metrics, name, metric);
					break;
				case INFO_CATEGORY_TM:
					String tmID = ((QueryScopeInfo.TaskManagerQueryScopeInfo) info).taskManagerID;
					tm = taskManagers.get(tmID);
					if (tm == null) {
						tm = new TaskManagerMetricStore();
						taskManagers.put(tmID, tm);
					}
					if (name.contains("GarbageCollector")) {
						String gcName = name.substring("Status.JVM.GarbageCollector.".length(), name.lastIndexOf('.'));
						tm.addGarbageCollectorName(gcName);
					}
					addMetric(tm.metrics, name, metric);
					break;
				case INFO_CATEGORY_JOB:
					QueryScopeInfo.JobQueryScopeInfo jobInfo = (QueryScopeInfo.JobQueryScopeInfo) info;
					job = jobs.get(jobInfo.jobID);
					if (job == null) {
						job = new JobMetricStore();
						jobs.put(jobInfo.jobID, job);
					}
					addMetric(job.metrics, name, metric);
					break;
				case INFO_CATEGORY_TASK:
					QueryScopeInfo.TaskQueryScopeInfo taskInfo = (QueryScopeInfo.TaskQueryScopeInfo) info;
					job = jobs.get(taskInfo.jobID);
					if (job == null) {
						job = new JobMetricStore();
						jobs.put(taskInfo.jobID, job);
					}
					task = job.tasks.get(taskInfo.vertexID);
					if (task == null) {
						task = new TaskMetricStore();
						job.tasks.put(taskInfo.vertexID, task);
					}
					subtask = task.subtasks.get(taskInfo.subtaskIndex);
					if (subtask == null) {
						subtask = new SubtaskMetricStore();
						task.subtasks.put(taskInfo.subtaskIndex, subtask);
					}
					/**
					 * The duplication is intended. Metrics scoped by subtask are useful for several job/task handlers,
					 * while the WebInterface task metric queries currently do not account for subtasks, so we don't 
					 * divide by subtask and instead use the concatenation of subtask index and metric name as the name
					 * for thos.
					 */
					addMetric(subtask.metrics, name, metric);
					addMetric(task.metrics, taskInfo.subtaskIndex + "." + name, metric);
					break;
				case INFO_CATEGORY_OPERATOR:
					QueryScopeInfo.OperatorQueryScopeInfo operatorInfo = (QueryScopeInfo.OperatorQueryScopeInfo) info;
					job = jobs.get(operatorInfo.jobID);
					if (job == null) {
						job = new JobMetricStore();
						jobs.put(operatorInfo.jobID, job);
					}
					task = job.tasks.get(operatorInfo.vertexID);
					if (task == null) {
						task = new TaskMetricStore();
						job.tasks.put(operatorInfo.vertexID, task);
					}
					/**
					 * As the WebInterface does not account for operators (because it can't) we don't 
					 * divide by operator and instead use the concatenation of subtask index, operator name and metric name 
					 * as the name.
					 */
					addMetric(task.metrics, operatorInfo.subtaskIndex + "." + operatorInfo.operatorName + "." + name, metric);
					break;
				default:
					LOG.debug("Invalid metric dump category: " + info.getCategory());
			}
		} catch (Exception e) {
			LOG.debug("Malformed metric dump.", e);
		}
	}

	private void addMetric(Map<String, String> target, String name, MetricDump metric) {
		switch (metric.getCategory()) {
			case METRIC_CATEGORY_COUNTER:
				MetricDump.CounterDump counter = (MetricDump.CounterDump) metric;
				target.put(name, String.valueOf(counter.count));
				break;
			case METRIC_CATEGORY_GAUGE:
				MetricDump.GaugeDump gauge = (MetricDump.GaugeDump) metric;
				target.put(name, gauge.value);
				break;
			case METRIC_CATEGORY_HISTOGRAM:
				MetricDump.HistogramDump histogram = (MetricDump.HistogramDump) metric;
				target.put(name + "_min", String.valueOf(histogram.min));
				target.put(name + "_max", String.valueOf(histogram.max));
				target.put(name + "_mean", String.valueOf(histogram.mean));
				target.put(name + "_median", String.valueOf(histogram.median));
				target.put(name + "_stddev", String.valueOf(histogram.stddev));
				target.put(name + "_p75", String.valueOf(histogram.p75));
				target.put(name + "_p90", String.valueOf(histogram.p90));
				target.put(name + "_p95", String.valueOf(histogram.p95));
				target.put(name + "_p98", String.valueOf(histogram.p98));
				target.put(name + "_p99", String.valueOf(histogram.p99));
				target.put(name + "_p999", String.valueOf(histogram.p999));
				break;
			case METRIC_CATEGORY_METER:
				MetricDump.MeterDump meter = (MetricDump.MeterDump) metric;
				target.put(name, String.valueOf(meter.rate));
				break;
		}
	}

	// -----------------------------------------------------------------------------------------------------------------
	// Accessors for sub MetricStores
	// -----------------------------------------------------------------------------------------------------------------

	/**
	 * Returns the {@link JobManagerMetricStore}.
	 *
	 * @return JobManagerMetricStore
	 */
	public JobManagerMetricStore getJobManagerMetricStore() {
		return jobManager;
	}

	/**
	 * Returns the {@link TaskManagerMetricStore} for the given taskmanager ID.
	 *
	 * @param tmID taskmanager ID
	 * @return TaskManagerMetricStore for the given ID, or null if no store for the given argument exists
	 */
	public TaskManagerMetricStore getTaskManagerMetricStore(String tmID) {
		return taskManagers.get(tmID);
	}

	/**
	 * Returns the {@link JobMetricStore} for the given job ID.
	 *
	 * @param jobID job ID
	 * @return JobMetricStore for the given ID, or null if no store for the given argument exists
	 */
	public JobMetricStore getJobMetricStore(String jobID) {
		return jobs.get(jobID);
	}

	/**
	 * Returns the {@link TaskMetricStore} for the given job/task ID.
	 *
	 * @param jobID  job ID
	 * @param taskID task ID
	 * @return TaskMetricStore for given IDs, or null if no store for the given arguments exists
	 */
	public TaskMetricStore getTaskMetricStore(String jobID, String taskID) {
		JobMetricStore job = getJobMetricStore(jobID);
		if (job == null) {
			return null;
		}
		return job.getTaskMetricStore(taskID);
	}

	/**
	 * Returns the {@link SubtaskMetricStore} for the given job/task ID and subtask index.
	 *
	 * @param jobID        job ID
	 * @param taskID       task ID
	 * @param subtaskIndex subtask index
	 * @return SubtaskMetricStore for the given IDs and index, or null if no store for the given arguments exists
	 */
	public SubtaskMetricStore getSubtaskMetricStore(String jobID, String taskID, int subtaskIndex) {
		TaskMetricStore task = getTaskMetricStore(jobID, taskID);
		if (task == null) {
			return null;
		}
		return task.getSubtaskMetricStore(subtaskIndex);
	}

	// -----------------------------------------------------------------------------------------------------------------
	// sub MetricStore classes
	// -----------------------------------------------------------------------------------------------------------------
	private static abstract class ComponentMetricStore {
		public final Map<String, String> metrics = new HashMap<>();

		public String getMetric(String name, String defaultValue) {
			String value = this.metrics.get(name);
			return value != null
				? value
				: defaultValue;
		}
	}

	/**
	 * Sub-structure containing metrics of the JobManager.
	 */
	public static class JobManagerMetricStore extends ComponentMetricStore {
	}

	/**
	 * Sub-structure containing metrics of a single TaskManager.
	 */
	public static class TaskManagerMetricStore extends ComponentMetricStore {
		public final Set<String> garbageCollectorNames = new HashSet<>();
		
		public void addGarbageCollectorName(String name) {
			garbageCollectorNames.add(name);
		}
	}

	/**
	 * Sub-structure containing metrics of a single Job.
	 */
	public static class JobMetricStore extends ComponentMetricStore {
		private final Map<String, TaskMetricStore> tasks = new HashMap<>();

		public TaskMetricStore getTaskMetricStore(String taskID) {
			return tasks.get(taskID);
		}
	}

	/**
	 * Sub-structure containing metrics of a single Task.
	 */
	public static class TaskMetricStore extends ComponentMetricStore {
		private final Map<Integer, SubtaskMetricStore> subtasks = new HashMap<>();

		public SubtaskMetricStore getSubtaskMetricStore(int subtaskIndex) {
			return subtasks.get(subtaskIndex);
		}
	}

	/**
	 * Sub-structure containing metrics of a single Subtask.
	 */
	public static class SubtaskMetricStore extends ComponentMetricStore {
	}
}
