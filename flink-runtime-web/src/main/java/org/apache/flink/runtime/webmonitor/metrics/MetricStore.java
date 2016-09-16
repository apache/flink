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
import java.util.Map;

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

	public void add(MetricDump metric) {
		try {
			QueryScopeInfo info = metric.scopeInfo;
			TaskManagerMetricStore tm;
			JobMetricStore job;
			TaskMetricStore task;

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
					/**
					 * As the WebInterface task metric queries currently do not account for subtasks we don't 
					 * divide by subtask and instead use the concatenation of subtask index and metric name as the name. 
					 */
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

	private void addMetric(Map<String, Object> target, String name, MetricDump metric) {
		switch (metric.getCategory()) {
			case METRIC_CATEGORY_COUNTER:
				MetricDump.CounterDump counter = (MetricDump.CounterDump) metric;
				target.put(name, counter.count);
				break;
			case METRIC_CATEGORY_GAUGE:
				MetricDump.GaugeDump gauge = (MetricDump.GaugeDump) metric;
				target.put(name, gauge.value);
				break;
			case METRIC_CATEGORY_HISTOGRAM:
				MetricDump.HistogramDump histogram = (MetricDump.HistogramDump) metric;
				target.put(name + "_min", histogram.min);
				target.put(name + "_max", histogram.max);
				target.put(name + "_mean", histogram.mean);
				target.put(name + "_median", histogram.median);
				target.put(name + "_stddev", histogram.stddev);
				target.put(name + "_p75", histogram.p75);
				target.put(name + "_p90", histogram.p90);
				target.put(name + "_p95", histogram.p95);
				target.put(name + "_p98", histogram.p98);
				target.put(name + "_p99", histogram.p99);
				target.put(name + "_p999", histogram.p999);
				break;
			case METRIC_CATEGORY_METER:
				MetricDump.MeterDump meter = (MetricDump.MeterDump) metric;
				target.put(name, meter.rate);
				break;
		}
	}

	/**
	 * Sub-structure containing metrics of the JobManager.
	 */
	static class JobManagerMetricStore {
		public final Map<String, Object> metrics = new HashMap<>();
	}

	/**
	 * Sub-structure containing metrics of a single TaskManager.
	 */
	static class TaskManagerMetricStore {
		public final Map<String, Object> metrics = new HashMap<>();
	}

	/**
	 * Sub-structure containing metrics of a single Job.
	 */
	static class JobMetricStore {
		public final Map<String, Object> metrics = new HashMap<>();
		public final Map<String, TaskMetricStore> tasks = new HashMap<>();
	}

	/**
	 * Sub-structure containing metrics of a single Task.
	 */
	static class TaskMetricStore {
		public final Map<String, Object> metrics = new HashMap<>();
	}
}
