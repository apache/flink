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

package org.apache.flink.runtime.rest.handler.legacy.metrics;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.messages.webmonitor.JobDetails.CurrentAttempts;
import org.apache.flink.runtime.metrics.dump.MetricDump;
import org.apache.flink.runtime.metrics.dump.QueryScopeInfo;
import org.apache.flink.util.CollectionUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Collections.unmodifiableMap;
import static java.util.Collections.unmodifiableSet;
import static org.apache.flink.runtime.metrics.dump.MetricDump.METRIC_CATEGORY_COUNTER;
import static org.apache.flink.runtime.metrics.dump.MetricDump.METRIC_CATEGORY_GAUGE;
import static org.apache.flink.runtime.metrics.dump.MetricDump.METRIC_CATEGORY_HISTOGRAM;
import static org.apache.flink.runtime.metrics.dump.MetricDump.METRIC_CATEGORY_METER;
import static org.apache.flink.runtime.metrics.dump.QueryScopeInfo.INFO_CATEGORY_JM;
import static org.apache.flink.runtime.metrics.dump.QueryScopeInfo.INFO_CATEGORY_JM_OPERATOR;
import static org.apache.flink.runtime.metrics.dump.QueryScopeInfo.INFO_CATEGORY_JOB;
import static org.apache.flink.runtime.metrics.dump.QueryScopeInfo.INFO_CATEGORY_OPERATOR;
import static org.apache.flink.runtime.metrics.dump.QueryScopeInfo.INFO_CATEGORY_TASK;
import static org.apache.flink.runtime.metrics.dump.QueryScopeInfo.INFO_CATEGORY_TM;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Nested data-structure to store metrics. */
@ThreadSafe
public class MetricStore {
    private static final Logger LOG = LoggerFactory.getLogger(MetricStore.class);

    private final ComponentMetricStore jobManager = new ComponentMetricStore();
    private final Map<String, TaskManagerMetricStore> taskManagers = new ConcurrentHashMap<>();
    private final Map<String, JobMetricStore> jobs = new ConcurrentHashMap<>();

    /**
     * The map holds the attempt number of the representing execution for each subtask of each
     * vertex. The keys and values are JobID -> JobVertexID -> SubtaskIndex ->
     * CurrentExecutionAttemptNumber. When a metric of an execution attempt is added, the metric can
     * also be added to the SubtaskMetricStore when it is of the representing execution.
     */
    private final Map<String, Map<String, Map<Integer, Integer>>> representativeAttempts =
            new ConcurrentHashMap<>();

    /**
     * Remove inactive task managers.
     *
     * @param activeTaskManagers to retain.
     */
    synchronized void retainTaskManagers(List<String> activeTaskManagers) {
        taskManagers.keySet().retainAll(activeTaskManagers);
    }

    /**
     * Remove inactive jobs..
     *
     * @param activeJobs to retain.
     */
    synchronized void retainJobs(List<String> activeJobs) {
        jobs.keySet().retainAll(activeJobs);
        representativeAttempts.keySet().retainAll(activeJobs);
    }

    public synchronized void updateCurrentExecutionAttempts(Collection<JobDetails> jobs) {
        for (JobDetails job : jobs) {
            String jobId = job.getJobId().toString();
            Map<String, Map<Integer, CurrentAttempts>> currentAttempts =
                    job.getCurrentExecutionAttempts();
            Map<String, Map<Integer, Integer>> jobRepresentativeAttempts =
                    representativeAttempts.compute(
                            jobId,
                            (k, overwritten) ->
                                    CollectionUtil.newHashMapWithExpectedSize(
                                            currentAttempts.size()));
            currentAttempts.forEach(
                    (vertexId, subtaskAttempts) -> {
                        Map<Integer, Integer> vertexAttempts =
                                jobRepresentativeAttempts.compute(
                                        vertexId, (k, overwritten) -> new HashMap<>());
                        Optional<TaskMetricStore> taskMetricStoreOptional =
                                Optional.ofNullable(this.jobs.get(jobId))
                                        .map(map -> map.getTaskMetricStore(vertexId));

                        // Retains current active subtasks to accommodate dynamic scaling
                        taskMetricStoreOptional.ifPresent(
                                taskMetricStore ->
                                        taskMetricStore.retainSubtasks(subtaskAttempts.keySet()));

                        subtaskAttempts.forEach(
                                (subtaskIndex, attempts) -> {
                                    // Updates representative attempts
                                    vertexAttempts.put(
                                            subtaskIndex, attempts.getRepresentativeAttempt());
                                    // Retains current attempt metrics to avoid memory leak
                                    taskMetricStoreOptional
                                            .map(
                                                    taskMetricStore ->
                                                            taskMetricStore.getSubtaskMetricStore(
                                                                    subtaskIndex))
                                            .ifPresent(
                                                    subtaskMetricStore ->
                                                            subtaskMetricStore.retainAttempts(
                                                                    attempts.getCurrentAttempts()));
                                });
                    });
        }
    }

    public Map<String, Map<String, Map<Integer, Integer>>> getRepresentativeAttempts() {
        return representativeAttempts;
    }

    /**
     * Add metric dumps to the store.
     *
     * @param metricDumps to add.
     */
    synchronized void addAll(List<MetricDump> metricDumps) {
        for (MetricDump metric : metricDumps) {
            add(metric);
        }
    }

    // -----------------------------------------------------------------------------------------------------------------
    // Accessors for sub MetricStores
    // -----------------------------------------------------------------------------------------------------------------

    /**
     * Returns the {@link ComponentMetricStore} for the JobManager.
     *
     * @return ComponentMetricStore for the JobManager
     */
    public synchronized ComponentMetricStore getJobManagerMetricStore() {
        return ComponentMetricStore.unmodifiable(jobManager);
    }

    /**
     * Returns the {@link TaskManagerMetricStore} for the given taskmanager ID.
     *
     * @param tmID taskmanager ID
     * @return TaskManagerMetricStore for the given ID, or null if no store for the given argument
     *     exists
     */
    public synchronized TaskManagerMetricStore getTaskManagerMetricStore(String tmID) {
        return tmID == null ? null : TaskManagerMetricStore.unmodifiable(taskManagers.get(tmID));
    }

    /**
     * Returns the {@link ComponentMetricStore} for the given job ID.
     *
     * @param jobID job ID
     * @return ComponentMetricStore for the given ID, or null if no store for the given argument
     *     exists
     */
    public synchronized ComponentMetricStore getJobMetricStore(String jobID) {
        return jobID == null ? null : ComponentMetricStore.unmodifiable(jobs.get(jobID));
    }

    /**
     * Returns the {@link ComponentMetricStore} for the given job/task ID.
     *
     * @param jobID job ID
     * @param taskID task ID
     * @return ComponentMetricStore for given IDs, or null if no store for the given arguments
     *     exists
     */
    public synchronized TaskMetricStore getTaskMetricStore(String jobID, String taskID) {
        JobMetricStore job = jobID == null ? null : jobs.get(jobID);
        if (job == null || taskID == null) {
            return null;
        }
        return TaskMetricStore.unmodifiable(job.getTaskMetricStore(taskID));
    }

    /**
     * Returns the {@link ComponentMetricStore} for the given job/task ID and subtask index.
     *
     * @param jobID job ID
     * @param taskID task ID
     * @param subtaskIndex subtask index
     * @return SubtaskMetricStore for the given IDs and index, or null if no store for the given
     *     arguments exists
     */
    public synchronized ComponentMetricStore getSubtaskMetricStore(
            String jobID, String taskID, int subtaskIndex) {
        JobMetricStore job = jobID == null ? null : jobs.get(jobID);
        if (job == null) {
            return null;
        }
        TaskMetricStore task = job.getTaskMetricStore(taskID);
        if (task == null) {
            return null;
        }
        return SubtaskMetricStore.unmodifiable(task.getSubtaskMetricStore(subtaskIndex));
    }

    public synchronized ComponentMetricStore getSubtaskAttemptMetricStore(
            String jobID, String taskID, int subtaskIndex, int attemptNumber) {
        JobMetricStore job = jobID == null ? null : jobs.get(jobID);
        if (job == null) {
            return null;
        }
        TaskMetricStore task = job.getTaskMetricStore(taskID);
        if (task == null) {
            return null;
        }
        SubtaskMetricStore subtask = task.getSubtaskMetricStore(subtaskIndex);
        if (subtask == null) {
            return null;
        }
        return ComponentMetricStore.unmodifiable(subtask.getAttemptsMetricStore(attemptNumber));
    }

    public synchronized Map<String, JobMetricStore> getJobs() {
        return unmodifiableMap(jobs);
    }

    public synchronized Map<String, TaskManagerMetricStore> getTaskManagers() {
        return unmodifiableMap(taskManagers);
    }

    /** @deprecated Use semantically equivalent {@link #getJobManagerMetricStore()}. */
    @Deprecated
    public synchronized ComponentMetricStore getJobManager() {
        return ComponentMetricStore.unmodifiable(jobManager);
    }

    @VisibleForTesting
    public void add(MetricDump metric) {
        try {
            QueryScopeInfo info = metric.scopeInfo;
            TaskManagerMetricStore tm;
            JobMetricStore job;
            TaskMetricStore task;
            SubtaskMetricStore subtask;
            ComponentMetricStore attempt;
            ComponentMetricStore jmOperator;
            boolean isRepresentativeAttempt;

            String name = info.scope.isEmpty() ? metric.name : info.scope + "." + metric.name;

            if (name.isEmpty()) { // malformed transmission
                return;
            }

            switch (info.getCategory()) {
                case INFO_CATEGORY_JM:
                    addMetric(jobManager.metrics, name, metric);
                    break;
                case INFO_CATEGORY_TM:
                    String tmID = ((QueryScopeInfo.TaskManagerQueryScopeInfo) info).taskManagerID;
                    tm = taskManagers.computeIfAbsent(tmID, k -> new TaskManagerMetricStore());
                    if (name.contains("GarbageCollector")) {
                        String gcName =
                                name.substring(
                                        "Status.JVM.GarbageCollector.".length(),
                                        name.lastIndexOf('.'));
                        tm.addGarbageCollectorName(gcName);
                    }
                    addMetric(tm.metrics, name, metric);
                    break;
                case INFO_CATEGORY_JOB:
                    QueryScopeInfo.JobQueryScopeInfo jobInfo =
                            (QueryScopeInfo.JobQueryScopeInfo) info;
                    job = jobs.computeIfAbsent(jobInfo.jobID, k -> new JobMetricStore());
                    addMetric(job.metrics, name, metric);
                    break;
                case INFO_CATEGORY_TASK:
                    QueryScopeInfo.TaskQueryScopeInfo taskInfo =
                            (QueryScopeInfo.TaskQueryScopeInfo) info;
                    job = jobs.computeIfAbsent(taskInfo.jobID, k -> new JobMetricStore());
                    task = job.tasks.computeIfAbsent(taskInfo.vertexID, k -> new TaskMetricStore());
                    subtask =
                            task.subtasks.computeIfAbsent(
                                    taskInfo.subtaskIndex, k -> new SubtaskMetricStore());

                    // The attempt is the representative one if the current execution attempt
                    // number for the subtask is not present in the currentExecutionAttempts,
                    // which means there should be only one execution
                    isRepresentativeAttempt =
                            isRepresentativeAttempt(
                                    taskInfo.jobID,
                                    taskInfo.vertexID,
                                    taskInfo.subtaskIndex,
                                    taskInfo.attemptNumber);
                    attempt =
                            subtask.attempts.computeIfAbsent(
                                    taskInfo.attemptNumber, k -> new ComponentMetricStore());
                    addMetric(attempt.metrics, name, metric);
                    // If the attempt is representative one, its metrics can be updated to the
                    // subtask and task metric store.
                    if (isRepresentativeAttempt) {
                        /**
                         * The duplication is intended. Metrics scoped by subtask are useful for
                         * several job/task handlers, while the WebInterface task metric queries
                         * currently do not account for subtasks, so we don't divide by subtask and
                         * instead use the concatenation of subtask index and metric name as the
                         * name for those.
                         */
                        addMetric(subtask.metrics, name, metric);
                        addMetric(task.metrics, taskInfo.subtaskIndex + "." + name, metric);
                    }
                    break;
                case INFO_CATEGORY_OPERATOR:
                    QueryScopeInfo.OperatorQueryScopeInfo operatorInfo =
                            (QueryScopeInfo.OperatorQueryScopeInfo) info;
                    job = jobs.computeIfAbsent(operatorInfo.jobID, k -> new JobMetricStore());
                    task =
                            job.tasks.computeIfAbsent(
                                    operatorInfo.vertexID, k -> new TaskMetricStore());
                    subtask =
                            task.subtasks.computeIfAbsent(
                                    operatorInfo.subtaskIndex, k -> new SubtaskMetricStore());

                    isRepresentativeAttempt =
                            isRepresentativeAttempt(
                                    operatorInfo.jobID,
                                    operatorInfo.vertexID,
                                    operatorInfo.subtaskIndex,
                                    operatorInfo.attemptNumber);

                    attempt =
                            subtask.attempts.computeIfAbsent(
                                    operatorInfo.attemptNumber, k -> new ComponentMetricStore());
                    addMetric(attempt.metrics, operatorInfo.operatorName + "." + name, metric);

                    // If the attempt is representative one, its metrics can be updated to the
                    // subtask and task metric store.
                    if (isRepresentativeAttempt) {
                        /**
                         * As the WebInterface does not account for operators (because it can't) we
                         * don't divide by operator and instead use the concatenation of subtask
                         * index, operator name and metric name as the name.
                         */
                        addMetric(subtask.metrics, operatorInfo.operatorName + "." + name, metric);
                        addMetric(
                                task.metrics,
                                operatorInfo.subtaskIndex
                                        + "."
                                        + operatorInfo.operatorName
                                        + "."
                                        + name,
                                metric);
                    }
                    break;
                case INFO_CATEGORY_JM_OPERATOR:
                    QueryScopeInfo.JobManagerOperatorQueryScopeInfo jmOperatorInfo =
                            (QueryScopeInfo.JobManagerOperatorQueryScopeInfo) info;
                    job = jobs.computeIfAbsent(jmOperatorInfo.jobID, k -> new JobMetricStore());
                    task =
                            job.tasks.computeIfAbsent(
                                    jmOperatorInfo.vertexID, k -> new TaskMetricStore());
                    jmOperator =
                            task.jmOperators.computeIfAbsent(
                                    jmOperatorInfo.operatorName, k -> new ComponentMetricStore());
                    addMetric(jmOperator.metrics, name, metric);
                    break;
                default:
                    LOG.debug("Invalid metric dump category: " + info.getCategory());
            }
        } catch (Exception e) {
            LOG.debug("Malformed metric dump.", e);
        }
    }

    // Returns whether the attempt is the representative one. It's also true if the current
    // execution attempt number for the subtask is not present in the currentExecutionAttempts,
    // which means there should be only one execution
    private boolean isRepresentativeAttempt(
            String jobID, String vertexID, int subtaskIndex, int attemptNumber) {
        return Optional.of(representativeAttempts)
                        .map(m -> m.get(jobID))
                        .map(m -> m.get(vertexID))
                        .map(m -> m.get(subtaskIndex))
                        .orElse(attemptNumber)
                == attemptNumber;
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
    // sub MetricStore classes
    // -----------------------------------------------------------------------------------------------------------------

    /** Structure containing metrics of a single component. */
    @ThreadSafe
    public static class ComponentMetricStore {
        public final Map<String, String> metrics;

        private ComponentMetricStore() {
            this(new ConcurrentHashMap<>());
        }

        private ComponentMetricStore(Map<String, String> metrics) {
            this.metrics = checkNotNull(metrics);
        }

        public String getMetric(String name) {
            return this.metrics.get(name);
        }

        public String getMetric(String name, String defaultValue) {
            String value = this.metrics.get(name);
            return value != null ? value : defaultValue;
        }

        private static ComponentMetricStore unmodifiable(ComponentMetricStore source) {
            if (source == null) {
                return null;
            }
            return new ComponentMetricStore(unmodifiableMap(source.metrics));
        }
    }

    /** Sub-structure containing metrics of a single TaskManager. */
    @ThreadSafe
    public static class TaskManagerMetricStore extends ComponentMetricStore {
        public final Set<String> garbageCollectorNames;

        private TaskManagerMetricStore() {
            this(new ConcurrentHashMap<>(), ConcurrentHashMap.newKeySet());
        }

        private TaskManagerMetricStore(
                Map<String, String> metrics, Set<String> garbageCollectorNames) {
            super(metrics);
            this.garbageCollectorNames = checkNotNull(garbageCollectorNames);
        }

        private void addGarbageCollectorName(String name) {
            garbageCollectorNames.add(name);
        }

        private static TaskManagerMetricStore unmodifiable(TaskManagerMetricStore source) {
            if (source == null) {
                return null;
            }
            return new TaskManagerMetricStore(
                    unmodifiableMap(source.metrics), unmodifiableSet(source.garbageCollectorNames));
        }
    }

    /** Sub-structure containing metrics of a single Job. */
    @ThreadSafe
    private static class JobMetricStore extends ComponentMetricStore {
        private final Map<String, TaskMetricStore> tasks = new ConcurrentHashMap<>();

        public TaskMetricStore getTaskMetricStore(String taskID) {
            return taskID == null ? null : tasks.get(taskID);
        }
    }

    /** Sub-structure containing metrics of a single Task. */
    @ThreadSafe
    public static class TaskMetricStore extends ComponentMetricStore {
        private final Map<Integer, SubtaskMetricStore> subtasks;
        private final Map<String, ComponentMetricStore> jmOperators;

        private TaskMetricStore() {
            this(new ConcurrentHashMap<>(), new ConcurrentHashMap<>(), new ConcurrentHashMap<>());
        }

        private TaskMetricStore(
                Map<String, String> metrics,
                Map<Integer, SubtaskMetricStore> subtasks,
                Map<String, ComponentMetricStore> jmOperators) {
            super(metrics);
            this.subtasks = checkNotNull(subtasks);
            this.jmOperators = checkNotNull(jmOperators);
        }

        public SubtaskMetricStore getSubtaskMetricStore(int subtaskIndex) {
            return subtasks.get(subtaskIndex);
        }

        public Map<Integer, SubtaskMetricStore> getAllSubtaskMetricStores() {
            return unmodifiableMap(subtasks);
        }

        void retainSubtasks(Set<Integer> activeSubtasks) {
            // Retain metrics of pattern subtaskIndex.metricName which are directly stored in
            // TaskMetricStore
            metrics.keySet()
                    .removeIf(
                            key -> {
                                // To prevent errors in metric parsing, here we only
                                // clean up metrics with a pattern of
                                // "subtaskIndex.metricName"
                                String index = key.substring(0, Math.max(key.indexOf('.'), 0));
                                return index.matches("\\d+")
                                        && !activeSubtasks.contains(Integer.parseInt(index));
                            });

            subtasks.keySet().retainAll(activeSubtasks);
        }

        public ComponentMetricStore getJobManagerOperatorMetricStores(String operatorName) {
            return jmOperators.get(operatorName);
        }

        private static TaskMetricStore unmodifiable(TaskMetricStore source) {
            if (source == null) {
                return null;
            }
            return new TaskMetricStore(
                    unmodifiableMap(source.metrics),
                    unmodifiableMap(source.subtasks),
                    unmodifiableMap(source.jmOperators));
        }
    }

    /** Sub-structure containing metrics of a single subtask. */
    @ThreadSafe
    public static class SubtaskMetricStore extends ComponentMetricStore {
        private final Map<Integer, ComponentMetricStore> attempts;

        private SubtaskMetricStore() {
            this(new ConcurrentHashMap<>(), new ConcurrentHashMap<>());
        }

        private SubtaskMetricStore(
                Map<String, String> metrics, Map<Integer, ComponentMetricStore> attempts) {
            super(metrics);
            this.attempts = checkNotNull(attempts);
        }

        public ComponentMetricStore getAttemptsMetricStore(int attemptNumber) {
            return attempts.get(attemptNumber);
        }

        public Map<Integer, ComponentMetricStore> getAllAttemptsMetricStores() {
            return unmodifiableMap(attempts);
        }

        void retainAttempts(Set<Integer> currentAttempts) {
            int latestAttempt = currentAttempts.stream().mapToInt(i -> i).max().orElse(0);
            attempts.keySet()
                    .removeIf(
                            attempt ->
                                    attempt < latestAttempt && !currentAttempts.contains(attempt));
        }

        private static SubtaskMetricStore unmodifiable(SubtaskMetricStore source) {
            if (source == null) {
                return null;
            }
            return new SubtaskMetricStore(
                    unmodifiableMap(source.metrics), unmodifiableMap(source.attempts));
        }
    }
}
