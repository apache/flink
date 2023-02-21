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

package org.apache.flink.runtime.messages.webmonitor;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.AccessExecution;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.executiongraph.AccessExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.AccessExecutionVertex;
import org.apache.flink.runtime.rest.messages.json.JobIDDeserializer;
import org.apache.flink.runtime.rest.messages.json.JobIDSerializer;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** An actor message with a detailed overview of the current status of a job. */
public class JobDetails implements Serializable {

    private static final long serialVersionUID = -3391462110304948766L;

    private static final String FIELD_NAME_JOB_ID = "jid";
    private static final String FIELD_NAME_JOB_NAME = "name";
    private static final String FIELD_NAME_START_TIME = "start-time";
    private static final String FIELD_NAME_END_TIME = "end-time";
    private static final String FIELD_NAME_DURATION = "duration";
    private static final String FIELD_NAME_STATUS = "state";
    private static final String FIELD_NAME_LAST_MODIFICATION = "last-modification";
    private static final String FIELD_NAME_TOTAL_NUMBER_TASKS = "total";
    private static final String FIELD_NAME_TASKS = "tasks";

    private final JobID jobId;

    private final String jobName;

    private final long startTime;

    private final long endTime;

    private final long duration;

    private final JobStatus status;

    private final long lastUpdateTime;

    private final int[] tasksPerState;

    private final int numTasks;

    private transient Map<String, Integer> lazyTaskInfo = null;

    /**
     * The map holds the attempt number of the current execution attempt in the Execution, which is
     * considered as the representing execution for the subtask of the vertex. The keys and values
     * are JobVertexID -> SubtaskIndex -> CurrenAttempts info.
     *
     * <p>The field is excluded from the json. Any usage from the web UI and the history server is
     * not allowed.
     */
    private final Map<String, Map<Integer, CurrentAttempts>> currentExecutionAttempts;

    @JsonCreator
    public JobDetails(
            @JsonProperty(FIELD_NAME_JOB_ID) @JsonDeserialize(using = JobIDDeserializer.class)
                    JobID jobId,
            @JsonProperty(FIELD_NAME_JOB_NAME) String jobName,
            @JsonProperty(FIELD_NAME_START_TIME) long startTime,
            @JsonProperty(FIELD_NAME_END_TIME) long endTime,
            @JsonProperty(FIELD_NAME_DURATION) long duration,
            @JsonProperty(FIELD_NAME_STATUS) JobStatus status,
            @JsonProperty(FIELD_NAME_LAST_MODIFICATION) long lastUpdateTime,
            @JsonProperty(FIELD_NAME_TASKS) Map<String, Integer> taskInfo) {
        this(
                jobId,
                jobName,
                startTime,
                endTime,
                duration,
                status,
                lastUpdateTime,
                extractNumTasksPerState(taskInfo),
                taskInfo.get(FIELD_NAME_TOTAL_NUMBER_TASKS));
    }

    @VisibleForTesting
    public JobDetails(
            JobID jobId,
            String jobName,
            long startTime,
            long endTime,
            long duration,
            JobStatus status,
            long lastUpdateTime,
            int[] tasksPerState,
            int numTasks) {
        this(
                jobId,
                jobName,
                startTime,
                endTime,
                duration,
                status,
                lastUpdateTime,
                tasksPerState,
                numTasks,
                new HashMap<>());
    }

    public JobDetails(
            JobID jobId,
            String jobName,
            long startTime,
            long endTime,
            long duration,
            JobStatus status,
            long lastUpdateTime,
            int[] tasksPerState,
            int numTasks,
            Map<String, Map<Integer, CurrentAttempts>> currentExecutionAttempts) {
        this.jobId = checkNotNull(jobId);
        this.jobName = checkNotNull(jobName);
        this.startTime = startTime;
        this.endTime = endTime;
        this.duration = duration;
        this.status = checkNotNull(status);
        this.lastUpdateTime = lastUpdateTime;
        Preconditions.checkArgument(
                tasksPerState.length == ExecutionState.values().length,
                "tasksPerState argument must be of size %s.",
                ExecutionState.values().length);
        this.tasksPerState = checkNotNull(tasksPerState);
        this.numTasks = numTasks;
        this.currentExecutionAttempts = checkNotNull(currentExecutionAttempts);
    }

    public static JobDetails createDetailsForJob(AccessExecutionGraph job) {
        JobStatus status = job.getState();

        long started = job.getStatusTimestamp(JobStatus.INITIALIZING);
        long finished = status.isGloballyTerminalState() ? job.getStatusTimestamp(status) : -1L;
        long duration = (finished >= 0L ? finished : System.currentTimeMillis()) - started;

        int[] countsPerStatus = new int[ExecutionState.values().length];
        long lastChanged = 0;
        int numTotalTasks = 0;
        Map<String, Map<Integer, CurrentAttempts>> currentExecutionAttempts = new HashMap<>();

        for (AccessExecutionJobVertex ejv : job.getVerticesTopologically()) {
            AccessExecutionVertex[] taskVertices = ejv.getTaskVertices();
            numTotalTasks += taskVertices.length;
            Map<Integer, CurrentAttempts> vertexAttempts = new HashMap<>();

            for (AccessExecutionVertex taskVertex : taskVertices) {
                ExecutionState state = taskVertex.getExecutionState();
                countsPerStatus[state.ordinal()]++;
                lastChanged = Math.max(lastChanged, taskVertex.getStateTimestamp(state));

                vertexAttempts.put(
                        taskVertex.getParallelSubtaskIndex(),
                        new CurrentAttempts(
                                taskVertex.getCurrentExecutionAttempt().getAttemptNumber(),
                                taskVertex.getCurrentExecutions().stream()
                                        .map(AccessExecution::getAttemptNumber)
                                        .collect(Collectors.toSet())));
            }

            if (!vertexAttempts.isEmpty()) {
                currentExecutionAttempts.put(String.valueOf(ejv.getJobVertexId()), vertexAttempts);
            }
        }

        lastChanged = Math.max(lastChanged, finished);

        return new JobDetails(
                job.getJobID(),
                job.getJobName(),
                started,
                finished,
                duration,
                status,
                lastChanged,
                countsPerStatus,
                numTotalTasks,
                currentExecutionAttempts);
    }

    // ------------------------------------------------------------------------

    @JsonProperty(FIELD_NAME_JOB_ID)
    @JsonSerialize(using = JobIDSerializer.class)
    public JobID getJobId() {
        return jobId;
    }

    @JsonProperty(FIELD_NAME_JOB_NAME)
    public String getJobName() {
        return jobName;
    }

    @JsonProperty(FIELD_NAME_START_TIME)
    public long getStartTime() {
        return startTime;
    }

    @JsonProperty(FIELD_NAME_END_TIME)
    public long getEndTime() {
        return endTime;
    }

    @JsonProperty(FIELD_NAME_DURATION)
    public long getDuration() {
        return duration;
    }

    @JsonProperty(FIELD_NAME_STATUS)
    public JobStatus getStatus() {
        return status;
    }

    @JsonProperty(FIELD_NAME_LAST_MODIFICATION)
    public long getLastUpdateTime() {
        return lastUpdateTime;
    }

    @JsonProperty(FIELD_NAME_TASKS)
    public Map<String, Integer> getTaskInfo() {
        if (lazyTaskInfo == null) {
            final Map<String, Integer> taskInfo = new HashMap<>();
            taskInfo.put(FIELD_NAME_TOTAL_NUMBER_TASKS, getNumTasks());
            for (ExecutionState executionState : ExecutionState.values()) {
                taskInfo.put(
                        executionState.name().toLowerCase(),
                        tasksPerState[executionState.ordinal()]);
            }
            lazyTaskInfo = taskInfo;
        }
        return lazyTaskInfo;
    }

    @JsonIgnore
    public int getNumTasks() {
        return numTasks;
    }

    @JsonIgnore
    public int[] getTasksPerState() {
        return tasksPerState;
    }

    @JsonIgnore
    public Map<String, Map<Integer, CurrentAttempts>> getCurrentExecutionAttempts() {
        return currentExecutionAttempts;
    }
    // ------------------------------------------------------------------------

    private static int[] extractNumTasksPerState(Map<String, Integer> ex) {
        int[] tasksPerState = new int[ExecutionState.values().length];
        for (ExecutionState value : ExecutionState.values()) {
            tasksPerState[value.ordinal()] =
                    ex.getOrDefault(value.name().toLowerCase(Locale.ROOT), 0);
        }
        return tasksPerState;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o != null && o.getClass() == JobDetails.class) {
            JobDetails that = (JobDetails) o;

            return this.endTime == that.endTime
                    && this.lastUpdateTime == that.lastUpdateTime
                    && this.numTasks == that.numTasks
                    && this.startTime == that.startTime
                    && this.status == that.status
                    && this.jobId.equals(that.jobId)
                    && this.jobName.equals(that.jobName)
                    && Arrays.equals(this.tasksPerState, that.tasksPerState)
                    && this.currentExecutionAttempts.equals(that.currentExecutionAttempts);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        int result = jobId.hashCode();
        result = 31 * result + jobName.hashCode();
        result = 31 * result + (int) (startTime ^ (startTime >>> 32));
        result = 31 * result + (int) (endTime ^ (endTime >>> 32));
        result = 31 * result + status.hashCode();
        result = 31 * result + (int) (lastUpdateTime ^ (lastUpdateTime >>> 32));
        result = 31 * result + Arrays.hashCode(tasksPerState);
        result = 31 * result + numTasks;
        result = 31 * result + currentExecutionAttempts.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "JobDetails {"
                + "jobId="
                + jobId
                + ", jobName='"
                + jobName
                + '\''
                + ", startTime="
                + startTime
                + ", endTime="
                + endTime
                + ", status="
                + status
                + ", lastUpdateTime="
                + lastUpdateTime
                + ", numVerticesPerExecutionState="
                + Arrays.toString(tasksPerState)
                + ", numTasks="
                + numTasks
                + '}';
    }

    /**
     * The CurrentAttempts holds the attempt number of the current representative execution attempt,
     * and the attempt numbers of all the running attempts.
     */
    public static final class CurrentAttempts implements Serializable {
        private final int representativeAttempt;

        private final Set<Integer> currentAttempts;

        public CurrentAttempts(int representativeAttempt, Set<Integer> currentAttempts) {
            this.representativeAttempt = representativeAttempt;
            this.currentAttempts = Collections.unmodifiableSet(currentAttempts);
        }

        public int getRepresentativeAttempt() {
            return representativeAttempt;
        }

        public Set<Integer> getCurrentAttempts() {
            return currentAttempts;
        }
    }
}
