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
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.executiongraph.AccessExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.AccessExecutionVertex;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonGenerator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonParser;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationContext;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializerProvider;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** An actor message with a detailed overview of the current status of a job. */
@JsonSerialize(using = JobDetails.JobDetailsSerializer.class)
@JsonDeserialize(using = JobDetails.JobDetailsDeserializer.class)
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
    private static final String FIELD_NAME_CURRENT_EXECUTION_ATTEMPTS =
            "current-execution-attempts";

    private final JobID jobId;

    private final String jobName;

    private final long startTime;

    private final long endTime;

    private final long duration;

    private final JobStatus status;

    private final long lastUpdateTime;

    private final int[] tasksPerState;

    private final int numTasks;

    /**
     * The map holds the attempt number of the current execution attempt in the Execution, which is
     * considered as the representing execution for the subtask of the vertex. The keys and values
     * are JobVertexID -> SubtaskIndex -> CurrentExecutionAttemptNumber. It is used to accumulate
     * the metrics of a subtask in MetricFetcher.
     */
    private final Map<String, Map<Integer, Integer>> currentExecutionAttempts;

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
            Map<String, Map<Integer, Integer>> currentExecutionAttempts) {
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
        Map<String, Map<Integer, Integer>> currentExecutionAttempts = new HashMap<>();

        for (AccessExecutionJobVertex ejv : job.getVerticesTopologically()) {
            AccessExecutionVertex[] taskVertices = ejv.getTaskVertices();
            numTotalTasks += taskVertices.length;
            Map<Integer, Integer> vertexAttempts = new HashMap<>();

            for (AccessExecutionVertex taskVertex : taskVertices) {
                if (taskVertex.getCurrentExecutions().size() > 1) {
                    vertexAttempts.put(
                            taskVertex.getParallelSubtaskIndex(),
                            taskVertex.getCurrentExecutionAttempt().getAttemptNumber());
                }
                ExecutionState state = taskVertex.getExecutionState();
                countsPerStatus[state.ordinal()]++;
                lastChanged = Math.max(lastChanged, taskVertex.getStateTimestamp(state));
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

    public JobID getJobId() {
        return jobId;
    }

    public String getJobName() {
        return jobName;
    }

    public long getStartTime() {
        return startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public long getDuration() {
        return duration;
    }

    public JobStatus getStatus() {
        return status;
    }

    public long getLastUpdateTime() {
        return lastUpdateTime;
    }

    public int getNumTasks() {
        return numTasks;
    }

    public int[] getTasksPerState() {
        return tasksPerState;
    }

    public Map<String, Map<Integer, Integer>> getCurrentExecutionAttempts() {
        return currentExecutionAttempts;
    }
    // ------------------------------------------------------------------------

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

    public static final class JobDetailsSerializer extends StdSerializer<JobDetails> {
        private static final long serialVersionUID = 7915913423515194428L;

        public JobDetailsSerializer() {
            super(JobDetails.class);
        }

        @Override
        public void serialize(
                JobDetails jobDetails,
                JsonGenerator jsonGenerator,
                SerializerProvider serializerProvider)
                throws IOException {
            jsonGenerator.writeStartObject();

            jsonGenerator.writeStringField(FIELD_NAME_JOB_ID, jobDetails.getJobId().toString());
            jsonGenerator.writeStringField(FIELD_NAME_JOB_NAME, jobDetails.getJobName());
            jsonGenerator.writeStringField(FIELD_NAME_STATUS, jobDetails.getStatus().name());

            jsonGenerator.writeNumberField(FIELD_NAME_START_TIME, jobDetails.getStartTime());
            jsonGenerator.writeNumberField(FIELD_NAME_END_TIME, jobDetails.getEndTime());
            jsonGenerator.writeNumberField(FIELD_NAME_DURATION, jobDetails.getDuration());
            jsonGenerator.writeNumberField(
                    FIELD_NAME_LAST_MODIFICATION, jobDetails.getLastUpdateTime());

            jsonGenerator.writeObjectFieldStart("tasks");
            jsonGenerator.writeNumberField(FIELD_NAME_TOTAL_NUMBER_TASKS, jobDetails.getNumTasks());

            final int[] perState = jobDetails.getTasksPerState();

            for (ExecutionState executionState : ExecutionState.values()) {
                jsonGenerator.writeNumberField(
                        executionState.name().toLowerCase(), perState[executionState.ordinal()]);
            }

            jsonGenerator.writeEndObject();

            if (!jobDetails.currentExecutionAttempts.isEmpty()) {
                jsonGenerator.writeObjectFieldStart(FIELD_NAME_CURRENT_EXECUTION_ATTEMPTS);
                for (Map.Entry<String, Map<Integer, Integer>> vertex :
                        jobDetails.currentExecutionAttempts.entrySet()) {
                    jsonGenerator.writeObjectFieldStart(vertex.getKey());
                    for (Map.Entry<Integer, Integer> attempt : vertex.getValue().entrySet()) {
                        jsonGenerator.writeNumberField(
                                String.valueOf(attempt.getKey()), attempt.getValue());
                    }
                    jsonGenerator.writeEndObject();
                }
                jsonGenerator.writeEndObject();
            }

            jsonGenerator.writeEndObject();
        }
    }

    public static final class JobDetailsDeserializer extends StdDeserializer<JobDetails> {

        private static final long serialVersionUID = 6089784742093294800L;

        public JobDetailsDeserializer() {
            super(JobDetails.class);
        }

        @Override
        public JobDetails deserialize(
                JsonParser jsonParser, DeserializationContext deserializationContext)
                throws IOException {

            JsonNode rootNode = jsonParser.readValueAsTree();

            JobID jobId = JobID.fromHexString(rootNode.get(FIELD_NAME_JOB_ID).textValue());
            String jobName = rootNode.get(FIELD_NAME_JOB_NAME).textValue();
            long startTime = rootNode.get(FIELD_NAME_START_TIME).longValue();
            long endTime = rootNode.get(FIELD_NAME_END_TIME).longValue();
            long duration = rootNode.get(FIELD_NAME_DURATION).longValue();
            JobStatus jobStatus = JobStatus.valueOf(rootNode.get(FIELD_NAME_STATUS).textValue());
            long lastUpdateTime = rootNode.get(FIELD_NAME_LAST_MODIFICATION).longValue();

            JsonNode tasksNode = rootNode.get("tasks");
            int numTasks = tasksNode.get(FIELD_NAME_TOTAL_NUMBER_TASKS).intValue();

            int[] numVerticesPerExecutionState = new int[ExecutionState.values().length];

            for (ExecutionState executionState : ExecutionState.values()) {
                JsonNode jsonNode = tasksNode.get(executionState.name().toLowerCase());

                numVerticesPerExecutionState[executionState.ordinal()] =
                        jsonNode == null ? 0 : jsonNode.intValue();
            }

            Map<String, Map<Integer, Integer>> attempts = new HashMap<>();
            JsonNode attemptsNode = rootNode.get(FIELD_NAME_CURRENT_EXECUTION_ATTEMPTS);
            if (attemptsNode != null) {
                attemptsNode
                        .fields()
                        .forEachRemaining(
                                vertex -> {
                                    String vertexId = vertex.getKey();
                                    Map<Integer, Integer> vertexAttempts =
                                            attempts.computeIfAbsent(
                                                    vertexId, k -> new HashMap<>());
                                    vertex.getValue()
                                            .fields()
                                            .forEachRemaining(
                                                    attempt ->
                                                            vertexAttempts.put(
                                                                    Integer.parseInt(
                                                                            attempt.getKey()),
                                                                    attempt.getValue().intValue()));
                                });
            }

            return new JobDetails(
                    jobId,
                    jobName,
                    startTime,
                    endTime,
                    duration,
                    jobStatus,
                    lastUpdateTime,
                    numVerticesPerExecutionState,
                    numTasks,
                    attempts);
        }
    }
}
