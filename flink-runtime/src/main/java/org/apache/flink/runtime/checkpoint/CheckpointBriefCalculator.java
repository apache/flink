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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionEdge;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** Computes the tasks to trigger, wait or commit for each checkpoint. */
public class CheckpointBriefCalculator {
    private static final Logger LOG = LoggerFactory.getLogger(CheckpointBriefCalculator.class);

    private final JobID jobId;

    private final CheckpointBriefCalculatorContext context;

    private final List<ExecutionJobVertex> jobVerticesInTopologyOrder = new ArrayList<>();

    private final List<ExecutionVertex> allTasks = new ArrayList<>();

    private final List<ExecutionVertex> sourceTasks = new ArrayList<>();

    public CheckpointBriefCalculator(
            JobID jobId,
            CheckpointBriefCalculatorContext context,
            Iterable<ExecutionJobVertex> jobVerticesInTopologyOrderIterable) {

        this.jobId = checkNotNull(jobId);
        this.context = checkNotNull(context);

        checkNotNull(jobVerticesInTopologyOrderIterable);
        jobVerticesInTopologyOrderIterable.forEach(
                jobVertex -> {
                    jobVerticesInTopologyOrder.add(jobVertex);
                    allTasks.addAll(Arrays.asList(jobVertex.getTaskVertices()));

                    if (jobVertex.getJobVertex().isInputVertex()) {
                        sourceTasks.addAll(Arrays.asList(jobVertex.getTaskVertices()));
                    }
                });
    }

    public CompletableFuture<CheckpointBrief> calculateCheckpointBrief() {
        CompletableFuture<CheckpointBrief> resultFuture = new CompletableFuture<>();

        context.getMainExecutor()
                .execute(
                        () -> {
                            try {
                                if (!isAllExecutionAttemptsAreInitiated()) {
                                    throw new CheckpointException(
                                            CheckpointFailureReason.NOT_ALL_REQUIRED_TASKS_RUNNING);
                                }

                                CheckpointBrief result;
                                if (!context.hasFinishedTasks()) {
                                    result = calculateWithAllTasksRunning();
                                } else {
                                    result = calculateAfterTasksFinished();
                                }

                                if (!isAllExecutionsToTriggerStarted(result.getTasksToTrigger())) {
                                    throw new CheckpointException(
                                            CheckpointFailureReason.NOT_ALL_REQUIRED_TASKS_RUNNING);
                                }

                                resultFuture.complete(result);
                            } catch (Throwable throwable) {
                                resultFuture.completeExceptionally(throwable);
                            }
                        });

        return resultFuture;
    }

    private boolean isAllExecutionAttemptsAreInitiated() {
        for (ExecutionVertex task : allTasks) {
            if (task.getCurrentExecutionAttempt() == null) {
                LOG.info(
                        "task {} of job {} is not being executed at the moment. Aborting checkpoint.",
                        task.getTaskNameWithSubtaskIndex(),
                        jobId);
                return false;
            }
        }

        return true;
    }

    private boolean isAllExecutionsToTriggerStarted(List<Execution> toTrigger) {
        for (Execution execution : toTrigger) {
            if (execution.getState() == ExecutionState.CREATED
                    || execution.getState() == ExecutionState.SCHEDULED
                    || execution.getState() == ExecutionState.DEPLOYING) {

                LOG.info(
                        "Checkpoint triggering task {} of job {} has not being executed at the moment. "
                                + "Aborting checkpoint.",
                        execution.getVertex().getTaskNameWithSubtaskIndex(),
                        jobId);
                return false;
            }
        }

        return true;
    }

    /**
     * Computes the checkpoint brief when all tasks are running. It would simply marks all the
     * source tasks as need to trigger and all the tasks as need to wait and commit.
     *
     * @return The brief of this checkpoint.
     */
    private CheckpointBrief calculateWithAllTasksRunning() {
        List<Execution> executionsToTrigger =
                sourceTasks.stream()
                        .map(ExecutionVertex::getCurrentExecutionAttempt)
                        .collect(Collectors.toList());

        Map<ExecutionAttemptID, ExecutionVertex> ackTasks = createTaskToAck(allTasks);

        return new CheckpointBrief(
                Collections.unmodifiableList(executionsToTrigger),
                ackTasks,
                Collections.unmodifiableList(allTasks),
                Collections.emptyList(),
                Collections.emptyList());
    }

    /**
     * Computes the checkpoint brief after some tasks have finished. Due to the problem of the order
     * of reporting FINISHED is nondeterministic, we have to first compute the accurate running
     * tasks. Then we would iterate the job graph to find the task that is still running, but do not
     * has precedent running tasks.
     *
     * @return The brief of this checkpoint.
     */
    private CheckpointBrief calculateAfterTasksFinished() {
        Map<JobVertexID, JobVertexTaskSet> runningTasksByVertex = calculateRunningTasks();

        List<Execution> tasksToTrigger = new ArrayList<>();

        Map<ExecutionAttemptID, ExecutionVertex> tasksToAck = new HashMap<>();
        List<Execution> finishedTasks = new ArrayList<>();
        List<ExecutionJobVertex> fullyFinishedJobVertex = new ArrayList<>();

        for (ExecutionJobVertex jobVertex : jobVerticesInTopologyOrder) {
            JobVertexTaskSet runningTasks = runningTasksByVertex.get(jobVertex.getJobVertexId());

            if (runningTasks.containsNoTasks()) {
                fullyFinishedJobVertex.add(jobVertex);
            }

            List<JobEdge> prevJobEdges = jobVertex.getJobVertex().getInputs();

            // Fast path: it should not have tasks to trigger if
            // 1. All tasks are not running
            // 2. Connected via ALL_TO_ALL edges and has running tasks.
            // 3. Connected via POINTWISE edge and are all running.
            boolean noTasksToTrigger = runningTasks.containsNoTasks();
            for (JobEdge jobEdge : prevJobEdges) {
                DistributionPattern distributionPattern = jobEdge.getDistributionPattern();
                JobVertexTaskSet sourceRunningTasks =
                        runningTasksByVertex.get(jobEdge.getSource().getProducer().getID());

                if ((distributionPattern == DistributionPattern.ALL_TO_ALL
                                && !sourceRunningTasks.containsNoTasks())
                        || (distributionPattern == DistributionPattern.POINTWISE
                                && sourceRunningTasks.containsAllTasks())) {
                    noTasksToTrigger = true;
                    break;
                }
            }

            for (ExecutionVertex vertex : jobVertex.getTaskVertices()) {
                if (runningTasks.contains(vertex.getID())) {
                    tasksToAck.put(vertex.getCurrentExecutionAttempt().getAttemptId(), vertex);

                    if (!noTasksToTrigger) {
                        boolean hasRunningPrecedentTasks =
                                IntStream.range(0, prevJobEdges.size())
                                        .filter(
                                                i ->
                                                        prevJobEdges.get(i).getDistributionPattern()
                                                                == DistributionPattern.POINTWISE)
                                        .boxed()
                                        .flatMap(i -> getPrecedentTasks(vertex, i).stream())
                                        .anyMatch(
                                                precedentTask ->
                                                        runningTasksByVertex
                                                                .get(precedentTask.getJobvertexId())
                                                                .contains(precedentTask.getID()));

                        if (!hasRunningPrecedentTasks) {
                            tasksToTrigger.add(vertex.getCurrentExecutionAttempt());
                        }
                    }
                } else {
                    finishedTasks.add(vertex.getCurrentExecutionAttempt());
                }
            }
        }

        return new CheckpointBrief(
                Collections.unmodifiableList(tasksToTrigger),
                tasksToAck,
                Collections.unmodifiableList(
                        tasksToAck.size() == allTasks.size()
                                ? allTasks
                                : new ArrayList<>(tasksToAck.values())),
                Collections.unmodifiableList(finishedTasks),
                Collections.unmodifiableList(fullyFinishedJobVertex));
    }

    /**
     * Compute the accurate running tasks for each job vertex. Currently if multiple tasks all
     * finished in short period, the order of their reports of FINISHED is nondeterministic, and
     * some tasks may report FINISHED before all its precedent tasks have.
     *
     * <p>To overcome this issue we would iterates the graph first to acquire the accurate running
     * tasks. We would iterate the job graph in reverse topological order, and for each job vertex,
     * we would remove those precedent tasks that connected to finished tasks of this job vertex
     * from possibly running tasks.
     *
     * @return An accurate set of running tasks for each job vertex.
     */
    @VisibleForTesting
    Map<JobVertexID, JobVertexTaskSet> calculateRunningTasks() {
        Map<JobVertexID, JobVertexTaskSet> runningTasksByVertex = new HashMap<>();

        ListIterator<ExecutionJobVertex> jobVertexIterator =
                jobVerticesInTopologyOrder.listIterator(jobVerticesInTopologyOrder.size());
        while (jobVertexIterator.hasPrevious()) {
            ExecutionJobVertex jobVertex = jobVertexIterator.previous();

            List<JobEdge> jobEdges = getOutputJobEdges(jobVertex);

            // Fast path: see if it has descendant tasks that are either
            // 1. Connected via ALL_TO_ALL edge and has finished tasks.
            // 2. Connected via POINTWISE edge and are all finished.
            boolean allTasksFinished = false;
            for (JobEdge jobEdge : jobEdges) {
                DistributionPattern distributionPattern = jobEdge.getDistributionPattern();
                JobVertexTaskSet targetRunningTasks =
                        runningTasksByVertex.get(jobEdge.getTarget().getID());

                if ((distributionPattern == DistributionPattern.ALL_TO_ALL
                                && !targetRunningTasks.containsAllTasks())
                        || (distributionPattern == DistributionPattern.POINTWISE
                                && targetRunningTasks.containsNoTasks())) {
                    allTasksFinished = true;
                    break;
                }
            }

            if (allTasksFinished) {
                runningTasksByVertex.put(
                        jobVertex.getJobVertexId(), JobVertexTaskSet.noTasks(jobVertex));
                continue;
            }

            Set<ExecutionVertexID> runningTasks =
                    Arrays.stream(jobVertex.getTaskVertices())
                            .filter(
                                    task -> {
                                        if (task.getCurrentExecutionAttempt().isFinished()) {
                                            return false;
                                        }

                                        for (JobEdge edge : jobEdges) {
                                            if (edge.getDistributionPattern()
                                                    == DistributionPattern.POINTWISE) {
                                                List<ExecutionVertex> targets =
                                                        getDescendantTasks(task, edge);
                                                for (ExecutionVertex target : targets) {
                                                    JobVertexTaskSet targetVertexSet =
                                                            runningTasksByVertex.get(
                                                                    target.getJobvertexId());
                                                    if (!targetVertexSet.contains(target.getID())) {
                                                        return false;
                                                    }
                                                }
                                            }
                                        }

                                        return true;
                                    })
                            .map(ExecutionVertex::getID)
                            .collect(Collectors.toSet());
            runningTasksByVertex.put(
                    jobVertex.getJobVertexId(),
                    JobVertexTaskSet.someTasks(jobVertex, runningTasks));
        }

        return runningTasksByVertex;
    }

    private List<JobEdge> getOutputJobEdges(ExecutionJobVertex vertex) {
        return vertex.getJobVertex().getProducedDataSets().stream()
                .flatMap(dataSet -> dataSet.getConsumers().stream())
                .collect(Collectors.toList());
    }

    private List<ExecutionVertex> getDescendantTasks(ExecutionVertex task, JobEdge jobEdge) {
        return task.getProducedPartitions()
                .get(
                        new IntermediateResultPartitionID(
                                jobEdge.getSourceId(), task.getParallelSubtaskIndex()))
                .getConsumers().stream()
                .flatMap(Collection::stream)
                .map(ExecutionEdge::getTarget)
                .collect(Collectors.toList());
    }

    private List<ExecutionVertex> getPrecedentTasks(ExecutionVertex task, int index) {
        return Arrays.stream(task.getInputEdges(index))
                .map(edge -> edge.getSource().getProducer())
                .collect(Collectors.toList());
    }

    private Map<ExecutionAttemptID, ExecutionVertex> createTaskToAck(List<ExecutionVertex> tasks) {
        Map<ExecutionAttemptID, ExecutionVertex> tasksToAck = new HashMap<>(tasks.size());
        tasks.forEach(
                task -> tasksToAck.put(task.getCurrentExecutionAttempt().getAttemptId(), task));
        return tasksToAck;
    }

    /**
     * An optimized representation for a set of tasks belonging to a single job vertex and need to
     * check during iteration of execution graph for some purpose. If all tasks or no tasks are in
     * this set, it would only stores a type flag instead of the detailed list of tasks.
     */
    @VisibleForTesting
    static class JobVertexTaskSet {

        private final ExecutionJobVertex jobVertex;

        private final TaskSetType type;

        private final Set<ExecutionVertexID> tasks;

        public static JobVertexTaskSet allTasks(ExecutionJobVertex jobVertex) {
            return new JobVertexTaskSet(jobVertex, TaskSetType.ALL_TASKS, Collections.emptySet());
        }

        public static JobVertexTaskSet noTasks(ExecutionJobVertex jobVertex) {
            return new JobVertexTaskSet(jobVertex, TaskSetType.NO_TASKS, Collections.emptySet());
        }

        public static JobVertexTaskSet someTasks(
                ExecutionJobVertex jobVertex, Set<ExecutionVertexID> tasks) {
            tasks.forEach(
                    taskId ->
                            checkState(taskId.getJobVertexId().equals(jobVertex.getJobVertexId())));

            if (tasks.size() == jobVertex.getTaskVertices().length) {
                return allTasks(jobVertex);
            } else if (tasks.size() == 0) {
                return noTasks(jobVertex);
            } else {
                return new JobVertexTaskSet(jobVertex, TaskSetType.SOME_TASKS, tasks);
            }
        }

        private JobVertexTaskSet(
                ExecutionJobVertex jobVertex, TaskSetType type, Set<ExecutionVertexID> tasks) {
            this.jobVertex = checkNotNull(jobVertex);
            this.type = type;
            this.tasks = checkNotNull(tasks);
        }

        public boolean contains(ExecutionVertexID taskId) {
            if (!taskId.getJobVertexId().equals(jobVertex.getJobVertexId())) {
                return false;
            }

            return type == TaskSetType.ALL_TASKS
                    || (type == TaskSetType.SOME_TASKS && tasks.contains(taskId));
        }

        public boolean containsAllTasks() {
            return type == TaskSetType.ALL_TASKS;
        }

        public boolean containsNoTasks() {
            return type == TaskSetType.NO_TASKS;
        }

        @VisibleForTesting
        ExecutionJobVertex getJobVertex() {
            return jobVertex;
        }
    }

    private enum TaskSetType {
        ALL_TASKS,
        SOME_TASKS,
        NO_TASKS
    }
}
