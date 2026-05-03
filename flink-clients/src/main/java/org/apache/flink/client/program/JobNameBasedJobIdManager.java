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

package org.apache.flink.client.program;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobInfo;
import org.apache.flink.streaming.api.graph.StreamGraph;

import javax.annotation.Nullable;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link JobIdManager} implementation that assigns job IDs based on job names, supporting
 * recovery from previously submitted jobs and deterministic ID assignment.
 *
 * <p>This manager ensures that:
 *
 * <ul>
 *   <li><b>Compatibility</b>: The first submitted job retains the original fixed {@code baseJobId},
 *       ensuring full backward compatibility with existing behavior.
 *   <li><b>Uniqueness</b>: Guarantees globally unique job IDs across all submitted jobs.
 *   <li><b>Correct Association</b>: Matches recovered jobs strictly by job name to prevent
 *       cross-name misassociation; for jobs with the same name, associates them in strict
 *       submission order.
 * </ul>
 */
public class JobNameBasedJobIdManager implements JobIdManager {

    private final JobID baseJobId;

    private final Map<String, ArrayDeque<JobID>> recoveredJobNameToJobIds = new HashMap<>();

    private final Set<JobID> occupiedJobIds = new HashSet<>();

    private @Nullable String firstJobName;

    /**
     * Creates a new job ID manager with the given base job ID and recovered job information.
     *
     * @param baseJobId the base job ID used as the origin for deriving all job IDs
     * @param allRecoveredJobInfos a collection of recovered job information; if non-empty, it
     *     <b>must contain</b> a job with ID equal to {@code baseJobId}
     */
    public JobNameBasedJobIdManager(JobID baseJobId, Collection<JobInfo> allRecoveredJobInfos) {
        this.baseJobId = checkNotNull(baseJobId);
        checkNotNull(allRecoveredJobInfos);

        if (!allRecoveredJobInfos.isEmpty()) {
            // step 1: find the first job name
            Optional<JobInfo> optionalBaseJobInfo =
                    allRecoveredJobInfos.stream()
                            .filter(jobInfo -> jobInfo.getJobId().equals(baseJobId))
                            .findFirst();

            if (optionalBaseJobInfo.isEmpty()) {
                throw new IllegalArgumentException(
                        String.format(
                                "Base job ID %s is not found in the recovered jobs.", baseJobId));
            }

            this.firstJobName = optionalBaseJobInfo.get().getJobName();

            // step 2: group jobs by job name and sort then according to the job index
            Map<String, List<JobID>> grouped =
                    allRecoveredJobInfos.stream()
                            .collect(
                                    Collectors.groupingBy(
                                            JobInfo::getJobName,
                                            Collectors.mapping(
                                                    JobInfo::getJobId, Collectors.toList())));

            for (Map.Entry<String, List<JobID>> entry : grouped.entrySet()) {
                String jobName = entry.getKey();
                List<JobID> jobIds = entry.getValue();

                int initialIndex = getInitialIndex(jobName);

                jobIds.sort(createJobIdComparator(initialIndex, baseJobId));

                this.recoveredJobNameToJobIds.put(jobName, new ArrayDeque<>(jobIds));
            }

            // step 3: initialize occupied job ids
            allRecoveredJobInfos.forEach(jobInfo -> this.occupiedJobIds.add(jobInfo.getJobId()));
        }
    }

    /**
     * Assigns a job ID to the given {@link StreamGraph} based on its job name and the current state
     * of recovered and occupied job IDs.
     *
     * <p>Assignment follows the logic:
     *
     * <ul>
     *   <li>First, attempts to reuse a recovered job ID for the same job name (in assignment
     *       order).
     *   <li>If no recovered job is available, assigns a new job ID by probing from the name-derived
     *       starting index until an unused ID is found.
     * </ul>
     *
     * @param streamGraph the stream graph to assign a job ID to
     */
    @Override
    public void updateJobId(StreamGraph streamGraph) {
        String jobName = checkNotNull(streamGraph.getJobName());

        // case 1: try to find a job id from the recovered jobs
        ArrayDeque<JobID> recoveredJobIds = recoveredJobNameToJobIds.get(jobName);
        if (recoveredJobIds != null) {
            JobID jobId = recoveredJobIds.pollFirst();
            if (recoveredJobIds.isEmpty()) {
                recoveredJobNameToJobIds.remove(jobName);
            }
            streamGraph.setJobId(jobId);
            return;
        }

        // case 2: generate a new job id
        final JobID assignedJobId;
        if (firstJobName == null) {
            firstJobName = jobName;
            assignedJobId = baseJobId;
        } else {
            int initialIndex = getInitialIndex(jobName);
            int step = 0;
            while (true) {
                // the calculated index may overflow and wrap around
                int candidateIndex = initialIndex + step;
                JobID candidateId = fromJobIndex(candidateIndex, baseJobId);
                if (!occupiedJobIds.contains(candidateId)) {
                    assignedJobId = candidateId;
                    break;
                }

                // the step may overflow and wrap around
                step++;
                if (step == 0) {
                    throw new IllegalStateException(
                            String.format("Exhausted all job IDs for job name: %s", jobName));
                }
            }
        }
        streamGraph.setJobId(assignedJobId);
        occupiedJobIds.add(assignedJobId);
    }

    @VisibleForTesting
    String getFirstJobName() {
        return firstJobName;
    }

    @VisibleForTesting
    int getInitialIndex(String jobName) {
        checkNotNull(firstJobName);

        return jobName.equals(firstJobName) ? 0 : jobName.hashCode();
    }

    /**
     * Creates a new job ID by adding the given index to both parts of the base job ID.
     *
     * <p>Note: Both the lower part and upper part may overflow and wrap around.
     *
     * @param index the index to add to the base job ID parts
     * @param baseId the base job ID to derive from
     * @return a new job ID with index applied to both parts
     */
    @VisibleForTesting
    static JobID fromJobIndex(int index, JobID baseId) {
        return new JobID(baseId.getLowerPart() + index, baseId.getUpperPart() + index);
    }

    /**
     * Computes the index of the given job ID relative to the base job ID.
     *
     * <p>This method correctly calculates the index even when the lower and upper parts of the job
     * ID have wrapped around due to overflow.
     *
     * @param jobId the job ID to derive the index from
     * @param baseId the base job ID
     * @return the index of the given job ID relative to the base job ID
     */
    @VisibleForTesting
    static int getJobIndex(JobID jobId, JobID baseId) {
        long lowerDiff = jobId.getLowerPart() - baseId.getLowerPart();
        long upperDiff = jobId.getUpperPart() - baseId.getUpperPart();

        if (lowerDiff != upperDiff
                || lowerDiff < Integer.MIN_VALUE
                || lowerDiff > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(
                    String.format(
                            "Job ID %s is not derived from the base job ID %s.", jobId, baseId));
        }

        return (int) lowerDiff;
    }

    /**
     * Creates a comparator for sorting job IDs based on their logical position relative to an
     * initial index, supporting circular sequence number semantics.
     *
     * <p>This comparator handles the case where job indices may have wrapped around due to overflow
     * by treating the difference as an unsigned 32-bit value. This allows proper ordering of job
     * IDs even when their indices span across the boundary where signed integers would overflow.
     *
     * @param initialIndex the starting index used as reference point for calculating logical
     *     offsets
     * @return a comparator that sorts job IDs by their calculated unsigned offset from the initial
     *     index
     */
    @VisibleForTesting
    static Comparator<JobID> createJobIdComparator(int initialIndex, JobID baseId) {
        return (id1, id2) -> {
            int idx1 = getJobIndex(id1, baseId);
            int idx2 = getJobIndex(id2, baseId);

            // calculate logical offset from initialIndex using unsigned 32-bit subtraction
            // to support circular sequence numbers.
            long steps1 = (idx1 - initialIndex) & 0xFFFFFFFFL;
            long steps2 = (idx2 - initialIndex) & 0xFFFFFFFFL;
            return Long.compare(steps1, steps2);
        };
    }
}
