/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.jobgraph;

import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Information about the parallelism of job vertices. */
public class JobResourceRequirements implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * A key for an internal config option (intentionally prefixed with $internal to make this
     * explicit), that we'll serialize the {@link JobResourceRequirements} into, when writing it to
     * {@link JobGraph}.
     */
    private static final String JOB_RESOURCE_REQUIREMENTS_KEY =
            "$internal.job-resource-requirements";

    private static final JobResourceRequirements EMPTY =
            new JobResourceRequirements(Collections.emptyMap());

    /**
     * Write {@link JobResourceRequirements resource requirements} into the configuration of a given
     * {@link JobGraph}.
     *
     * @param jobGraph job graph to write requirements to
     * @param jobResourceRequirements resource requirements to write
     * @throws IOException in case we're not able to serialize requirements into the configuration
     */
    public static void writeToJobGraph(
            JobGraph jobGraph, JobResourceRequirements jobResourceRequirements) throws IOException {
        InstantiationUtil.writeObjectToConfig(
                jobResourceRequirements,
                jobGraph.getJobConfiguration(),
                JOB_RESOURCE_REQUIREMENTS_KEY);
    }

    /**
     * Read {@link JobResourceRequirements resource requirements} from the configuration of a given
     * {@link JobGraph}.
     *
     * @param jobGraph job graph to read requirements from
     * @throws IOException in case we're not able to deserialize requirements from the configuration
     */
    public static Optional<JobResourceRequirements> readFromJobGraph(JobGraph jobGraph)
            throws IOException {
        try {
            return Optional.ofNullable(
                    InstantiationUtil.readObjectFromConfig(
                            jobGraph.getJobConfiguration(),
                            JOB_RESOURCE_REQUIREMENTS_KEY,
                            JobResourceRequirements.class.getClassLoader()));
        } catch (ClassNotFoundException e) {
            throw new IOException(
                    "Unable to deserialize JobResourceRequirements due to missing classes. This might happen when the JobGraph was written from a different Flink version.",
                    e);
        }
    }

    /**
     * This method validates that:
     *
     * <ul>
     *   <li>The requested boundaries are less or equal than the max parallelism.
     *   <li>The requested boundaries are greater than zero.
     *   <li>The requested upper bound is greater than the lower bound.
     *   <li>There are no unknown job vertex ids and that we're not missing any.
     * </ul>
     *
     * In case any boundary is set to {@code -1}, it will be expanded to the default value ({@code
     * 1} for the lower bound and the max parallelism for the upper bound), before the validation.
     *
     * @param jobResourceRequirements contains the new resources requirements for the job vertices
     * @param maxParallelismPerVertex allows us to look up maximum possible parallelism for a job
     *     vertex
     * @return a list of validation errors
     */
    public static List<String> validate(
            JobResourceRequirements jobResourceRequirements,
            Map<JobVertexID, Integer> maxParallelismPerVertex) {
        final List<String> errors = new ArrayList<>();
        final Set<JobVertexID> missingJobVertexIds =
                new HashSet<>(maxParallelismPerVertex.keySet());
        for (JobVertexID jobVertexId : jobResourceRequirements.getJobVertices()) {
            missingJobVertexIds.remove(jobVertexId);
            final Optional<Integer> maybeMaxParallelism =
                    Optional.ofNullable(maxParallelismPerVertex.get(jobVertexId));
            if (maybeMaxParallelism.isPresent()) {
                final JobVertexResourceRequirements.Parallelism requestedParallelism =
                        jobResourceRequirements.getParallelism(jobVertexId);
                int lowerBound =
                        requestedParallelism.getLowerBound() == -1
                                ? 1
                                : requestedParallelism.getLowerBound();
                int upperBound =
                        requestedParallelism.getUpperBound() == -1
                                ? maybeMaxParallelism.get()
                                : requestedParallelism.getUpperBound();
                if (lowerBound < 1 || upperBound < 1) {
                    errors.add(
                            String.format(
                                    "Both, the requested lower bound [%d] and upper bound [%d] for job vertex [%s] must be greater than zero.",
                                    lowerBound, upperBound, jobVertexId));
                    // Don't validate this vertex any further to avoid additional noise.
                    continue;
                }
                if (lowerBound > upperBound) {
                    errors.add(
                            String.format(
                                    "The requested lower bound [%d] for job vertex [%s] is higher than the upper bound [%d].",
                                    lowerBound, jobVertexId, upperBound));
                }
                if (maybeMaxParallelism.get() < upperBound) {
                    errors.add(
                            String.format(
                                    "The newly requested parallelism %d for the job vertex %s exceeds its maximum parallelism %d.",
                                    upperBound, jobVertexId, maybeMaxParallelism.get()));
                }
            } else {
                errors.add(
                        String.format(
                                "Job vertex [%s] was not found in the JobGraph.", jobVertexId));
            }
        }
        for (JobVertexID jobVertexId : missingJobVertexIds) {
            errors.add(
                    String.format(
                            "The request is incomplete, missing job vertex [%s] resource requirements.",
                            jobVertexId));
        }
        return errors;
    }

    public static JobResourceRequirements empty() {
        return JobResourceRequirements.EMPTY;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static final class Builder {

        private final Map<JobVertexID, JobVertexResourceRequirements> vertexResources =
                new HashMap<>();

        public Builder setParallelismForJobVertex(
                JobVertexID jobVertexId, int lowerBound, int upperBound) {
            vertexResources.put(
                    jobVertexId,
                    new JobVertexResourceRequirements(
                            new JobVertexResourceRequirements.Parallelism(lowerBound, upperBound)));
            return this;
        }

        public JobResourceRequirements build() {
            return new JobResourceRequirements(vertexResources);
        }
    }

    private final Map<JobVertexID, JobVertexResourceRequirements> vertexResources;

    public JobResourceRequirements(
            Map<JobVertexID, JobVertexResourceRequirements> vertexResources) {
        this.vertexResources =
                Collections.unmodifiableMap(new HashMap<>(checkNotNull(vertexResources)));
    }

    public JobVertexResourceRequirements.Parallelism getParallelism(JobVertexID jobVertexId) {
        return Optional.ofNullable(vertexResources.get(jobVertexId))
                .map(JobVertexResourceRequirements::getParallelism)
                .orElseThrow(
                        () ->
                                new IllegalStateException(
                                        "No requirement set for vertex " + jobVertexId));
    }

    public Set<JobVertexID> getJobVertices() {
        return vertexResources.keySet();
    }

    public Map<JobVertexID, JobVertexResourceRequirements> getJobVertexParallelisms() {
        return vertexResources;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final JobResourceRequirements that = (JobResourceRequirements) o;
        return Objects.equals(vertexResources, that.vertexResources);
    }

    @Override
    public int hashCode() {
        return Objects.hash(vertexResources);
    }

    @Override
    public String toString() {
        return "JobResourceRequirements{" + "vertexResources=" + vertexResources + '}';
    }
}
