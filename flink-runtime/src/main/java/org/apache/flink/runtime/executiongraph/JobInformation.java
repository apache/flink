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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.UnmodifiableConfiguration;
import org.apache.flink.runtime.blob.PermanentBlobKey;
import org.apache.flink.runtime.jobgraph.JobType;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializedValue;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableCollection;
import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableList;

import java.io.Serializable;
import java.net.URL;
import java.util.Collection;
import java.util.Objects;

/** Container class for job information which is stored in the {@link ExecutionGraph}. */
public class JobInformation implements Serializable {

    private static final long serialVersionUID = 8367087049937822140L;

    /** Id of the job. */
    private final JobID jobId;

    /** Type of the job. */
    private final JobType jobType;

    /** Job name. */
    private final String jobName;

    /** Serialized execution config because it can contain user code classes. */
    private final SerializedValue<ExecutionConfig> serializedExecutionConfig;

    /** Configuration of the job. */
    private final UnmodifiableConfiguration jobConfiguration;

    /** Blob keys for the required jar files. */
    private final ImmutableCollection<PermanentBlobKey> requiredJarFileBlobKeys;

    /** URLs specifying the classpath to add to the class loader. */
    private final ImmutableCollection<URL> requiredClasspathURLs;

    public JobInformation(
            JobID jobId,
            String jobName,
            SerializedValue<ExecutionConfig> serializedExecutionConfig,
            Configuration jobConfiguration,
            Collection<PermanentBlobKey> requiredJarFileBlobKeys,
            Collection<URL> requiredClasspathURLs) {
        this(
                jobId,
                JobType.STREAMING,
                jobName,
                serializedExecutionConfig,
                jobConfiguration,
                requiredJarFileBlobKeys,
                requiredClasspathURLs);
    }

    public JobInformation(
            JobID jobId,
            JobType jobType,
            String jobName,
            SerializedValue<ExecutionConfig> serializedExecutionConfig,
            Configuration jobConfiguration,
            Collection<PermanentBlobKey> requiredJarFileBlobKeys,
            Collection<URL> requiredClasspathURLs) {
        this.jobId = Preconditions.checkNotNull(jobId);
        this.jobType = Preconditions.checkNotNull(jobType);
        this.jobName = Preconditions.checkNotNull(jobName);
        this.serializedExecutionConfig = Preconditions.checkNotNull(serializedExecutionConfig);
        this.jobConfiguration =
                new UnmodifiableConfiguration(Preconditions.checkNotNull(jobConfiguration));
        this.requiredJarFileBlobKeys =
                ImmutableList.copyOf(Preconditions.checkNotNull(requiredJarFileBlobKeys));
        this.requiredClasspathURLs =
                ImmutableList.copyOf(Preconditions.checkNotNull(requiredClasspathURLs));
    }

    public JobID getJobId() {
        return jobId;
    }

    public String getJobName() {
        return jobName;
    }

    public JobType getJobType() {
        return jobType;
    }

    public SerializedValue<ExecutionConfig> getSerializedExecutionConfig() {
        return serializedExecutionConfig;
    }

    public UnmodifiableConfiguration getJobConfiguration() {
        return jobConfiguration;
    }

    public ImmutableCollection<PermanentBlobKey> getRequiredJarFileBlobKeys() {
        return requiredJarFileBlobKeys;
    }

    public ImmutableCollection<URL> getRequiredClasspathURLs() {
        return requiredClasspathURLs;
    }

    // All fields are immutable, so return this directly.
    public JobInformation deepCopy() {
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        JobInformation that = (JobInformation) o;
        return Objects.equals(jobId, that.jobId)
                && Objects.equals(jobName, that.jobName)
                && Objects.equals(serializedExecutionConfig, that.serializedExecutionConfig)
                && Objects.equals(jobConfiguration, that.jobConfiguration)
                && Objects.equals(requiredJarFileBlobKeys, that.requiredJarFileBlobKeys)
                && Objects.equals(requiredClasspathURLs, that.requiredClasspathURLs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                jobId,
                jobName,
                serializedExecutionConfig,
                jobConfiguration,
                requiredJarFileBlobKeys,
                requiredClasspathURLs);
    }

    // ------------------------------------------------------------------------

    @Override
    public String toString() {
        return "JobInformation for '" + jobName + "' (" + jobId + ')';
    }
}
