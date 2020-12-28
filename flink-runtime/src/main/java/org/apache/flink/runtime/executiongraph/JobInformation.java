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
import org.apache.flink.runtime.blob.PermanentBlobKey;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializedValue;

import java.io.Serializable;
import java.net.URL;
import java.util.Collection;

/** Container class for job information which is stored in the {@link ExecutionGraph}. */
public class JobInformation implements Serializable {

    private static final long serialVersionUID = 8367087049937822140L;

    /** Id of the job */
    private final JobID jobId;

    /** Job name */
    private final String jobName;

    /** Serialized execution config because it can contain user code classes */
    private final SerializedValue<ExecutionConfig> serializedExecutionConfig;

    /** Configuration of the job */
    private final Configuration jobConfiguration;

    /** Blob keys for the required jar files */
    private final Collection<PermanentBlobKey> requiredJarFileBlobKeys;

    /** URLs specifying the classpath to add to the class loader */
    private final Collection<URL> requiredClasspathURLs;

    public JobInformation(
            JobID jobId,
            String jobName,
            SerializedValue<ExecutionConfig> serializedExecutionConfig,
            Configuration jobConfiguration,
            Collection<PermanentBlobKey> requiredJarFileBlobKeys,
            Collection<URL> requiredClasspathURLs) {
        this.jobId = Preconditions.checkNotNull(jobId);
        this.jobName = Preconditions.checkNotNull(jobName);
        this.serializedExecutionConfig = Preconditions.checkNotNull(serializedExecutionConfig);
        this.jobConfiguration = Preconditions.checkNotNull(jobConfiguration);
        this.requiredJarFileBlobKeys = Preconditions.checkNotNull(requiredJarFileBlobKeys);
        this.requiredClasspathURLs = Preconditions.checkNotNull(requiredClasspathURLs);
    }

    public JobID getJobId() {
        return jobId;
    }

    public String getJobName() {
        return jobName;
    }

    public SerializedValue<ExecutionConfig> getSerializedExecutionConfig() {
        return serializedExecutionConfig;
    }

    public Configuration getJobConfiguration() {
        return jobConfiguration;
    }

    public Collection<PermanentBlobKey> getRequiredJarFileBlobKeys() {
        return requiredJarFileBlobKeys;
    }

    public Collection<URL> getRequiredClasspathURLs() {
        return requiredClasspathURLs;
    }

    // ------------------------------------------------------------------------

    @Override
    public String toString() {
        return "JobInformation for '" + jobName + "' (" + jobId + ')';
    }
}
