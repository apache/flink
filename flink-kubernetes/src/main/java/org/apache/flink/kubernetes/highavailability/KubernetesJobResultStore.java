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

package org.apache.flink.kubernetes.highavailability;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ApplicationID;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient;
import org.apache.flink.runtime.highavailability.AbstractThreadsafeJobResultStore;
import org.apache.flink.runtime.highavailability.JobResultEntry;
import org.apache.flink.runtime.highavailability.JobResultStore;
import org.apache.flink.runtime.jobmaster.JobResult;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.function.BooleanSupplier;

/**
 * A {@link JobResultStore} backed by a Kubernetes ConfigMap of the cluster. Each job has one entry:
 * the key is the job ID, the value holds the globally terminal {@link JobStatus} and the {@link
 * ApplicationID} of the job while the entry is dirty, and is {@link #CLEAN_VALUE} once the entry is
 * clean and retained. The {@link KubernetesResultStoreFencedClient} fences the updates by a
 * leadership check and makes them idempotent, so the Kubernetes client may retry them.
 */
public class KubernetesJobResultStore extends AbstractThreadsafeJobResultStore {

    @VisibleForTesting static final String CLEAN_VALUE = "CLEAN";

    private static final String VALUE_SEPARATOR = ":";

    private final KubernetesResultStoreFencedClient fencedClient;

    /**
     * @param kubeClient the client used to access the ConfigMaps.
     * @param configMapName the name of the ConfigMap holding the entries.
     * @param clusterId the cluster ID that determines the labels of the ConfigMap.
     * @param leadershipCheck tells whether this JobManager holds the leadership; it is evaluated
     *     right before every update, and an update without leadership fails.
     * @param deleteOnCommit whether entries are deleted instead of marked as clean.
     * @param ioExecutor the executor running the store operations.
     */
    public KubernetesJobResultStore(
            FlinkKubeClient kubeClient,
            String configMapName,
            String clusterId,
            BooleanSupplier leadershipCheck,
            boolean deleteOnCommit,
            Executor ioExecutor) {
        super(ioExecutor);
        this.fencedClient =
                new KubernetesResultStoreFencedClient(
                        kubeClient,
                        configMapName,
                        clusterId,
                        leadershipCheck,
                        deleteOnCommit,
                        CLEAN_VALUE::equals,
                        ignoredDirtyValue -> CLEAN_VALUE);
    }

    @Override
    protected void createDirtyResultInternal(JobResultEntry jobResultEntry) throws IOException {
        fencedClient.createDirtyEntry(
                keyOf(jobResultEntry.getJobId()), encodeDirtyValue(jobResultEntry.getJobResult()));
    }

    @Override
    protected void markResultAsCleanInternal(JobID jobId)
            throws IOException, NoSuchElementException {
        fencedClient.markEntryAsClean(keyOf(jobId));
    }

    @Override
    protected boolean hasDirtyJobResultEntryInternal(JobID jobId) {
        return fencedClient.hasDirtyEntry(keyOf(jobId));
    }

    @Override
    protected boolean hasCleanJobResultEntryInternal(JobID jobId) {
        return fencedClient.hasCleanEntry(keyOf(jobId));
    }

    @Override
    protected Set<JobResult> getDirtyResultsInternal() throws IOException {
        final Set<JobResult> dirtyResults = new HashSet<>();
        for (Map.Entry<String, String> entry : fencedClient.getDirtyEntries().entrySet()) {
            try {
                dirtyResults.add(decodeDirtyValue(entry.getKey(), entry.getValue()));
            } catch (IllegalArgumentException e) {
                throw new IOException(
                        String.format(
                                "Could not parse the job result entry %s=%s in ConfigMap %s.",
                                entry.getKey(), entry.getValue(), fencedClient.getConfigMapName()),
                        e);
            }
        }
        return dirtyResults;
    }

    private static String keyOf(JobID jobId) {
        return jobId.toHexString();
    }

    /** Stores only the job status and the application ID: no accumulators, no failure cause. */
    private static String encodeDirtyValue(JobResult jobResult) {
        final JobStatus jobStatus =
                jobResult
                        .getJobStatus()
                        .orElseThrow(
                                () ->
                                        new IllegalArgumentException(
                                                String.format(
                                                        "The result of job %s has no globally terminal job status.",
                                                        jobResult.getJobId())));
        return jobResult
                .getApplicationId()
                .map(
                        applicationId ->
                                jobStatus.name() + VALUE_SEPARATOR + applicationId.toHexString())
                .orElseGet(jobStatus::name);
    }

    /** The counterpart of {@link #encodeDirtyValue}; fails on a value it did not write. */
    private static JobResult decodeDirtyValue(String key, String value) {
        final int separator = value.indexOf(VALUE_SEPARATOR);
        final String jobStatus = separator < 0 ? value : value.substring(0, separator);
        final JobResult.Builder builder =
                new JobResult.Builder()
                        .jobId(JobID.fromHexString(key))
                        .jobStatus(JobStatus.valueOf(jobStatus))
                        .netRuntime(0);
        if (separator >= 0) {
            builder.applicationId(ApplicationID.fromHexString(value.substring(separator + 1)));
        }
        return builder.build();
    }
}
