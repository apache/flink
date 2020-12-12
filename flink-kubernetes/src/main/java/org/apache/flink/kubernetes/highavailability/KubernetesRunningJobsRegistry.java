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

package org.apache.flink.kubernetes.highavailability;

import org.apache.flink.api.common.JobID;
import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesConfigMap;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesLeaderElector;
import org.apache.flink.runtime.highavailability.RunningJobsRegistry;
import org.apache.flink.util.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;

import static org.apache.flink.kubernetes.utils.Constants.RUNNING_JOBS_REGISTRY_KEY_PREFIX;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * {@link RunningJobsRegistry} implementation for Kubernetes. All the running jobs will be stored in
 * Dispatcher-leader ConfigMap. The key is the job id with prefix
 * {@link org.apache.flink.kubernetes.utils.Constants#RUNNING_JOBS_REGISTRY_KEY_PREFIX},
 * and value is job status.
 */
public class KubernetesRunningJobsRegistry implements RunningJobsRegistry {

	private static final Logger LOG = LoggerFactory.getLogger(KubernetesRunningJobsRegistry.class);

	private final FlinkKubeClient kubeClient;

	private final String configMapName;

	private final String lockIdentity;

	public KubernetesRunningJobsRegistry(FlinkKubeClient kubeClient, String configMapName, String lockIdentity) {
		this.kubeClient = checkNotNull(kubeClient);
		this.configMapName = checkNotNull(configMapName);
		this.lockIdentity = checkNotNull(lockIdentity);
	}

	@Override
	public void setJobRunning(JobID jobID) throws IOException {
		checkNotNull(jobID);

		writeJobStatusToConfigMap(jobID, JobSchedulingStatus.RUNNING);
	}

	@Override
	public void setJobFinished(JobID jobID) throws IOException {
		checkNotNull(jobID);

		writeJobStatusToConfigMap(jobID, JobSchedulingStatus.DONE);
	}

	@Override
	public JobSchedulingStatus getJobSchedulingStatus(JobID jobID) throws IOException {
		checkNotNull(jobID);

		return kubeClient.getConfigMap(configMapName)
			.map(configMap -> getJobStatus(configMap, jobID).orElse(JobSchedulingStatus.PENDING))
			.orElseThrow(() -> new IOException("ConfigMap " + configMapName + " does not exist."));
	}

	@Override
	public void clearJob(JobID jobID) throws IOException {
		checkNotNull(jobID);

		try {
			kubeClient.checkAndUpdateConfigMap(
				configMapName,
				configMap -> {
					if (KubernetesLeaderElector.hasLeadership(configMap, lockIdentity)) {
						if (configMap.getData().remove(getKeyForJobId(jobID)) != null) {
							return Optional.of(configMap);
						}
					}
					return Optional.empty();
				}
			).get();
		} catch (Exception e) {
			throw new IOException("Failed to clear job state in ConfigMap " + configMapName + " for job " + jobID, e);
		}
	}

	private void writeJobStatusToConfigMap(JobID jobID, JobSchedulingStatus status) throws IOException {
		LOG.debug("Setting scheduling state for job {} to {}.", jobID, status);
		final String key = getKeyForJobId(jobID);
		try {
			kubeClient.checkAndUpdateConfigMap(
				configMapName,
				configMap -> {
					if (KubernetesLeaderElector.hasLeadership(configMap, lockIdentity)) {
						final Optional<JobSchedulingStatus> optional = getJobStatus(configMap, jobID);
						if (!optional.isPresent() || optional.get() != status) {
							configMap.getData().put(key, status.name());
							return Optional.of(configMap);
						}
					}
					return Optional.empty();
				}
			).get();
		} catch (Exception e) {
			throw new IOException("Failed to set " + status.name() + " state in ConfigMap "
				+ configMapName + " for job " + jobID, e);
		}
	}

	private Optional<JobSchedulingStatus> getJobStatus(KubernetesConfigMap configMap, JobID jobId) {
		final String key = getKeyForJobId(jobId);
		final String status = configMap.getData().get(key);
		if (!StringUtils.isNullOrWhitespaceOnly(status)) {
			return Optional.of(JobSchedulingStatus.valueOf(status));
		}
		return Optional.empty();
	}

	private String getKeyForJobId(JobID jobId) {
		return RUNNING_JOBS_REGISTRY_KEY_PREFIX + jobId.toString();
	}
}

