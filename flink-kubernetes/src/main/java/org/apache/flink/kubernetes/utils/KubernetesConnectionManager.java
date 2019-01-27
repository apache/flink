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

package org.apache.flink.kubernetes.utils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.runtime.resourcemanager.exceptions.ResourceManagerException;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Implements connection manager to be able to connect with Kubernetes
 * and handle reties internally.
 */
public class KubernetesConnectionManager {

	protected static final Logger LOG = LoggerFactory.getLogger(KubernetesConnectionManager.class);

	protected KubernetesClient kubernetesClient;

	protected int retryTimes;

	protected long retryIntervalMs;

	public KubernetesConnectionManager(Configuration conf) {
		kubernetesClient = KubernetesClientFactory.create(conf);
		retryTimes = conf.getInteger(KubernetesConfigOptions.KUBERNETES_CONNECTION_RETRY_TIMES);
		retryIntervalMs = conf.getLong(KubernetesConfigOptions.KUBERNETES_CONNECTION_RETRY_INTERVAL_MS);
	}

	public void createPod(Pod pod) throws ResourceManagerException {
		Supplier<Void> action = () -> {
			kubernetesClient.pods().create(pod);
			LOG.info("Requested pod: {}", pod.getMetadata().getName());
			return null;
		};
		runWithRetry(action);
	}

	public void removePods(Map<String, String> labels) throws ResourceManagerException {
		Supplier<Void> action = () -> {
			kubernetesClient.pods().withLabels(labels).delete();
			LOG.info("Removed TM pods with labels: {}", labels);
			return null;
		};
		runWithRetry(action);
	}

	public <T> Object runWithRetry(Supplier<T> action) throws ResourceManagerException {
		try {
			return RetryUtils.run(action, retryTimes, retryIntervalMs);
		} catch (Exception e) {
			throw new ResourceManagerException(e);
		}
	}

	public void removePod(Pod pod) throws ResourceManagerException {
		Supplier<Void> action = () -> {
			boolean deleted = kubernetesClient.pods().delete(pod);
			LOG.info("{} pod: {}", deleted ? "Removed" : "Failed to remove", pod.getMetadata().getName());
			return null;
		};
		runWithRetry(action);
	}

	public void removePod(String podName) throws ResourceManagerException {
		Supplier<Void> action = () -> {
			boolean deleted = kubernetesClient.pods().withName(podName).delete();
			LOG.info("{} pod: {}", deleted ? "Removed" : "Failed to remove", podName);
			return null;
		};
		runWithRetry(action);
	}

	public PodList getPods(Map<String, String> labels) throws ResourceManagerException {
		return (PodList) runWithRetry(() -> kubernetesClient.pods().withLabels(labels).list());
	}

	public Service getService(String serviceName) throws ResourceManagerException {
		return (Service) runWithRetry(() -> kubernetesClient.services().withName(serviceName).get());
	}

	public ConfigMap createOrReplaceConfigMap(ConfigMap configMap) throws ResourceManagerException {
		return (ConfigMap) runWithRetry(() -> kubernetesClient.configMaps().createOrReplace(configMap));
	}

	public Watch createAndStartPodsWatcher(Map<String, String> labels,
		BiConsumer<Watcher.Action, Pod> podEventHandler, Consumer<Exception> watcherCloseHandler)
		throws ResourceManagerException {
		Watcher watcher = new Watcher<Pod>() {
			@Override
			public void eventReceived(Action action, Pod pod) {
				podEventHandler.accept(action, pod);
			}

			@Override
			public void onClose(KubernetesClientException e) {
				LOG.error("Pods watcher onClose");
				if (e != null) {
					LOG.error(e.getMessage(), e);
				}
				watcherCloseHandler.accept(e);
			}
		};
		return (Watch) runWithRetry(() -> kubernetesClient.pods().withLabels(labels).watch(watcher));
	}

	public void close() {
		if (kubernetesClient != null) {
			kubernetesClient.close();
		}
	}

}
