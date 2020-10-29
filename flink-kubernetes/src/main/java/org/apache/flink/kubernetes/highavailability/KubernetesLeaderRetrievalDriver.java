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

import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesConfigMap;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesWatch;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalDriver;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalEventHandler;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalException;
import org.apache.flink.runtime.rpc.FatalErrorHandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.apache.flink.kubernetes.utils.KubernetesUtils.checkConfigMaps;
import static org.apache.flink.kubernetes.utils.KubernetesUtils.getLeaderInformationFromConfigMap;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The counterpart to the {@link KubernetesLeaderElectionDriver}.
 * {@link LeaderRetrievalDriver} implementation for Kubernetes. It retrieves the current leader which has
 * been elected by the {@link KubernetesLeaderElectionDriver}.
 * The leader address as well as the current leader session ID is retrieved from Kubernetes ConfigMap.
 */
public class KubernetesLeaderRetrievalDriver implements LeaderRetrievalDriver {

	private static final Logger LOG = LoggerFactory.getLogger(KubernetesLeaderRetrievalDriver.class);

	private final String configMapName;

	private final LeaderRetrievalEventHandler leaderRetrievalEventHandler;

	private final KubernetesWatch kubernetesWatch;

	private final FatalErrorHandler fatalErrorHandler;

	private volatile boolean running;

	public KubernetesLeaderRetrievalDriver(
			FlinkKubeClient kubeClient,
			String configMapName,
			LeaderRetrievalEventHandler leaderRetrievalEventHandler,
			FatalErrorHandler fatalErrorHandler) {
		checkNotNull(kubeClient, "Kubernetes client");
		this.configMapName = checkNotNull(configMapName, "ConfigMap name");
		this.leaderRetrievalEventHandler = checkNotNull(leaderRetrievalEventHandler, "LeaderRetrievalEventHandler");
		this.fatalErrorHandler = checkNotNull(fatalErrorHandler);

		kubernetesWatch = kubeClient.watchConfigMaps(configMapName, new ConfigMapCallbackHandlerImpl());

		running = true;
	}

	@Override
	public void close() {
		if (!running) {
			return;
		}
		running = false;

		LOG.info("Stopping {}.", this);
		kubernetesWatch.close();
	}

	private class ConfigMapCallbackHandlerImpl implements FlinkKubeClient.WatchCallbackHandler<KubernetesConfigMap> {

		@Override
		public void onAdded(List<KubernetesConfigMap> configMaps) {
			// The ConfigMap is created by KubernetesLeaderElectionDriver with empty data. We do not process this
			// useless event.
		}

		@Override
		public void onModified(List<KubernetesConfigMap> configMaps) {
			final KubernetesConfigMap configMap = checkConfigMaps(configMaps, configMapName);
			leaderRetrievalEventHandler.notifyLeaderAddress(getLeaderInformationFromConfigMap(configMap));
		}

		@Override
		public void onDeleted(List<KubernetesConfigMap> configMaps) {
			// Nothing to do since the delete event will be handled in the leader election part.
		}

		@Override
		public void onError(List<KubernetesConfigMap> configMaps) {
			fatalErrorHandler.onFatalError(
				new LeaderRetrievalException("Error while watching the ConfigMap " + configMapName));
		}

		@Override
		public void handleFatalError(Throwable throwable) {
			fatalErrorHandler.onFatalError(
				new LeaderRetrievalException("Error while watching the ConfigMap " + configMapName));
		}
	}

	@Override
	public String toString() {
		return "KubernetesLeaderRetrievalDriver{configMapName='" + configMapName + "'}";
	}
}
