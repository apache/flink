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

package org.apache.flink.kubernetes.kubeclient;

import org.apache.flink.kubernetes.kubeclient.resources.KubernetesPod;
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesService;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;

/**
 * The client to talk with kubernetes.
 */
public interface FlinkKubeClient extends AutoCloseable {

	/**
	 * Create the Master components, this can include the Deployment, the ConfigMap(s), and the Service(s).
	 *
	 */
	void createJobManagerComponent(KubernetesJobManagerSpecification kubernetesJMSpec);

	/**
	 * Create task manager pod.
	 */
	void createTaskManagerPod(KubernetesPod kubernetesPod);

	/**
	 * Stop a specified pod by name.
	 *
	 * @param podName pod name
	 */
	void stopPod(String podName);

	/**
	 * Stop cluster and clean up all resources, include services, auxiliary services and all running pods.
	 *
	 * @param clusterId cluster id
	 */
	void stopAndCleanupCluster(String clusterId);

	/**
	 * Get the kubernetes internal service of the given flink clusterId.
	 *
	 * @param clusterId cluster id
	 * @return Return the internal service of the specified cluster id. Return null if the service does not exist.
	 */
	@Nullable
	KubernetesService getInternalService(String clusterId);

	/**
	 * Get the kubernetes rest service of the given flink clusterId.
	 *
	 * @param clusterId cluster id
	 * @return Return the rest service of the specified cluster id. Return null if the service does not exist.
	 */
	@Nullable
	KubernetesService getRestService(String clusterId);

	/**
	 * Get the rest endpoint for access outside cluster.
	 *
	 * @param clusterId cluster id
	 * @return Return null if the service does not exist or could not extract the Endpoint from the service.
	 */
	@Nullable
	Endpoint getRestEndpoint(String clusterId);

	/**
	 * List the pods with specified labels.
	 *
	 * @param labels labels to filter the pods
	 * @return pod list
	 */
	List<KubernetesPod> getPodsWithLabels(Map<String, String> labels);

	/**
	 * Log exceptions.
	 */
	void handleException(Exception e);

	/**
	 * Watch the pods selected by labels and do the {@link PodCallbackHandler}.
	 *
	 * @param labels labels to filter the pods to watch
	 * @param callbackHandler {@link PodCallbackHandler} will be called when the watcher receive the corresponding events.
	 */
	void watchPodsAndDoCallback(Map<String, String> labels, PodCallbackHandler callbackHandler);

	/**
	 * Callback handler for kubernetes pods.
	 */
	interface PodCallbackHandler {

		void onAdded(List<KubernetesPod> pods);

		void onModified(List<KubernetesPod> pods);

		void onDeleted(List<KubernetesPod> pods);

		void onError(List<KubernetesPod> pods);
	}

}
