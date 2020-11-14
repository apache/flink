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

package org.apache.flink.kubernetes.kubeclient.resources;

import org.apache.flink.kubernetes.kubeclient.FlinkKubeClient;

import io.fabric8.kubernetes.api.model.Pod;

import java.util.Collections;
import java.util.List;

/**
 * Watcher for pods in Kubernetes.
 */
public class KubernetesPodsWatcher extends AbstractKubernetesWatcher<Pod, KubernetesPod> {

	public KubernetesPodsWatcher(FlinkKubeClient.WatchCallbackHandler<KubernetesPod> callbackHandler) {
		super(callbackHandler);
	}

	@Override
	public void eventReceived(Action action, Pod pod) {
		logger.debug("Received {} event for pod {}, details: {}", action, pod.getMetadata().getName(), pod.getStatus());
		final List<KubernetesPod> pods = Collections.singletonList(new KubernetesPod(pod));
		switch (action) {
			case ADDED:
				callbackHandler.onAdded(pods);
				break;
			case MODIFIED:
				callbackHandler.onModified(pods);
				break;
			case ERROR:
				callbackHandler.onError(pods);
				break;
			case DELETED:
				callbackHandler.onDeleted(pods);
				break;
			default:
				logger.debug("Ignore handling {} event for pod {}", action, pod.getMetadata().getName());
				break;
		}
	}
}
