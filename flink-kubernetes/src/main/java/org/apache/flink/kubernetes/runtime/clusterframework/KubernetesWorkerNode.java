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

package org.apache.flink.kubernetes.runtime.clusterframework;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceIDRetrievable;
import org.apache.flink.util.Preconditions;

import io.fabric8.kubernetes.api.model.Pod;

/**
 * A representation of a registered Kubernetes pod managed by the {@link KubernetesSessionResourceManager}.
 */
public class KubernetesWorkerNode implements ResourceIDRetrievable {

	private static final int DEFAULT_PRIORITY = 0;

	private Pod pod;
	private ResourceID resourceId;
	private long podId;
	private int priority;

	public KubernetesWorkerNode(Pod pod, String podName, long podId) {
		Preconditions.checkNotNull(pod);
		this.pod = pod;
		this.resourceId = new ResourceID(podName);
		this.podId = podId;
		this.priority = DEFAULT_PRIORITY;
	}

	public KubernetesWorkerNode(Pod pod, String podName, long podId, int priority) {
		Preconditions.checkNotNull(pod);
		this.pod = pod;
		this.resourceId = new ResourceID(podName);
		this.podId = podId;
		this.priority = priority;
	}

	@Override
	public ResourceID getResourceID() {
		return resourceId;
	}

	public long getPodId() {
		return podId;
	}

	public Pod getPod() {
		return pod;
	}

	public int getPriority() {
		return priority;
	}
}
