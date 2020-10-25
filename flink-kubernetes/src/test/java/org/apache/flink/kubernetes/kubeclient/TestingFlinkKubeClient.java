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
import org.apache.flink.kubernetes.kubeclient.resources.KubernetesWatch;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.util.Preconditions;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Testing implementation of {@link FlinkKubeClient}.
 */
public class TestingFlinkKubeClient implements FlinkKubeClient {

	private final Function<KubernetesPod, CompletableFuture<Void>> createTaskManagerPodFunction;
	private final Function<String, CompletableFuture<Void>> stopPodFunction;
	private final Consumer<String> stopAndCleanupClusterConsumer;
	private final Function<Map<String, String>, List<KubernetesPod>> getPodsWithLabelsFunction;
	private final BiFunction<Map<String, String>, PodCallbackHandler, KubernetesWatch> watchPodsAndDoCallbackFunction;

	private TestingFlinkKubeClient(
			Function<KubernetesPod, CompletableFuture<Void>> createTaskManagerPodFunction,
			Function<String, CompletableFuture<Void>> stopPodFunction,
			Consumer<String> stopAndCleanupClusterConsumer,
			Function<Map<String, String>, List<KubernetesPod>> getPodsWithLabelsFunction,
			BiFunction<Map<String, String>, PodCallbackHandler, KubernetesWatch> watchPodsAndDoCallbackFunction) {

		this.createTaskManagerPodFunction = createTaskManagerPodFunction;
		this.stopPodFunction = stopPodFunction;
		this.stopAndCleanupClusterConsumer = stopAndCleanupClusterConsumer;
		this.getPodsWithLabelsFunction = getPodsWithLabelsFunction;
		this.watchPodsAndDoCallbackFunction = watchPodsAndDoCallbackFunction;
	}

	@Override
	public void createJobManagerComponent(KubernetesJobManagerSpecification kubernetesJMSpec) {
		throw new UnsupportedOperationException();
	}

	@Override
	public CompletableFuture<Void> createTaskManagerPod(KubernetesPod kubernetesPod) {
		return createTaskManagerPodFunction.apply(kubernetesPod);
	}

	@Override
	public CompletableFuture<Void> stopPod(String podName) {
		return stopPodFunction.apply(podName);
	}

	@Override
	public void stopAndCleanupCluster(String clusterId) {
		stopAndCleanupClusterConsumer.accept(clusterId);
	}

	@Override
	public Optional<KubernetesService> getRestService(String clusterId) {
		throw new UnsupportedOperationException();
	}

	@Override
	public Optional<Endpoint> getRestEndpoint(String clusterId) {
		throw new UnsupportedOperationException();
	}

	@Override
	public List<KubernetesPod> getPodsWithLabels(Map<String, String> labels) {
		return getPodsWithLabelsFunction.apply(labels);
	}

	@Override
	public void handleException(Exception e) {
		throw new UnsupportedOperationException();
	}

	@Override
	public KubernetesWatch watchPodsAndDoCallback(Map<String, String> labels, PodCallbackHandler podCallbackHandler) {
		return watchPodsAndDoCallbackFunction.apply(labels, podCallbackHandler);
	}

	@Override
	public void close() throws Exception {
		// noop
	}

	public static Builder builder() {
		return new Builder();
	}

	/**
	 * Builder class for {@link TestingFlinkKubeClient}.
	 */
	public static class Builder {
		private Function<KubernetesPod, CompletableFuture<Void>> createTaskManagerPodFunction =
				(ignore) -> FutureUtils.completedVoidFuture();
		private Function<String, CompletableFuture<Void>> stopPodFunction =
				(ignore) -> FutureUtils.completedVoidFuture();
		private Consumer<String> stopAndCleanupClusterConsumer =
				(ignore) -> {};
		private Function<Map<String, String>, List<KubernetesPod>> getPodsWithLabelsFunction =
				(ignore) -> Collections.emptyList();
		private BiFunction<Map<String, String>, PodCallbackHandler, KubernetesWatch> watchPodsAndDoCallbackFunction =
				(ignore1, ignore2) -> new MockKubernetesWatch();

		private Builder() {}

		public Builder setCreateTaskManagerPodFunction(Function<KubernetesPod, CompletableFuture<Void>> createTaskManagerPodFunction) {
			this.createTaskManagerPodFunction = Preconditions.checkNotNull(createTaskManagerPodFunction);
			return this;
		}

		public Builder setStopPodFunction(Function<String, CompletableFuture<Void>> stopPodFunction) {
			this.stopPodFunction = Preconditions.checkNotNull(stopPodFunction);
			return this;
		}

		public Builder setStopAndCleanupClusterConsumer(Consumer<String> stopAndCleanupClusterConsumer) {
			this.stopAndCleanupClusterConsumer = Preconditions.checkNotNull(stopAndCleanupClusterConsumer);
			return this;
		}

		public Builder setGetPodsWithLabelsFunction(Function<Map<String, String>, List<KubernetesPod>> getPodsWithLabelsFunction) {
			this.getPodsWithLabelsFunction = Preconditions.checkNotNull(getPodsWithLabelsFunction);
			return this;
		}

		public Builder setWatchPodsAndDoCallbackFunction(BiFunction<Map<String, String>, PodCallbackHandler, KubernetesWatch> watchPodsAndDoCallbackFunction) {
			this.watchPodsAndDoCallbackFunction = Preconditions.checkNotNull(watchPodsAndDoCallbackFunction);
			return this;
		}

		public TestingFlinkKubeClient build() {
			return new TestingFlinkKubeClient(
					createTaskManagerPodFunction,
					stopPodFunction,
					stopAndCleanupClusterConsumer,
					getPodsWithLabelsFunction,
					watchPodsAndDoCallbackFunction);
		}
	}

	/**
	 * Testing implementation of {@link KubernetesWatch}.
	 */
	public static class MockKubernetesWatch extends KubernetesWatch {
		public MockKubernetesWatch() {
			super(null);
		}

		@Override
		public void close() {
			// noop
		}
	}
}
