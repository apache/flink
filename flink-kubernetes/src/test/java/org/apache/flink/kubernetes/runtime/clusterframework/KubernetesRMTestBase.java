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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.utils.KubernetesConnectionManager;
import org.apache.flink.runtime.resourcemanager.ResourceManager;
import org.apache.flink.runtime.resourcemanager.exceptions.ResourceManagerException;
import org.apache.flink.util.TestLogger;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import org.junit.Assert;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Test base for testing Kubernetes RM.
 */
public class KubernetesRMTestBase extends TestLogger {

	protected static final String APP_ID = "k8s-cluster-1234";

	protected static final String CONTAINER_IMAGE = "flink-k8s:latest";

	protected static final String MASTER_URL = "http://127.0.0.1:49359";

	protected static final String RPC_PORT = "11111";

	protected static final String HOSTNAME = "127.0.0.1";

	protected Configuration flinkConf;

	public void setup() {
		flinkConf = new Configuration();
		flinkConf.setString(KubernetesConfigOptions.CLUSTER_ID, APP_ID);
		flinkConf.setString(KubernetesConfigOptions.MASTER_URL, MASTER_URL);
		flinkConf.setString(KubernetesConfigOptions.CONTAINER_IMAGE, CONTAINER_IMAGE);
		flinkConf.setString(TaskManagerOptions.RPC_PORT, RPC_PORT);
		flinkConf.setString(RestOptions.ADDRESS, HOSTNAME);
		flinkConf.setString(JobManagerOptions.ADDRESS, HOSTNAME);
		flinkConf.setInteger(TaskManagerOptions.TASK_MANAGER_HEAP_MEMORY, 128);
		flinkConf.setLong(TaskManagerOptions.MANAGED_MEMORY_SIZE, 128);
		flinkConf.setLong(TaskManagerOptions.FLOATING_MANAGED_MEMORY_SIZE, 10);
		flinkConf.setLong(TaskManagerOptions.NETWORK_BUFFERS_MEMORY_MAX, 64 << 20);
		flinkConf.setInteger(TaskManagerOptions.TASK_MANAGER_PROCESS_NETTY_MEMORY, 10);
		flinkConf.setInteger(TaskManagerOptions.TASK_MANAGER_PROCESS_NATIVE_MEMORY, 10);
		flinkConf.setInteger(TaskManagerOptions.TASK_MANAGER_PROCESS_HEAP_MEMORY, 10);
	}

	/**
	 * Testing implementation of KubernetesConnectionManager.
	 */
	public static class TestingKubernetesConnectionManager extends KubernetesConnectionManager {

		private List<Service> services = new ArrayList<>();

		private List<Pod> pods = new ArrayList<>();

		private List<ConfigMap> configMaps = new ArrayList<>();

		public TestingKubernetesConnectionManager(Configuration conf) {
			super(conf);
		}

		public void createPod(Pod pod) {
			pods.add(pod);
		}

		public void removePods(Map<String, String> labels) throws ResourceManagerException {
			Supplier<Void> action = () -> {
				List<Pod> removePods = pods.stream().filter(p -> Objects.equals(p.getMetadata().getLabels(), labels)).collect(Collectors.toList());
				pods.removeAll(removePods);
				return null;
			};
			runWithRetry(action);
		}

		public void removePod(Pod pod) throws ResourceManagerException {
			Supplier<Void> action = () -> {
				pods.remove(pod);
				return null;
			};
			runWithRetry(action);
		}

		public void removePod(String podName) throws ResourceManagerException {
			Supplier<Void> action = () -> {
				Optional<Pod> targetPodOpt = pods.stream().filter(
					p -> p.getMetadata().getName().equals(podName)
				).findFirst();
				if (targetPodOpt.isPresent()) {
					pods.remove(targetPodOpt.get());
				}
				return null;
			};
			runWithRetry(action);
		}

		public PodList getPods(Map<String, String> labels) throws ResourceManagerException {
			Supplier<PodList> action = () -> {
				PodList podList = new PodList();
				List<Pod> matchedPods = pods.stream().filter(
					p -> p.getMetadata().getLabels().entrySet().containsAll(labels.entrySet())
				).collect(Collectors.toList());
				podList.setItems(matchedPods);
				return podList;
			};
			return (PodList) runWithRetry(action);
		}

		public Service getService(String serviceName) throws ResourceManagerException {
			Supplier<Service> action = () -> {
				List<Service> matchedServices = services.stream().filter(s -> Objects.equals(serviceName, s.getMetadata().getName())).collect(Collectors.toList());
				return matchedServices.isEmpty() ? null : matchedServices.get(0);
			};
			return (Service) runWithRetry(action);
		}

		public ConfigMap createOrReplaceConfigMap(ConfigMap configMap) throws ResourceManagerException {
			Supplier<ConfigMap> action = () -> {
				configMaps.add(configMap);
				return configMap;
			};
			return (ConfigMap) runWithRetry(action);
		}

		public Watch createAndStartPodsWatcher(Map<String, String> labels,
			BiConsumer<Watcher.Action, Pod> podEventHandler, Consumer<Exception> watcherCloseHandler) {
			return null;
		}
	}

	public ResourceManager mockResourceManager(Configuration flinkConf, KubernetesConnectionManager kubernetesConnectionManager) {
		return null;
	}

	public void testConnectionLostButNotReachMaxRetryTimes() throws ResourceManagerException {
		Configuration newFlinkConf = new Configuration(flinkConf);
		newFlinkConf.setInteger(KubernetesConfigOptions.KUBERNETES_CONNECTION_RETRY_TIMES, 3);
		newFlinkConf.setLong(KubernetesConfigOptions.KUBERNETES_CONNECTION_RETRY_INTERVAL_MS, 100);
		KubernetesConnectionManager kubernetesConnectionManager =
			new KubernetesRMTestBase.TestingKubernetesConnectionManager(newFlinkConf);
		KubernetesConnectionManager spyKubernetesConnectionManager = Mockito.spy(kubernetesConnectionManager);
		ResourceManager spyKubernetesRM =
			mockResourceManager(newFlinkConf, spyKubernetesConnectionManager);

		AtomicInteger failureCount = new AtomicInteger(0);
		Supplier failed2TimesSupplier = () -> {
			if (failureCount.getAndIncrement() < 2) {
				throw new RuntimeException("Mock exception");
			}
			return null;
		};
		Mockito.doAnswer(new Answer() {
			@Override
			public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
				kubernetesConnectionManager.runWithRetry(failed2TimesSupplier);
				return null;
			}
		}).when(spyKubernetesConnectionManager).runWithRetry(Mockito.any());
		// start session RM
		try {
			spyKubernetesRM.start();
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail("RM should be started successfully.");
		}
	}

	public void testConnectionLostAndReachMaxRetryTimes() throws ResourceManagerException {
		Configuration newFlinkConf = new Configuration(flinkConf);
		newFlinkConf.setInteger(KubernetesConfigOptions.KUBERNETES_CONNECTION_RETRY_TIMES, 3);
		newFlinkConf.setLong(KubernetesConfigOptions.KUBERNETES_CONNECTION_RETRY_INTERVAL_MS, 100);
		KubernetesConnectionManager kubernetesConnectionManager =
			new KubernetesRMTestBase.TestingKubernetesConnectionManager(newFlinkConf);
		KubernetesConnectionManager spyKubernetesConnectionManager = Mockito.spy(kubernetesConnectionManager);
		ResourceManager spyKubernetesRM =
			mockResourceManager(newFlinkConf, spyKubernetesConnectionManager);

		Supplier alwaysFailedSupplier = () -> {
			throw new RuntimeException("Mock exception");
		};
		Mockito.doAnswer(new Answer() {
			@Override
			public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
				kubernetesConnectionManager.runWithRetry(alwaysFailedSupplier);
				return null;
			}
		}).when(spyKubernetesConnectionManager).runWithRetry(Mockito.any());
		// start session RM
		try {
			spyKubernetesRM.start();
			Assert.fail("RM should throw exception.");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
