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

package org.apache.flink.yarn;

import org.apache.flink.api.common.resources.CPUResource;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.clusterframework.TaskExecutorProcessSpec;
import org.apache.flink.runtime.util.HadoopUtils;
import org.apache.flink.util.TestLogger;

import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;

/**
 * Tests for {@link TaskExecutorProcessSpecContainerResourcePriorityAdapter}.
 */
public class TaskExecutorProcessSpecContainerResourcePriorityAdapterTest extends TestLogger {

	private static final Resource MAX_CONTAINER_RESOURCE = Resource.newInstance(102400, 100);

	private static final String SUPPORTED_EXTERNAL_RESOURCE_NAME = "testing-external-resource";
	private static final long SUPPORTED_EXTERNAL_RESOURCE_MAX = 10000L;

	private static final TaskExecutorProcessSpec TASK_EXECUTOR_PROCESS_SPEC_1 = new TaskExecutorProcessSpec(
		new CPUResource(1.0),
		MemorySize.ofMebiBytes(100),
		MemorySize.ofMebiBytes(100),
		MemorySize.ofMebiBytes(100),
		MemorySize.ofMebiBytes(100),
		MemorySize.ofMebiBytes(100),
		MemorySize.ofMebiBytes(100),
		MemorySize.ofMebiBytes(100),
		MemorySize.ofMebiBytes(100));

	private static final TaskExecutorProcessSpec TASK_EXECUTOR_PROCESS_SPEC_2 = new TaskExecutorProcessSpec(
		new CPUResource(2.0),
		MemorySize.ofMebiBytes(200),
		MemorySize.ofMebiBytes(200),
		MemorySize.ofMebiBytes(200),
		MemorySize.ofMebiBytes(200),
		MemorySize.ofMebiBytes(200),
		MemorySize.ofMebiBytes(200),
		MemorySize.ofMebiBytes(200),
		MemorySize.ofMebiBytes(200));

	private static final TaskExecutorProcessSpec TASK_EXECUTOR_PROCESS_SPEC_EXCEED_MAX = new TaskExecutorProcessSpec(
		new CPUResource(200.0),
		MemorySize.ofMebiBytes(102400),
		MemorySize.ofMebiBytes(102400),
		MemorySize.ofMebiBytes(102400),
		MemorySize.ofMebiBytes(102400),
		MemorySize.ofMebiBytes(102400),
		MemorySize.ofMebiBytes(102400),
		MemorySize.ofMebiBytes(102400),
		MemorySize.ofMebiBytes(102400));

	@Test
	public void testGetResourceFromSpec() {
		final TaskExecutorProcessSpecContainerResourcePriorityAdapter adapter = getAdapter();
		final Resource resource = getResource(adapter, TASK_EXECUTOR_PROCESS_SPEC_1);
		assertThat(resource.getMemory(), is(TASK_EXECUTOR_PROCESS_SPEC_1.getTotalProcessMemorySize().getMebiBytes()));
		assertThat(resource.getVirtualCores(), is(TASK_EXECUTOR_PROCESS_SPEC_1.getCpuCores().getValue().intValue()));
	}

	@Test
	public void testGetPriorityFromSpec() {
		final TaskExecutorProcessSpecContainerResourcePriorityAdapter adapter = getAdapter();
		final Priority priority1 = getPriority(adapter, TASK_EXECUTOR_PROCESS_SPEC_1);
		final Priority priority2 = getPriority(adapter, TASK_EXECUTOR_PROCESS_SPEC_2);
		final Priority priority3 = getPriority(adapter, TASK_EXECUTOR_PROCESS_SPEC_1);
		assertThat(priority1, not(priority2));
		assertThat(priority1, is(priority3));
	}

	@Test
	public void testMaxContainerResource() {
		final TaskExecutorProcessSpecContainerResourcePriorityAdapter adapter = getAdapter();
		assertThat(adapter.getPriorityAndResource(TASK_EXECUTOR_PROCESS_SPEC_EXCEED_MAX).isPresent(), is(false));
	}

	@Test
	public void testGetTaskExecutorProcessSpecAndResource() {
		final TaskExecutorProcessSpecContainerResourcePriorityAdapter adapter = getAdapter();

		final TaskExecutorProcessSpecContainerResourcePriorityAdapter.PriorityAndResource addedPriorityAndResource =
			adapter.getPriorityAndResource(TASK_EXECUTOR_PROCESS_SPEC_1).get();
		final Priority unknownPriority = Priority.newInstance(987);

		final TaskExecutorProcessSpecContainerResourcePriorityAdapter.TaskExecutorProcessSpecAndResource resultSpecAndResource =
			adapter.getTaskExecutorProcessSpecAndResource(addedPriorityAndResource.getPriority()).get();

		assertThat(resultSpecAndResource.getTaskExecutorProcessSpec(), is(TASK_EXECUTOR_PROCESS_SPEC_1));
		assertThat(resultSpecAndResource.getResource(), is(addedPriorityAndResource.getResource()));
		assertThat(adapter.getTaskExecutorProcessSpecAndResource(unknownPriority).isPresent(), is(false));
	}

	@Test
	public void testExternalResource() {
		assumeTrue(isExternalResourceSupported());

		final long amount = 1;
		final TaskExecutorProcessSpecContainerResourcePriorityAdapter adapter =
			getAdapterWithExternalResources(SUPPORTED_EXTERNAL_RESOURCE_NAME, amount);
		final Resource resource = getResource(adapter, TASK_EXECUTOR_PROCESS_SPEC_1);

		final Map<String, Long> resultExternalResources = ResourceInformationReflector.INSTANCE.getExternalResources(resource);
		assertThat(resultExternalResources.size(), is(1));
		assertThat(resultExternalResources.get(SUPPORTED_EXTERNAL_RESOURCE_NAME), is(amount));
	}

	@Test(expected = IllegalStateException.class)
	public void testExternalResourceFailExceedMax() {
		assumeTrue(isExternalResourceSupported());

		getAdapterWithExternalResources(SUPPORTED_EXTERNAL_RESOURCE_NAME, SUPPORTED_EXTERNAL_RESOURCE_MAX + 1);
	}

	@Test(expected = IllegalStateException.class)
	public void testExternalResourceFailResourceTypeNotSupported() {
		assumeTrue(isExternalResourceSupported());

		getAdapterWithExternalResources("testing-unsupported-resource", 1);
	}

	@Test(expected = IllegalStateException.class)
	public void testExternalResourceFailHadoopVersionNotSupported() {
		assumeFalse(isExternalResourceSupported());

		getAdapterWithExternalResources(SUPPORTED_EXTERNAL_RESOURCE_NAME, 100);
	}

	private static TaskExecutorProcessSpecContainerResourcePriorityAdapter getAdapter() {
		return new TaskExecutorProcessSpecContainerResourcePriorityAdapter(MAX_CONTAINER_RESOURCE, Collections.emptyMap());
	}

	private static TaskExecutorProcessSpecContainerResourcePriorityAdapter getAdapterWithExternalResources(String name, long amount) {
		final Resource maxResource = Resource.newInstance(MAX_CONTAINER_RESOURCE.getMemory(), MAX_CONTAINER_RESOURCE.getVirtualCores());
		ResourceInformationReflector.INSTANCE.setResourceInformation(
			maxResource, SUPPORTED_EXTERNAL_RESOURCE_NAME, SUPPORTED_EXTERNAL_RESOURCE_MAX);

		final Map<String, Long> externalResources = new HashMap<>();
		externalResources.put(name, amount);

		return new TaskExecutorProcessSpecContainerResourcePriorityAdapter(maxResource, externalResources);
	}

	private static Resource getResource(TaskExecutorProcessSpecContainerResourcePriorityAdapter adapter, TaskExecutorProcessSpec spec) {
		return adapter.getPriorityAndResource(spec).get().getResource();
	}

	private static Priority getPriority(TaskExecutorProcessSpecContainerResourcePriorityAdapter adapter, TaskExecutorProcessSpec spec) {
		return adapter.getPriorityAndResource(spec).get().getPriority();
	}

	private static boolean isExternalResourceSupported() {
		return HadoopUtils.isMinHadoopVersion(2, 10) &&
			ClassLoader.getSystemResource("resource-types.xml") != null;
	}
}
