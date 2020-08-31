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
import org.apache.flink.util.TestLogger;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.impl.pb.ResourcePBImpl;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link TaskExecutorProcessSpecContainerResourceAdapter}.
 */
public class TaskExecutorProcessSpecContainerResourceAdapterTest extends TestLogger {

	@Test
	public void testMatchVcores() {
		final TaskExecutorProcessSpecContainerResourceAdapter.MatchingStrategy strategy =
			TaskExecutorProcessSpecContainerResourceAdapter.MatchingStrategy.MATCH_VCORE;
		final int minMemMB = 100;
		final int minVcore = 10;
		final int unitMemMB = 50;
		final int unitVcore = 5;
		final TaskExecutorProcessSpecContainerResourceAdapter adapter =
			new TaskExecutorProcessSpecContainerResourceAdapter(
				minMemMB,
				minVcore,
				Integer.MAX_VALUE,
				Integer.MAX_VALUE,
				unitMemMB,
				unitVcore,
				Collections.emptyMap());

		// mem < minMem, vcore < minVcore, should be normalized to [minMem, minVcore]
		final TaskExecutorProcessSpec taskExecutorProcessSpec1 = new TaskExecutorProcessSpec(
			new CPUResource(8.0),
			MemorySize.ZERO,
			MemorySize.ZERO,
			MemorySize.ofMebiBytes(20),
			MemorySize.ofMebiBytes(20),
			MemorySize.ofMebiBytes(20),
			MemorySize.ofMebiBytes(20),
			MemorySize.ZERO,
			MemorySize.ZERO);

		// mem = minMem, mem % unitMem = 0, vcore = minVcore, vcore % unitVcore = 0, should not be changed
		final TaskExecutorProcessSpec taskExecutorProcessSpec2 = new TaskExecutorProcessSpec(
			new CPUResource(10.0),
			MemorySize.ZERO,
			MemorySize.ZERO,
			MemorySize.ofMebiBytes(25),
			MemorySize.ofMebiBytes(25),
			MemorySize.ofMebiBytes(25),
			MemorySize.ofMebiBytes(25),
			MemorySize.ZERO,
			MemorySize.ZERO);

		// mem > minMem, mem % unitMem != 0, vcore < minVcore, should be normalized to [n * unitMem, minVcore]
		final TaskExecutorProcessSpec taskExecutorProcessSpec3 = new TaskExecutorProcessSpec(
			new CPUResource(8.0),
			MemorySize.ZERO,
			MemorySize.ZERO,
			MemorySize.ofMebiBytes(30),
			MemorySize.ofMebiBytes(30),
			MemorySize.ofMebiBytes(30),
			MemorySize.ofMebiBytes(30),
			MemorySize.ZERO,
			MemorySize.ZERO);

		// mem < minMem, vcore > minVcore, vcore % unitVcore != 0, should be normalized to [minMem, n * unitVcore]
		final TaskExecutorProcessSpec taskExecutorProcessSpec4 = new TaskExecutorProcessSpec(
			new CPUResource(12.0),
			MemorySize.ZERO,
			MemorySize.ZERO,
			MemorySize.ofMebiBytes(20),
			MemorySize.ofMebiBytes(20),
			MemorySize.ofMebiBytes(20),
			MemorySize.ofMebiBytes(20),
			MemorySize.ZERO,
			MemorySize.ZERO);

		final Resource containerResource1 = Resource.newInstance(100, 10);
		final Resource containerResource2 = Resource.newInstance(150, 10);
		final Resource containerResource3 = Resource.newInstance(100, 15);

		assertThat(adapter.getTaskExecutorProcessSpec(containerResource1, strategy), empty());
		assertThat(adapter.getTaskExecutorProcessSpec(containerResource2, strategy), empty());

		assertThat(adapter.tryComputeContainerResource(taskExecutorProcessSpec1).get(), is(containerResource1));
		assertThat(adapter.tryComputeContainerResource(taskExecutorProcessSpec2).get(), is(containerResource1));
		assertThat(adapter.tryComputeContainerResource(taskExecutorProcessSpec3).get(), is(containerResource2));
		assertThat(adapter.tryComputeContainerResource(taskExecutorProcessSpec4).get(), is(containerResource3));

		assertThat(adapter.getTaskExecutorProcessSpec(containerResource1, strategy), containsInAnyOrder(taskExecutorProcessSpec1, taskExecutorProcessSpec2));
		assertThat(adapter.getTaskExecutorProcessSpec(containerResource2, strategy), contains(taskExecutorProcessSpec3));
		assertThat(adapter.getTaskExecutorProcessSpec(containerResource3, strategy), contains(taskExecutorProcessSpec4));
	}

	@Test
	public void testIgnoreVcores() {
		final TaskExecutorProcessSpecContainerResourceAdapter.MatchingStrategy strategy =
			TaskExecutorProcessSpecContainerResourceAdapter.MatchingStrategy.IGNORE_VCORE;
		final int minMemMB = 100;
		final int minVcore = 1;
		final int unitMemMB = 50;
		final int unitVcore = 1;
		final TaskExecutorProcessSpecContainerResourceAdapter adapter =
			new TaskExecutorProcessSpecContainerResourceAdapter(
				minMemMB,
				minVcore,
				Integer.MAX_VALUE,
				Integer.MAX_VALUE,
				unitMemMB,
				unitVcore,
				Collections.emptyMap());

		// mem < minMem, should be normalized to [minMem, vcore], equivalent to [minMem, 1]
		final TaskExecutorProcessSpec taskExecutorProcessSpec1 = new TaskExecutorProcessSpec(
			new CPUResource(5.0),
			MemorySize.ZERO,
			MemorySize.ZERO,
			MemorySize.ofMebiBytes(10),
			MemorySize.ofMebiBytes(10),
			MemorySize.ofMebiBytes(10),
			MemorySize.ofMebiBytes(10),
			MemorySize.ZERO,
			MemorySize.ZERO);

		// mem < minMem, should be normalized to [minMem, vcore], equivalent to [minMem, 1]
		final TaskExecutorProcessSpec taskExecutorProcessSpec2 = new TaskExecutorProcessSpec(
			new CPUResource(10.0),
			MemorySize.ZERO,
			MemorySize.ZERO,
			MemorySize.ofMebiBytes(10),
			MemorySize.ofMebiBytes(10),
			MemorySize.ofMebiBytes(10),
			MemorySize.ofMebiBytes(10),
			MemorySize.ZERO,
			MemorySize.ZERO);

		// mem = minMem, mem % unitMem = 0, should not be changed, equivalent to [mem, 1]
		final TaskExecutorProcessSpec taskExecutorProcessSpec3 = new TaskExecutorProcessSpec(
			new CPUResource(5.0),
			MemorySize.ZERO,
			MemorySize.ZERO,
			MemorySize.ofMebiBytes(25),
			MemorySize.ofMebiBytes(25),
			MemorySize.ofMebiBytes(25),
			MemorySize.ofMebiBytes(25),
			MemorySize.ZERO,
			MemorySize.ZERO);

		// mem > minMem, mem % unitMem != 0, should be normalized to [n * unitMem, vcore], equivalent to [n * unitMem, 1]
		final TaskExecutorProcessSpec taskExecutorProcessSpec4 = new TaskExecutorProcessSpec(
			new CPUResource(5.0),
			MemorySize.ZERO,
			MemorySize.ZERO,
			MemorySize.ofMebiBytes(30),
			MemorySize.ofMebiBytes(30),
			MemorySize.ofMebiBytes(30),
			MemorySize.ofMebiBytes(30),
			MemorySize.ZERO,
			MemorySize.ZERO);

		final Resource containerResource1 = Resource.newInstance(100, 5);
		final Resource containerResource2 = Resource.newInstance(100, 10);
		final Resource containerResource3 = Resource.newInstance(150, 5);

		final Resource containerResource4 = Resource.newInstance(100, 1);
		final Resource containerResource5 = Resource.newInstance(150, 1);

		assertThat(adapter.tryComputeContainerResource(taskExecutorProcessSpec1).get(), is(containerResource1));
		assertThat(adapter.tryComputeContainerResource(taskExecutorProcessSpec2).get(), is(containerResource2));
		assertThat(adapter.tryComputeContainerResource(taskExecutorProcessSpec3).get(), is(containerResource1));
		assertThat(adapter.tryComputeContainerResource(taskExecutorProcessSpec4).get(), is(containerResource3));

		assertThat(adapter.getEquivalentContainerResource(containerResource4, strategy), containsInAnyOrder(containerResource1, containerResource2));
		assertThat(adapter.getEquivalentContainerResource(containerResource5, strategy), contains(containerResource3));

		assertThat(adapter.getTaskExecutorProcessSpec(containerResource4, strategy), containsInAnyOrder(taskExecutorProcessSpec1, taskExecutorProcessSpec2, taskExecutorProcessSpec3));
		assertThat(adapter.getTaskExecutorProcessSpec(containerResource5, strategy), contains(taskExecutorProcessSpec4));
	}

	@Test
	public void testMaxLimit() {
		final int minMemMB = 100;
		final int minVcore = 1;
		final int maxMemMB = 1000;
		final int maxVcore = 10;
		final int unitMemMB = 100;
		final int unitVcore = 1;
		final TaskExecutorProcessSpecContainerResourceAdapter adapter =
			new TaskExecutorProcessSpecContainerResourceAdapter(
				minMemMB,
				minVcore,
				maxMemMB,
				maxVcore,
				unitMemMB,
				unitVcore,
				Collections.emptyMap());

		final TaskExecutorProcessSpec taskExecutorProcessSpec1 = new TaskExecutorProcessSpec(
			new CPUResource(5.0),
			MemorySize.ZERO,
			MemorySize.ZERO,
			MemorySize.ofMebiBytes(300),
			MemorySize.ofMebiBytes(300),
			MemorySize.ofMebiBytes(300),
			MemorySize.ofMebiBytes(300),
			MemorySize.ZERO,
			MemorySize.ZERO);
		final TaskExecutorProcessSpec taskExecutorProcessSpec2 = new TaskExecutorProcessSpec(
			new CPUResource(15.0),
			MemorySize.ZERO,
			MemorySize.ZERO,
			MemorySize.ofMebiBytes(10),
			MemorySize.ofMebiBytes(10),
			MemorySize.ofMebiBytes(10),
			MemorySize.ofMebiBytes(10),
			MemorySize.ZERO,
			MemorySize.ZERO);

		assertFalse(adapter.tryComputeContainerResource(taskExecutorProcessSpec1).isPresent());
		assertFalse(adapter.tryComputeContainerResource(taskExecutorProcessSpec2).isPresent());
	}

	@Test
	public void testMatchResourceWithDifferentImplementation() {
		final TaskExecutorProcessSpecContainerResourceAdapter.MatchingStrategy strategy =
			TaskExecutorProcessSpecContainerResourceAdapter.MatchingStrategy.IGNORE_VCORE;
		final int minMemMB = 1;
		final int minVcore = 1;
		final int unitMemMB = 1;
		final int unitVcore = 1;

		final TaskExecutorProcessSpecContainerResourceAdapter adapter =
			new TaskExecutorProcessSpecContainerResourceAdapter(
				minMemMB,
				minVcore,
				Integer.MAX_VALUE,
				Integer.MAX_VALUE,
				unitMemMB,
				unitVcore,
				Collections.emptyMap());

		final TaskExecutorProcessSpec taskExecutorProcessSpec = new TaskExecutorProcessSpec(
			new CPUResource(1.0),
			MemorySize.ZERO,
			MemorySize.ZERO,
			MemorySize.ofMebiBytes(100),
			MemorySize.ofMebiBytes(200),
			MemorySize.ofMebiBytes(300),
			MemorySize.ofMebiBytes(400),
			MemorySize.ZERO,
			MemorySize.ZERO);

		Optional<Resource> resourceOpt = adapter.tryComputeContainerResource(taskExecutorProcessSpec);
		assertTrue(resourceOpt.isPresent());
		Resource resourceImpl1 = resourceOpt.get();

		Resource resourceImpl2 = new TestingResourceImpl(
			resourceImpl1.getMemory(),
			resourceImpl1.getVirtualCores() + 1);

		assertThat(adapter.getEquivalentContainerResource(resourceImpl2, strategy), contains(resourceImpl1));
		assertThat(adapter.getTaskExecutorProcessSpec(resourceImpl2, strategy), contains(taskExecutorProcessSpec));
	}

	@Test
	public void testMatchInternalContainerResourceIgnoresZeroValueExternalResources() {
		final Map<String, Long> externalResources1 = new HashMap<>();
		final Map<String, Long> externalResources2 = new HashMap<>();

		externalResources1.put("foo", 0L);
		externalResources1.put("bar", 1L);
		externalResources2.put("zoo", 0L);
		externalResources2.put("bar", 1L);

		final TaskExecutorProcessSpecContainerResourceAdapter.InternalContainerResource internalContainerResource1 =
			new TaskExecutorProcessSpecContainerResourceAdapter.InternalContainerResource(1024, 1, externalResources1);
		final TaskExecutorProcessSpecContainerResourceAdapter.InternalContainerResource internalContainerResource2 =
			new TaskExecutorProcessSpecContainerResourceAdapter.InternalContainerResource(1024, 1, externalResources2);

		assertEquals(internalContainerResource1, internalContainerResource2);
	}

	private static class TestingResourceImpl extends ResourcePBImpl {

		private TestingResourceImpl(int memory, int vcore) {
			super();
			setMemory(memory);
			setVirtualCores(vcore);
		}

		@Override
		public int hashCode() {
			return super.hashCode() + 678;
		}
	}
}
