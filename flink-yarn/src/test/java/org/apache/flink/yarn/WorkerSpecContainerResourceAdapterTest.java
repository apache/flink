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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.resourcemanager.WorkerResourceSpec;
import org.apache.flink.util.TestLogger;

import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.impl.pb.ResourcePBImpl;
import org.junit.Test;

import java.util.Collections;
import java.util.Optional;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link WorkerSpecContainerResourceAdapter}.
 */
public class WorkerSpecContainerResourceAdapterTest extends TestLogger {

	@Test
	public void testMatchVcores() {
		final WorkerSpecContainerResourceAdapter.MatchingStrategy strategy =
			WorkerSpecContainerResourceAdapter.MatchingStrategy.MATCH_VCORE;
		final int minMemMB = 100;
		final int minVcore = 10;
		final WorkerSpecContainerResourceAdapter adapter =
			new WorkerSpecContainerResourceAdapter(
				getConfigProcessSpecEqualsWorkerSpec(),
				minMemMB,
				minVcore,
				Integer.MAX_VALUE,
				Integer.MAX_VALUE,
				Collections.emptyMap());

		final WorkerResourceSpec workerSpec1 = new WorkerResourceSpec.Builder()
			.setCpuCores(1.0)
			.setTaskHeapMemoryMB(10)
			.setTaskOffHeapMemoryMB(10)
			.setNetworkMemoryMB(10)
			.setManagedMemoryMB(10)
			.build();
		final WorkerResourceSpec workerSpec2 = new WorkerResourceSpec.Builder()
			.setCpuCores(10.0)
			.setTaskHeapMemoryMB(25)
			.setTaskOffHeapMemoryMB(25)
			.setNetworkMemoryMB(25)
			.setManagedMemoryMB(25)
			.build();
		final WorkerResourceSpec workerSpec3 = new WorkerResourceSpec.Builder()
			.setCpuCores(5.0)
			.setTaskHeapMemoryMB(30)
			.setTaskOffHeapMemoryMB(30)
			.setNetworkMemoryMB(30)
			.setManagedMemoryMB(30)
			.build();
		final WorkerResourceSpec workerSpec4 = new WorkerResourceSpec.Builder()
			.setCpuCores(15.0)
			.setTaskHeapMemoryMB(10)
			.setTaskOffHeapMemoryMB(10)
			.setNetworkMemoryMB(10)
			.setManagedMemoryMB(10)
			.build();

		final Resource containerResource1 = Resource.newInstance(100, 10);
		final Resource containerResource2 = Resource.newInstance(200, 10);
		final Resource containerResource3 = Resource.newInstance(100, 20);

		assertThat(adapter.getWorkerSpecs(containerResource1, strategy), empty());
		assertThat(adapter.getWorkerSpecs(containerResource2, strategy), empty());

		assertThat(adapter.tryComputeContainerResource(workerSpec1).get(), is(containerResource1));
		assertThat(adapter.tryComputeContainerResource(workerSpec2).get(), is(containerResource1));
		assertThat(adapter.tryComputeContainerResource(workerSpec3).get(), is(containerResource2));
		assertThat(adapter.tryComputeContainerResource(workerSpec4).get(), is(containerResource3));

		assertThat(adapter.getWorkerSpecs(containerResource1, strategy), containsInAnyOrder(workerSpec1, workerSpec2));
		assertThat(adapter.getWorkerSpecs(containerResource2, strategy), contains(workerSpec3));
		assertThat(adapter.getWorkerSpecs(containerResource3, strategy), contains(workerSpec4));
	}

	@Test
	public void testIgnoreVcores() {
		final WorkerSpecContainerResourceAdapter.MatchingStrategy strategy =
			WorkerSpecContainerResourceAdapter.MatchingStrategy.IGNORE_VCORE;
		final int minMemMB = 100;
		final int minVcore = 1;
		final WorkerSpecContainerResourceAdapter adapter =
			new WorkerSpecContainerResourceAdapter(
				getConfigProcessSpecEqualsWorkerSpec(),
				minMemMB,
				minVcore,
				Integer.MAX_VALUE,
				Integer.MAX_VALUE,
				Collections.emptyMap());

		final WorkerResourceSpec workerSpec1 = new WorkerResourceSpec.Builder()
			.setCpuCores(5.0)
			.setTaskHeapMemoryMB(10)
			.setTaskOffHeapMemoryMB(10)
			.setNetworkMemoryMB(10)
			.setManagedMemoryMB(10)
			.build();
		final WorkerResourceSpec workerSpec2 = new WorkerResourceSpec.Builder()
			.setCpuCores(10.0)
			.setTaskHeapMemoryMB(10)
			.setTaskOffHeapMemoryMB(10)
			.setNetworkMemoryMB(10)
			.setManagedMemoryMB(10)
			.build();
		final WorkerResourceSpec workerSpec3 = new WorkerResourceSpec.Builder()
			.setCpuCores(5.0)
			.setTaskHeapMemoryMB(25)
			.setTaskOffHeapMemoryMB(25)
			.setNetworkMemoryMB(25)
			.setManagedMemoryMB(25)
			.build();
		final WorkerResourceSpec workerSpec4 = new WorkerResourceSpec.Builder()
			.setCpuCores(5.0)
			.setTaskHeapMemoryMB(30)
			.setTaskOffHeapMemoryMB(30)
			.setNetworkMemoryMB(30)
			.setManagedMemoryMB(30)
			.build();

		final Resource containerResource1 = Resource.newInstance(100, 5);
		final Resource containerResource2 = Resource.newInstance(100, 10);
		final Resource containerResource3 = Resource.newInstance(200, 5);

		final Resource containerResource4 = Resource.newInstance(100, 1);
		final Resource containerResource5 = Resource.newInstance(200, 1);

		assertThat(adapter.tryComputeContainerResource(workerSpec1).get(), is(containerResource1));
		assertThat(adapter.tryComputeContainerResource(workerSpec2).get(), is(containerResource2));
		assertThat(adapter.tryComputeContainerResource(workerSpec3).get(), is(containerResource1));
		assertThat(adapter.tryComputeContainerResource(workerSpec4).get(), is(containerResource3));

		assertThat(adapter.getEquivalentContainerResource(containerResource4, strategy), containsInAnyOrder(containerResource1, containerResource2));
		assertThat(adapter.getEquivalentContainerResource(containerResource5, strategy), contains(containerResource3));

		assertThat(adapter.getWorkerSpecs(containerResource4, strategy), containsInAnyOrder(workerSpec1, workerSpec2, workerSpec3));
		assertThat(adapter.getWorkerSpecs(containerResource5, strategy), contains(workerSpec4));
	}

	@Test
	public void testMaxLimit() {
		final int minMemMB = 100;
		final int minVcore = 1;
		final int maxMemMB = 1000;
		final int maxVcore = 10;
		final WorkerSpecContainerResourceAdapter adapter =
			new WorkerSpecContainerResourceAdapter(
				getConfigProcessSpecEqualsWorkerSpec(),
				minMemMB,
				minVcore,
				maxMemMB,
				maxVcore,
				Collections.emptyMap());

		final WorkerResourceSpec workerSpec1 = new WorkerResourceSpec.Builder()
			.setCpuCores(5.0)
			.setTaskHeapMemoryMB(300)
			.setTaskOffHeapMemoryMB(300)
			.setNetworkMemoryMB(300)
			.setManagedMemoryMB(300)
			.build();
		final WorkerResourceSpec workerSpec2 = new WorkerResourceSpec.Builder()
			.setCpuCores(15.0)
			.setTaskHeapMemoryMB(10)
			.setTaskOffHeapMemoryMB(10)
			.setNetworkMemoryMB(10)
			.setManagedMemoryMB(10)
			.build();

		assertFalse(adapter.tryComputeContainerResource(workerSpec1).isPresent());
		assertFalse(adapter.tryComputeContainerResource(workerSpec2).isPresent());
	}

	@Test
	public void testMatchResourceWithDifferentImplementation() {
		final WorkerSpecContainerResourceAdapter.MatchingStrategy strategy =
			WorkerSpecContainerResourceAdapter.MatchingStrategy.IGNORE_VCORE;
		final int minMemMB = 1;
		final int minVcore = 1;

		final WorkerSpecContainerResourceAdapter adapter =
			new WorkerSpecContainerResourceAdapter(
				getConfigProcessSpecEqualsWorkerSpec(),
				minMemMB,
				minVcore,
				Integer.MAX_VALUE,
				Integer.MAX_VALUE,
				Collections.emptyMap());

		final WorkerResourceSpec workerSpec = new WorkerResourceSpec.Builder()
			.setCpuCores(1.0)
			.setTaskHeapMemoryMB(100)
			.setTaskOffHeapMemoryMB(200)
			.setNetworkMemoryMB(300)
			.setManagedMemoryMB(400)
			.build();

		Optional<Resource> resourceOpt = adapter.tryComputeContainerResource(workerSpec);
		assertTrue(resourceOpt.isPresent());
		Resource resourceImpl1 = resourceOpt.get();

		Resource resourceImpl2 = new TestingResourceImpl(
			resourceImpl1.getMemory(),
			resourceImpl1.getVirtualCores() + 1);

		assertThat(adapter.getEquivalentContainerResource(resourceImpl2, strategy), contains(resourceImpl1));
		assertThat(adapter.getWorkerSpecs(resourceImpl2, strategy), contains(workerSpec));
	}

	private Configuration getConfigProcessSpecEqualsWorkerSpec() {
		final Configuration config = new Configuration();
		config.set(TaskManagerOptions.FRAMEWORK_HEAP_MEMORY, MemorySize.ZERO);
		config.set(TaskManagerOptions.FRAMEWORK_OFF_HEAP_MEMORY, MemorySize.ZERO);
		config.set(TaskManagerOptions.JVM_METASPACE, MemorySize.ZERO);
		config.set(TaskManagerOptions.JVM_OVERHEAD_MIN, MemorySize.ZERO);
		config.set(TaskManagerOptions.JVM_OVERHEAD_MAX, MemorySize.ZERO);
		return config;
	}

	private class TestingResourceImpl extends ResourcePBImpl {

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
