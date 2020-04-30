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

package org.apache.flink.runtime.memory;

import org.apache.flink.core.memory.MemoryType;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for the shared resource acquisition and initialization through the {@link MemoryManager}.
 */
public class MemoryManagerSharedResourcesTest {

	@Test
	public void getSameTypeGetsSameHandle() throws Exception {
		final MemoryManager memoryManager = createMemoryManager();

		final OpaqueMemoryResource<TestResource> resource1 = memoryManager.getSharedMemoryResourceForManagedMemory(
			"type", TestResource::new, 0.1);
		final OpaqueMemoryResource<TestResource> resource2 = memoryManager.getSharedMemoryResourceForManagedMemory(
			"type", TestResource::new, 0.1);

		assertNotSame(resource1, resource2);
		assertSame(resource1.getResourceHandle(), resource2.getResourceHandle());
	}

	@Test
	public void getDifferentTypeGetsDifferentResources() throws Exception {
		final MemoryManager memoryManager = createMemoryManager();

		final OpaqueMemoryResource<TestResource> resource1 = memoryManager.getSharedMemoryResourceForManagedMemory(
			"type1", TestResource::new, 0.1);
		final OpaqueMemoryResource<TestResource> resource2 = memoryManager.getSharedMemoryResourceForManagedMemory(
			"type2", TestResource::new, 0.1);

		assertNotSame(resource1, resource2);
		assertNotSame(resource1.getResourceHandle(), resource2.getResourceHandle());
	}

	@Test
	public void testAllocatesFractionOfTotalMemory() throws Exception {
		final MemoryManager memoryManager = createMemoryManager();
		final double fraction = 0.2;

		final OpaqueMemoryResource<TestResource> resource = memoryManager.getSharedMemoryResourceForManagedMemory(
			"type", TestResource::new, fraction);

		assertEquals((long) (0.2 * memoryManager.getMemorySize()), resource.getSize());
	}

	@Test
	public void getAllocateNewReservesMemory() throws Exception {
		final MemoryManager memoryManager = createMemoryManager();

		memoryManager.getSharedMemoryResourceForManagedMemory("type", TestResource::new, 0.5);

		assertEquals(memoryManager.getMemorySize() / 2, memoryManager.availableMemory(MemoryType.OFF_HEAP));
	}

	@Test
	public void getExistingDoesNotAllocateAdditionalMemory() throws Exception {
		final MemoryManager memoryManager = createMemoryManager();
		memoryManager.getSharedMemoryResourceForManagedMemory("type", TestResource::new, 0.8);
		final long freeMemory = memoryManager.availableMemory(MemoryType.OFF_HEAP);

		memoryManager.getSharedMemoryResourceForManagedMemory("type", TestResource::new, 0.8);

		assertEquals(freeMemory, memoryManager.availableMemory(MemoryType.OFF_HEAP));
	}

	@Test
	public void testFailReservation() throws Exception {
		final MemoryManager memoryManager = createMemoryManager();
		memoryManager.getSharedMemoryResourceForManagedMemory("type", TestResource::new, 0.8);

		try {
			memoryManager.getSharedMemoryResourceForManagedMemory("type2", TestResource::new, 0.8);
			fail("exception expected");
		}
		catch (MemoryAllocationException e) {
			// expected
		}
	}

	@Test
	public void testPartialReleaseDoesNotReleaseMemory() throws Exception {
		final MemoryManager memoryManager = createMemoryManager();
		final OpaqueMemoryResource<TestResource> resource1 = memoryManager.getSharedMemoryResourceForManagedMemory(
			"type", TestResource::new, 0.1);
		memoryManager.getSharedMemoryResourceForManagedMemory(
			"type", TestResource::new, 0.1);
		assertFalse(memoryManager.verifyEmpty());

		resource1.close();

		assertFalse(resource1.getResourceHandle().closed);
		assertFalse(memoryManager.verifyEmpty());
	}

	@Test
	public void testLastReleaseReleasesMemory() throws Exception {
		final MemoryManager memoryManager = createMemoryManager();
		final OpaqueMemoryResource<TestResource> resource1 = memoryManager.getSharedMemoryResourceForManagedMemory(
			"type", TestResource::new, 0.1);
		final OpaqueMemoryResource<TestResource> resource2 = memoryManager.getSharedMemoryResourceForManagedMemory(
			"type", TestResource::new, 0.1);
		assertFalse(memoryManager.verifyEmpty());

		resource1.close();
		resource2.close();

		assertTrue(resource1.getResourceHandle().closed);
		assertTrue(memoryManager.verifyEmpty());
	}

	@Test
	public void testPartialReleaseDoesNotDisposeResource() throws Exception {
		final MemoryManager memoryManager = createMemoryManager();
		final OpaqueMemoryResource<TestResource> resource1 = memoryManager.getSharedMemoryResourceForManagedMemory(
			"type", TestResource::new, 0.1);
		memoryManager.getSharedMemoryResourceForManagedMemory(
			"type", TestResource::new, 0.1);

		resource1.close();

		assertFalse(resource1.getResourceHandle().closed);
		assertFalse(memoryManager.verifyEmpty());
	}

	@Test
	public void testLastReleaseDisposesResource() throws Exception {
		final MemoryManager memoryManager = createMemoryManager();
		final OpaqueMemoryResource<TestResource> resource1 = memoryManager.getSharedMemoryResourceForManagedMemory(
			"type", TestResource::new, 0.1);
		final OpaqueMemoryResource<TestResource> resource2 = memoryManager.getSharedMemoryResourceForManagedMemory(
			"type", TestResource::new, 0.1);

		resource1.close();
		resource2.close();

		assertTrue(resource1.getResourceHandle().closed);
		assertTrue(resource2.getResourceHandle().closed);
		assertTrue(memoryManager.verifyEmpty());
	}

	@Test
	public void getAllocateExternalResource() throws Exception {
		final MemoryManager memoryManager = createMemoryManager();
		final OpaqueMemoryResource<TestResource> resource = memoryManager.getExternalSharedMemoryResource(
			"external-type", TestResource::new, 1337);

		assertEquals(1337, resource.getSize());
	}

	@Test
	public void getExistingExternalResource() throws Exception {
		final MemoryManager memoryManager = createMemoryManager();
		final OpaqueMemoryResource<TestResource> resource1 = memoryManager.getExternalSharedMemoryResource(
			"external-type", TestResource::new, 1337);

		final OpaqueMemoryResource<TestResource> resource2 = memoryManager.getExternalSharedMemoryResource(
			"external-type", TestResource::new, 1337);

		assertNotSame(resource1, resource2);
		assertSame(resource1.getResourceHandle(), resource2.getResourceHandle());
	}

	@Test
	public void getDifferentExternalResources() throws Exception {
		final MemoryManager memoryManager = createMemoryManager();
		final OpaqueMemoryResource<TestResource> resource1 = memoryManager.getExternalSharedMemoryResource(
			"external-type-1", TestResource::new, 1337);

		final OpaqueMemoryResource<TestResource> resource2 = memoryManager.getExternalSharedMemoryResource(
			"external-type-2", TestResource::new, 1337);

		assertNotSame(resource1, resource2);
		assertNotSame(resource1.getResourceHandle(), resource2.getResourceHandle());
	}

	@Test
	public void testReleaseDisposesExternalResource() throws Exception {
		final MemoryManager memoryManager = createMemoryManager();
		final OpaqueMemoryResource<TestResource> resource = memoryManager.getExternalSharedMemoryResource(
			"external-type", TestResource::new, 1337);

		resource.close();

		assertTrue(resource.getResourceHandle().closed);
	}

	// ------------------------------------------------------------------------
	//  Utils
	// ------------------------------------------------------------------------

	private static MemoryManager createMemoryManager() {
		final long size = 128 * 1024 * 1024;
		final MemoryManager mm = MemoryManager.forDefaultPageSize(size);

		// this is to guard test assumptions
		assertEquals(size, mm.getMemorySize());
		assertEquals(size, mm.availableMemory(MemoryType.OFF_HEAP));

		return mm;
	}

	private static final class TestResource implements AutoCloseable {

		final long size;
		boolean closed;

		TestResource(long size) {
			this.size = size;
		}

		@Override
		public void close() {
			closed = true;
		}
	}
}
