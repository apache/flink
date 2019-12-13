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

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests for the {@link SharedResources} class.
 */
public class SharedResourcesTest {

	@Test
	public void testAllocatedResourcesInMap() throws Exception {
		final SharedResources resources = new SharedResources();

		final TestResource tr = resources
				.getOrAllocateSharedResource("theType", new Object(), TestResource::new, 100)
				.resourceHandle();

		assertEquals(1, resources.getNumResources());
		assertFalse(tr.closed);
	}

	@Test
	public void testIntermediateReleaseDoesNotRemoveFromMap() throws Exception {
		final SharedResources resources = new SharedResources();
		final String type = "theType";
		final Object leaseHolder1 = new Object();
		final Object leaseHolder2 = new Object();

		resources.getOrAllocateSharedResource(type, leaseHolder1, TestResource::new, 100);
		resources.getOrAllocateSharedResource(type, leaseHolder2, TestResource::new, 100);

		resources.release(type, leaseHolder1);

		assertEquals(1, resources.getNumResources());
	}

	@Test
	public void testReleaseIsIdempotent() throws Exception {
		final SharedResources resources = new SharedResources();
		final String type = "theType";
		final Object leaseHolder1 = new Object();
		final Object leaseHolder2 = new Object();

		resources.getOrAllocateSharedResource(type, leaseHolder1, TestResource::new, 100);
		resources.getOrAllocateSharedResource(type, leaseHolder2, TestResource::new, 100);

		resources.release(type, leaseHolder2);
		resources.release(type, leaseHolder2);

		assertEquals(1, resources.getNumResources());
	}

	@Test
	public void testLastReleaseRemovesFromMap() throws Exception {
		final SharedResources resources = new SharedResources();
		final String type = "theType";
		final Object leaseHolder = new Object();
		resources.getOrAllocateSharedResource(type, leaseHolder, TestResource::new, 100);

		resources.release(type, leaseHolder);

		assertEquals(0, resources.getNumResources());
	}

	@Test
	public void testLastReleaseDisposesResource() throws Exception {
		final SharedResources resources = new SharedResources();
		final String type = "theType";
		final Object leaseHolder = new Object();

		final TestResource tr = resources
				.getOrAllocateSharedResource(type, leaseHolder, TestResource::new, 100)
				.resourceHandle();

		resources.release(type, leaseHolder);

		assertTrue(tr.closed);
	}

	@Test
	public void testReleaseNoneExistingLease() throws Exception {
		final SharedResources resources = new SharedResources();

		resources.release("theType", new Object());

		assertEquals(0, resources.getNumResources());
	}

	// ------------------------------------------------------------------------

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
