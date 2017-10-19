/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.util;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

public class ResourceGuardTest extends TestLogger {

	@Test
	public void testClose() {
		ResourceGuard resourceGuard = new ResourceGuard();
		Assert.assertFalse(resourceGuard.isClosed());
		resourceGuard.close();
		Assert.assertTrue(resourceGuard.isClosed());
		try {
			resourceGuard.acquireResource();
			Assert.fail();
		} catch (IOException ignore) {
		}
	}

	@Test
	public void testAcquireReleaseClose() throws IOException {
		ResourceGuard resourceGuard = new ResourceGuard();
		ResourceGuard.Lease lease = resourceGuard.acquireResource();
		Assert.assertEquals(1, resourceGuard.getLeaseCount());
		lease.close();
		Assert.assertEquals(0, resourceGuard.getLeaseCount());
		resourceGuard.close();
		Assert.assertTrue(resourceGuard.isClosed());
	}

	@Test
	public void testCloseBlockIfAcquired() throws Exception {
		ResourceGuard resourceGuard = new ResourceGuard();
		ResourceGuard.Lease lease_1 = resourceGuard.acquireResource();
		AtomicBoolean checker = new AtomicBoolean(true);

		Thread closerThread = new Thread() {
			@Override
			public void run() {
				try {
					// this line should block until all acquires are matched by releases.
					resourceGuard.close();
					checker.set(false);
				} catch (Exception ignore) {
					checker.set(false);
				}
			}
		};

		closerThread.start();

		ResourceGuard.Lease lease_2 = resourceGuard.acquireResource();
		lease_2.close();
		Assert.assertTrue(checker.get());

		// this matches the first acquire and will unblock the close.
		lease_1.close();
		closerThread.join(60_000);
		Assert.assertFalse(checker.get());
	}

	@Test
	public void testInterruptHandledCorrectly() throws Exception {
		ResourceGuard resourceGuard = new ResourceGuard();
		ResourceGuard.Lease lease = resourceGuard.acquireResource();
		AtomicBoolean checker = new AtomicBoolean(true);

		Thread closerThread = new Thread() {
			@Override
			public void run() {
				try {
					// this line should block until all acquires are matched by releases.
					resourceGuard.close();
					checker.set(false);
				} catch (Exception ignore) {
					checker.set(false);
				}
			}
		};

		closerThread.start();
		closerThread.interrupt();

		Assert.assertTrue(checker.get());

		lease.close();
		closerThread.join(60_000);
		Assert.assertFalse(checker.get());
	}

	@Test
	public void testLeaseCloseIsIdempotent() throws Exception {
		ResourceGuard resourceGuard = new ResourceGuard();
		ResourceGuard.Lease lease_1 = resourceGuard.acquireResource();
		ResourceGuard.Lease lease_2 = resourceGuard.acquireResource();
		Assert.assertEquals(2, resourceGuard.getLeaseCount());
		lease_1.close();
		Assert.assertEquals(1, resourceGuard.getLeaseCount());
		lease_1.close();
		Assert.assertEquals(1, resourceGuard.getLeaseCount());
		lease_2.close();
		Assert.assertEquals(0, resourceGuard.getLeaseCount());
		ResourceGuard.Lease lease_3 = resourceGuard.acquireResource();
		Assert.assertEquals(1, resourceGuard.getLeaseCount());
		lease_2.close();
		Assert.assertEquals(1, resourceGuard.getLeaseCount());
		lease_3.close();
		Assert.assertEquals(0, resourceGuard.getLeaseCount());
		resourceGuard.close();
	}
}
