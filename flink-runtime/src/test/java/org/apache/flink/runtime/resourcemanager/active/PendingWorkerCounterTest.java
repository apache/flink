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

package org.apache.flink.runtime.resourcemanager.active;

import org.apache.flink.runtime.resourcemanager.WorkerResourceSpec;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

/**
 * Tests for {@link PendingWorkerCounter}.
 */
public class PendingWorkerCounterTest extends TestLogger {

	@Test
	public void testPendingWorkerCounterIncreaseAndDecrease() {
		final WorkerResourceSpec spec1 = new WorkerResourceSpec.Builder().setCpuCores(1.0).build();
		final WorkerResourceSpec spec2 = new WorkerResourceSpec.Builder().setCpuCores(2.0).build();

		final PendingWorkerCounter counter = new PendingWorkerCounter();
		assertThat(counter.getTotalNum(), is(0));
		assertThat(counter.getNum(spec1), is(0));
		assertThat(counter.getNum(spec2), is(0));

		assertThat(counter.increaseAndGet(spec1), is(1));
		assertThat(counter.getTotalNum(), is(1));
		assertThat(counter.getNum(spec1), is(1));
		assertThat(counter.getNum(spec2), is(0));

		assertThat(counter.increaseAndGet(spec1), is(2));
		assertThat(counter.getTotalNum(), is(2));
		assertThat(counter.getNum(spec1), is(2));
		assertThat(counter.getNum(spec2), is(0));

		assertThat(counter.increaseAndGet(spec2), is(1));
		assertThat(counter.getTotalNum(), is(3));
		assertThat(counter.getNum(spec1), is(2));
		assertThat(counter.getNum(spec2), is(1));

		assertThat(counter.decreaseAndGet(spec1), is(1));
		assertThat(counter.getTotalNum(), is(2));
		assertThat(counter.getNum(spec1), is(1));
		assertThat(counter.getNum(spec2), is(1));

		assertThat(counter.decreaseAndGet(spec2), is(0));
		assertThat(counter.getTotalNum(), is(1));
		assertThat(counter.getNum(spec1), is(1));
		assertThat(counter.getNum(spec2), is(0));
	}

	@Test(expected = IllegalStateException.class)
	public void testPendingWorkerCounterDecreaseOnZero() {
		final WorkerResourceSpec spec = new WorkerResourceSpec.Builder().build();
		final PendingWorkerCounter counter = new PendingWorkerCounter();
		counter.decreaseAndGet(spec);
	}
}
