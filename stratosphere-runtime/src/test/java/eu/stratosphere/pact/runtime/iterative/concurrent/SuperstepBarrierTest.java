/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.pact.runtime.iterative.concurrent;

import eu.stratosphere.nephele.event.task.AbstractTaskEvent;
import eu.stratosphere.pact.runtime.iterative.event.AllWorkersDoneEvent;
import eu.stratosphere.pact.runtime.iterative.event.TerminationEvent;

import org.junit.Test;

import java.util.Random;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SuperstepBarrierTest {

	@Test
	public void syncAllWorkersDone() throws InterruptedException {
		for (int n = 0; n < 20; n++) {
			sync(new AllWorkersDoneEvent());
		}
	}

	@Test
	public void syncTermination() throws InterruptedException {
		for (int n = 0; n < 20; n++) {
			sync(new TerminationEvent());
		}
	}

	private void sync(AbstractTaskEvent event) throws InterruptedException {

		TerminationSignaled terminationSignaled = new TerminationSignaled();

		SuperstepBarrier barrier = new SuperstepBarrier(getClass().getClassLoader());
		barrier.setup();

		Thread headThread = new Thread(new IterationHead(barrier, terminationSignaled));
		Thread syncThread = new Thread(new IterationSync(barrier, event));

		headThread.start();
		syncThread.start();

		headThread.join();
		syncThread.join();

		if (event instanceof TerminationEvent) {
			assertTrue(terminationSignaled.isTerminationSignaled());
		} else {
			assertFalse(terminationSignaled.isTerminationSignaled());
		}
	}

	class IterationHead implements Runnable {

		private final SuperstepBarrier barrier;

		private final TerminationSignaled terminationSignaled;

		private final Random random;

		IterationHead(SuperstepBarrier barrier, TerminationSignaled terminationSignaled) {
			this.barrier = barrier;
			this.terminationSignaled = terminationSignaled;
			random = new Random();
		}

		@Override
		public void run() {
			try {
				Thread.sleep(random.nextInt(10));

				barrier.waitForOtherWorkers();

				if (barrier.terminationSignaled()) {
					terminationSignaled.setTerminationSignaled();
				}

			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
	}

	class IterationSync implements Runnable {

		private final SuperstepBarrier barrier;

		private final AbstractTaskEvent event;

		private final Random random;

		IterationSync(SuperstepBarrier barrier, AbstractTaskEvent event) {
			this.barrier = barrier;
			this.event = event;
			random = new Random();
		}

		@Override
		public void run() {
			try {
				Thread.sleep(random.nextInt(10));

				barrier.eventOccurred(event);

			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
	}

	class TerminationSignaled {

		private volatile boolean terminationSignaled;

		public boolean isTerminationSignaled() {
			return terminationSignaled;
		}

		public void setTerminationSignaled() {
			terminationSignaled = true;
		}
	}
}
