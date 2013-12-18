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

import eu.stratosphere.pact.runtime.iterative.task.IterationHeadPactTask;
import eu.stratosphere.pact.runtime.iterative.task.IterationTailPactTask;

import java.util.concurrent.CountDownLatch;

/**
 * Resettable barrier to synchronize {@link IterationHeadPactTask} and {@link IterationTailPactTask} in case of
 * iterations that contain a separate solution set tail.
 */
public class SolutionSetUpdateBarrier {

    private CountDownLatch latch;

    public void setup() {
        latch = new CountDownLatch(1);
    }

    /**
     * Waits (blocking) on barrier.
     *
     * @throws InterruptedException
     */
    public void waitForSolutionSetUpdate() throws InterruptedException {
        latch.await();
    }

    /**
     * Releases the waiting thread.
     */
    public void notifySolutionSetUpdate() {
        latch.countDown();
    }
}
