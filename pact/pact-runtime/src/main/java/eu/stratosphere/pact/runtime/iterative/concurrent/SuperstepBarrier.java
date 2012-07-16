/***********************************************************************************************************************
 *
 * Copyright (C) 2012 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.pact.runtime.iterative.concurrent;

import eu.stratosphere.nephele.event.task.AbstractTaskEvent;
import eu.stratosphere.nephele.event.task.EventListener;

import java.util.concurrent.locks.AbstractQueuedSynchronizer;

/** a resettable one-shot latch */
public class SuperstepBarrier implements EventListener {

  private Sync sync = new Sync();

  /** setup the barrier, has to be called at the beginning of each superstep */
  public void setup() {
    sync = new Sync();
  }

  /** wait on the barrier */
  public void waitForOtherWorkers() throws InterruptedException {
    sync.tryAcquireShared(0);
  }

  /** barrier will release the waiting thread if an event occurs*/
  @Override
  public void eventOccurred(AbstractTaskEvent event) {
    sync.releaseShared(0);
  }

  private class Sync extends AbstractQueuedSynchronizer {

    private static final int OPEN = 1;

    @Override
    protected int tryAcquireShared(int ignored) {
      return getState() == OPEN ? 1 : -1;
    }

    @Override
    protected boolean tryReleaseShared(int ignored) {
      setState(OPEN);
      return true;
    }
  }
}
