/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.pact.runtime.iterative.io;

import com.google.common.base.Preconditions;
import eu.stratosphere.nephele.event.task.AbstractTaskEvent;
import eu.stratosphere.nephele.event.task.EventListener;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * a delegating {@link MutableObjectIterator} that interrupts the current thread when a given number of events occured.
 * This is necessary to repetitively read channels when executing iterative data flows. The wrapped iterator must return false
 * on interruption, see {@link eu.stratosphere.pact.runtime.task.util.ReaderInterruptionBehaviors}
 */
public class InterruptingMutableObjectIterator<E> implements MutableObjectIterator<E>, EventListener {

  private final MutableObjectIterator<E> delegate;
  private final String owner;
  private final int numberOfEventsUntilInterrupt;
  private final AtomicInteger eventCounter;

  private static final Log log = LogFactory.getLog(InterruptingMutableObjectIterator.class);

  public InterruptingMutableObjectIterator(MutableObjectIterator<E> delegate, int numberOfEventsUntilInterrupt,
      String owner) {
    Preconditions.checkArgument(numberOfEventsUntilInterrupt > 0);
    this.delegate = delegate;
    this.numberOfEventsUntilInterrupt = numberOfEventsUntilInterrupt;
    this.eventCounter = new AtomicInteger(0);
    this.owner = owner;
  }

  @Override
  public void eventOccurred(AbstractTaskEvent event) {
    int numberOfEventsSeen = eventCounter.incrementAndGet();
    if (log.isInfoEnabled()) {
      log.info("InterruptibleIterator of " + owner + " received " + event.getClass().getSimpleName() +
          "(" + numberOfEventsSeen +")");
    }
    if (numberOfEventsSeen % numberOfEventsUntilInterrupt == 0) {
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public boolean next(E target) throws IOException {
    return delegate.next(target);
  }

}
