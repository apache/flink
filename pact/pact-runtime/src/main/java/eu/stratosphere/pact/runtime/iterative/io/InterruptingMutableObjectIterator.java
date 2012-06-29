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

import eu.stratosphere.nephele.event.task.AbstractTaskEvent;
import eu.stratosphere.nephele.event.task.EventListener;
import eu.stratosphere.pact.common.util.MutableObjectIterator;

import java.io.IOException;

public class InterruptingMutableObjectIterator<E> implements MutableObjectIterator<E>, EventListener {

  private final MutableObjectIterator<E> delegate;
  private final String owner;

  public InterruptingMutableObjectIterator(MutableObjectIterator<E> delegate, String owner) {
    this.delegate = delegate;
    this.owner = owner;
  }

  @Override
  public void eventOccurred(AbstractTaskEvent event) {
    //TODO this class has to know how many events to receive until interruption!
    System.out.println("InterruptibleIterator of " + owner + " received " + event.getClass().getSimpleName() +
        " [" + System.currentTimeMillis() + "]");
    Thread.currentThread().interrupt();
  }

  @Override
  public boolean next(E target) throws IOException {
    return delegate.next(target);
  }

}
