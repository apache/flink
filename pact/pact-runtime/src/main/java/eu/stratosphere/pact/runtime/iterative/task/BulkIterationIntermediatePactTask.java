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

package eu.stratosphere.pact.runtime.iterative.task;

import eu.stratosphere.nephele.event.task.AbstractTaskEvent;
import eu.stratosphere.nephele.io.AbstractRecordWriter;
import eu.stratosphere.pact.common.stubs.Stub;
import eu.stratosphere.pact.runtime.iterative.event.Callback;
import eu.stratosphere.pact.runtime.iterative.event.EndOfSuperstepEvent;
import eu.stratosphere.pact.runtime.iterative.event.TerminationEvent;
import eu.stratosphere.pact.runtime.task.util.ReaderInterruptionBehavior;
import eu.stratosphere.pact.runtime.task.util.ReaderInterruptionBehaviors;

import java.io.IOException;

public class BulkIterationIntermediatePactTask<S extends Stub, OT> extends AbstractIterativePactTask<S, OT> {

  private int numIterations = 0;
  private boolean terminated = false;

  @Override
  protected ReaderInterruptionBehavior readerInterruptionBehavior() {
    return ReaderInterruptionBehaviors.FALSE_ON_INTERRUPT;
  }


  @Override
  public void invoke() throws Exception {

    listenToEndOfSuperstep(new Callback<EndOfSuperstepEvent>() {
      @Override
      public void execute(EndOfSuperstepEvent event) throws Exception {
        propagateEvent(event);
      }
    });

    listenToTermination(new Callback<TerminationEvent>() {
      @Override
      public void execute(TerminationEvent event) throws Exception {
        propagateEvent(event);
        terminated = true;
      }
    });

    boolean inFirstIteration = true;

    while (!terminated/* && numIterations < 5*/) {

      System.out.println("Intermediate: starting iteration [" + numIterations + "] [" + System.currentTimeMillis() + "]");
      if (!inFirstIteration) {
        reinstantiateDriver();
        inFirstIteration = false;
      }

      super.invoke();

      System.out.println("Intermediate: finishing iteration [" + numIterations + "] [" + System.currentTimeMillis() + "]");
      numIterations++;
    }
  }

  private void propagateEvent(AbstractTaskEvent event) throws IOException, InterruptedException {
    System.out.println("Intermediate: got " + event.getClass().getSimpleName() + " [" + System.currentTimeMillis() + "]");
    for (AbstractRecordWriter<?> eventualOutput : eventualOutputs) {
      eventualOutput.publishEvent(event);
    }
  }

}
