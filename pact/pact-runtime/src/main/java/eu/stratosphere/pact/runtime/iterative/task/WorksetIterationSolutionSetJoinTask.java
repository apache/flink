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

import com.google.common.base.Preconditions;
import eu.stratosphere.nephele.event.task.AbstractTaskEvent;
import eu.stratosphere.nephele.io.AbstractRecordWriter;
import eu.stratosphere.pact.common.stubs.Stub;
import eu.stratosphere.pact.runtime.hash.MutableHashTable;
import eu.stratosphere.pact.runtime.iterative.concurrent.SolutionSetBroker;
import eu.stratosphere.pact.runtime.iterative.event.EndOfSuperstepEvent;
import eu.stratosphere.pact.runtime.iterative.event.TerminationEvent;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;

public class WorksetIterationSolutionSetJoinTask<S extends Stub, OT> extends AbstractIterativePactTask<S, OT> {

  private static final Log log = LogFactory.getLog(WorksetIterationSolutionSetJoinTask.class);

  private SolutionSetMatchDriver solutionSetMatchDriver;

  @Override
  public void invoke() throws Exception {

    Preconditions.checkState(SolutionSetMatchDriver.class.equals(driver.getClass()));

    //TODO type safety
    solutionSetMatchDriver = (SolutionSetMatchDriver) driver;

    /* retrieve hashJoin from the head */
    MutableHashTable hashJoin = SolutionSetBroker.instance().get(brokerKey());

    solutionSetMatchDriver.setHashJoin(hashJoin);

    while (!terminationRequested() && currentIteration() < 6) {

      if (log.isInfoEnabled()) {
        log.info(formatLogString("starting iteration [" + currentIteration() + "]"));
      }

      super.invoke();

      if (log.isInfoEnabled()) {
        log.info(formatLogString("finishing iteration [" + currentIteration() + "]"));
      }

      if (!terminationRequested()) {
        propagateEvent(new EndOfSuperstepEvent());
        incrementIterationCounter();
      } else {
        propagateEvent(new TerminationEvent());
      }
    }
  }

  private void propagateEvent(AbstractTaskEvent event) throws IOException, InterruptedException {
    if (log.isInfoEnabled()) {
      log.info(formatLogString("propagating " + event.getClass().getSimpleName()));
    }
    for (AbstractRecordWriter<?> eventualOutput : eventualOutputs) {
      eventualOutput.publishEvent(event);
    }
  }
}
