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
import eu.stratosphere.pact.runtime.iterative.concurrent.SolutionsetBroker;
import eu.stratosphere.pact.runtime.iterative.driver.SolutionsetMatchDriver;
import eu.stratosphere.pact.runtime.iterative.event.EndOfSuperstepEvent;
import eu.stratosphere.pact.runtime.iterative.event.TerminationEvent;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;

/**
 * A specialized {@link IterationIntermediatePactTask} for workset iterations that receives a hash-join (a {@link MutableHashTable}) from the
 * iteration head. This hash-join will hold the solution set of the workset iteration and all output emitted from the
 * {@link eu.stratosphere.pact.common.stubs.MatchStub} that runs inside this task will be inserted into the build-side of the hash-join, which
 * will form the final output of the workset iteration.
 *
 * This implementation is only allowed to run a {@link eu.stratosphere.pact.runtime.iterative.driver.SolutionsetMatchDriver} inside!
 *
 * @param <S>
 * @param <OT>
 */
public class WorksetIterationSolutionsetJoinTask<S extends Stub, OT> extends AbstractIterativePactTask<S, OT> {

  private static final Log log = LogFactory.getLog(WorksetIterationSolutionsetJoinTask.class);

  private SolutionsetMatchDriver solutionsetMatchDriver;

  @Override
  public void invoke() throws Exception {

    Preconditions.checkState(SolutionsetMatchDriver.class == driver.getClass());
    solutionsetMatchDriver = (SolutionsetMatchDriver) driver;

    //TODO type safety
    /* retrieve hashJoin instantiated by the iteration head */
    MutableHashTable hashJoin = SolutionsetBroker.instance().get(brokerKey());

    solutionsetMatchDriver.injectHashJoin(hashJoin);

    while (!terminationRequested()) {

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
