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

import com.google.common.collect.Lists;
import eu.stratosphere.nephele.event.task.AbstractTaskEvent;
import eu.stratosphere.nephele.io.AbstractRecordWriter;
import eu.stratosphere.pact.common.stubs.Stub;
import eu.stratosphere.pact.runtime.iterative.concurrent.BlockingBackChannel;
import eu.stratosphere.pact.runtime.iterative.concurrent.BlockingBackChannelBroker;
import eu.stratosphere.pact.runtime.iterative.concurrent.Broker;
import eu.stratosphere.pact.runtime.iterative.event.EndOfSuperstepEvent;
import eu.stratosphere.pact.runtime.iterative.event.TerminationEvent;
import eu.stratosphere.pact.runtime.iterative.io.DataOutputCollector;
import eu.stratosphere.pact.runtime.iterative.monitoring.IterationMonitoring;
import eu.stratosphere.pact.runtime.task.PactTaskContext;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.List;

//TODO could this be an output???
/**
 * The tail of an iteration, which is able to run a {@link eu.stratosphere.pact.runtime.task.PactDriver} inside. It will send back its output to
 * the iteration's head via a {@link BlockingBackChannel}. Therefore this task must be scheduled on the same instance as the head.
 */
public class IterationTailPactTask<S extends Stub, OT> extends AbstractIterativePactTask<S, OT>
    implements PactTaskContext<S, OT> {

  private List<AbstractRecordWriter> validOutputs;

  private static final Log log = LogFactory.getLog(IterationTailPactTask.class);

  private BlockingBackChannel retrieveBackChannel() throws Exception {
    // blocking call to retrieve the backchannel from the iteration head
    Broker<BlockingBackChannel> broker = BlockingBackChannelBroker.instance();
    return broker.get(brokerKey());
  }

  @Override
  protected void initOutputs() throws Exception {
    super.initOutputs();
    //TODO remove implicit order assumption
    //TODO type safety
    validOutputs = Lists.newArrayListWithCapacity(eventualOutputs.size() - 1);

    for (int n = 0; n < eventualOutputs.size() - 1; n++) {
      validOutputs.add(eventualOutputs.get(n));
    }
  }

  @Override
  public void invoke() throws Exception {

    boolean isJoinOnConstantDataPath = driver instanceof RepeatableHashJoinMatchDriver || driver instanceof RepeatableHashJoinMatchDriver2;

    // Initially retreive the backchannel from the iteration head
    final BlockingBackChannel backChannel = retrieveBackChannel();

    // redirect output to the backchannel
    //TODO type safety
    DataOutputCollector outputCollector = new DataOutputCollector(backChannel.getWriteEnd(),
        createOutputTypeSerializer(), validOutputs);
    output = outputCollector;

    while (!terminationRequested()) {

      notifyMonitor(IterationMonitoring.Event.TAIL_STARTING);
      if (log.isInfoEnabled()) {
        log.info(formatLogString("starting iteration [" + currentIteration() + "]"));
      }

      notifyMonitor(IterationMonitoring.Event.TAIL_PACT_STARTING);
      if (!inFirstIteration() && !isJoinOnConstantDataPath) {
        reinstantiateDriver();
      }

      super.invoke();
      notifyMonitor(IterationMonitoring.Event.TAIL_PACT_FINISHED);

      long elementsCollected = outputCollector.getElementsCollectedAndReset();
      if (log.isInfoEnabled()) {
        log.info("IterationTail [" + getEnvironment().getIndexInSubtaskGroup() +"] inserted [" +
            elementsCollected + "] elements into backchannel in iteration [" + currentIteration() + "]");
      }

      if (log.isInfoEnabled()) {
        log.info(formatLogString("finishing iteration [" + currentIteration() + "]"));
      }

      if (!terminationRequested()) {
        backChannel.notifyOfEndOfSuperstep();
        propagateEvent(new EndOfSuperstepEvent());
        incrementIterationCounter();
      } else {
        propagateEvent(new TerminationEvent());
      }
      notifyMonitor(IterationMonitoring.Event.TAIL_FINISHED);
    }

    if (isJoinOnConstantDataPath) {

      if (driver instanceof RepeatableHashJoinMatchDriver) {
        ((RepeatableHashJoinMatchDriver) driver).finalCleanup();
      } else {
        ((RepeatableHashJoinMatchDriver2) driver).finalCleanup();
      }
    }
  }

  private void propagateEvent(AbstractTaskEvent event) throws IOException, InterruptedException {
    if (log.isInfoEnabled()) {
      log.info(formatLogString("propagating " + event.getClass().getSimpleName()));
    }
    for (AbstractRecordWriter<?> validOutput : validOutputs) {
      validOutput.publishEvent(event);
    }
  }
}
