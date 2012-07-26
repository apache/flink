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
import com.google.common.collect.Lists;
import eu.stratosphere.nephele.event.task.AbstractTaskEvent;
import eu.stratosphere.nephele.io.AbstractRecordWriter;
import eu.stratosphere.nephele.io.Writer;
import eu.stratosphere.nephele.services.memorymanager.DataInputView;
import eu.stratosphere.nephele.services.memorymanager.MemorySegment;
import eu.stratosphere.pact.common.generic.types.TypeSerializer;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.Stub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.runtime.io.InputViewIterator;
import eu.stratosphere.pact.runtime.iterative.concurrent.BlockingBackChannel;
import eu.stratosphere.pact.runtime.iterative.concurrent.BlockingBackChannelBroker;
import eu.stratosphere.pact.runtime.iterative.concurrent.Broker;
import eu.stratosphere.pact.runtime.iterative.concurrent.SuperstepBarrier;
import eu.stratosphere.pact.runtime.iterative.event.AllWorkersDoneEvent;
import eu.stratosphere.pact.runtime.iterative.event.EndOfSuperstepEvent;
import eu.stratosphere.pact.runtime.iterative.event.TerminationEvent;
import eu.stratosphere.pact.runtime.iterative.io.SerializedUpdateBuffer;
import eu.stratosphere.pact.runtime.shipping.PactRecordOutputCollector;
import eu.stratosphere.pact.runtime.task.PactTaskContext;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.List;

/**
 * The head is responsible for coordinating an iteration and can run a {@link eu.stratosphere.pact.runtime.task.PactDriver} inside. It will read
 * the initial input and establish a {@link BlockingBackChannel} to the iteration's tail. After successfully processing the input, it will send
 * {@link EndOfSuperstepEvent} events to its outputs. It must also be connected to a synchronization task and after each superstep, it will wait
 * until it receives an {@link AllWorkersDoneEvent} from the sync, which signals that all other heads have also finished their iteration. Starting with
 * the second iteration, the input for the head is the output of the tail, transmitted through the backchannel. Once the iteration is done, the head
 * will send a {@link TerminationEvent} to all it's connected tasks, signalling them to shutdown.
 *
 * Contracts for this class:
 *
 * The final output of the iteration must be connected as last task, the sync right before that.
 *
 * The input for the iteration must arrive in the first gate
 */
public class BulkIterationHeadPactTask<S extends Stub, OT> extends AbstractIterativePactTask<S, OT>
    implements PactTaskContext<S, OT> {

  private static final Log log = LogFactory.getLog(BulkIterationHeadPactTask.class);

  /**
   * the iteration head prepares the backchannel: it allocates memory, instantiates a {@link BlockingBackChannel} and
   * hands it to the iteration tail via a {@link Broker} singleton
   **/
  private BlockingBackChannel initBackChannel() throws Exception {

    TaskConfig taskConfig = getTaskConfig();

    // compute the size of the memory available to the backchannel
    long completeMemorySize = taskConfig.getMemorySize();
    long backChannelMemorySize = (long) (completeMemorySize * taskConfig.getBackChannelMemoryFraction());
    taskConfig.setMemorySize(completeMemorySize - backChannelMemorySize);

    // allocate the memory available to the backchannel
    List<MemorySegment> segments = Lists.newArrayList();
    int segmentSize = getMemoryManager().getPageSize();
    getMemoryManager().allocatePages(this, segments, backChannelMemorySize);

    // instantiate the backchannel
    BlockingBackChannel backChannel = new BlockingBackChannel(new SerializedUpdateBuffer(segments, segmentSize,
        getIOManager()));

    // hand the backchannel over to the iteration tail
    Broker<BlockingBackChannel> broker = BlockingBackChannelBroker.instance();
    broker.handIn(identifier(), backChannel);

    return backChannel;
  }

  private SuperstepBarrier initSuperstepBarrier() {
    SuperstepBarrier barrier = new SuperstepBarrier();

    getSyncOutput().subscribeToEvent(barrier, AllWorkersDoneEvent.class);
    getSyncOutput().subscribeToEvent(barrier, TerminationEvent.class);

    return barrier;
  }

  //TODO should positions be configured?

  private AbstractRecordWriter<?> getSyncOutput() {
    return eventualOutputs.get(eventualOutputs.size() - 2);
  }

  private AbstractRecordWriter<?> getFinalOutput() {
    return eventualOutputs.get(eventualOutputs.size() - 1);
  }

  private int getIterationInputIndex() {
    return 0;
  }

  @Override
  public void invoke() throws Exception {

    /** used for receiving the current iteration result from iteration tail */
    BlockingBackChannel backChannel = initBackChannel();

    SuperstepBarrier barrier = initSuperstepBarrier();

    TypeSerializer serializer = getInputSerializer(getIterationInputIndex());
    //TODO type safety
    output = (Collector<OT>) iterationCollector();

    while (!terminationRequested()) {

      if (log.isInfoEnabled()) {
        log.info(formatLogString("starting iteration [" + currentIteration() + "]"));
      }

      if (!inFirstIteration()) {
        reinstantiateDriver();
      }

      barrier.setup();

      super.invoke();

      EndOfSuperstepEvent endOfSuperstepEvent = new EndOfSuperstepEvent();

      // signal to connected tasks that we are done with the superstep
      sendEventToAllIterationOutputs(endOfSuperstepEvent);

      // blocking call to wait for the result
      DataInputView superStepResult = backChannel.getReadEndAfterSuperstepEnded();
      if (log.isInfoEnabled()) {
        log.info(formatLogString("finishing iteration [" + currentIteration() + "]"));
      }

      sendEventToSync(endOfSuperstepEvent);

      if (log.isInfoEnabled()) {
        log.info(formatLogString("waiting for other workers in iteration [" + currentIteration() + "]"));
      }

      barrier.waitForOtherWorkers();

      if (barrier.terminationSignaled()) {
        if (log.isInfoEnabled()) {
          log.info(formatLogString("head received termination request in iteration [" + currentIteration() + "]"));
        }
        requestTermination();
        sendEventToAllIterationOutputs(new TerminationEvent());
      } else {
        feedBackSuperstepResult(superStepResult, serializer);
        incrementIterationCounter();
      }
    }

    if (log.isInfoEnabled()) {
      log.info(formatLogString("streaming out final result after [" + currentIteration() + "] iterations"));
    }
    streamOutFinalOutput();
  }

  // send output to all but the last two connected task while iterating
  private Collector<PactRecord> iterationCollector() {
    int numOutputs = eventualOutputs.size();
    Preconditions.checkState(numOutputs > 2);
    List<AbstractRecordWriter<PactRecord>> writers = Lists.newArrayListWithCapacity(numOutputs - 1);
    //TODO remove implicit assumption
    for (int outputIndex = 0; outputIndex < numOutputs - 2; outputIndex++) {
      //TODO type safety
      writers.add((AbstractRecordWriter<PactRecord>) eventualOutputs.get(outputIndex));
    }
    return new PactRecordOutputCollector(writers);
  }

  //TODO we can avoid this if we can detect that we are in the last iteration
  private void streamOutFinalOutput() throws IOException, InterruptedException {
    //TODO type safety
    Writer<PactRecord> writer = (Writer<PactRecord>) getFinalOutput();
    MutableObjectIterator<PactRecord> results = (MutableObjectIterator<PactRecord>) inputs[getIterationInputIndex()];

    int recordsPut = 0;
    PactRecord record = new PactRecord();
    while (results.next(record)) {
      writer.emit(record);
      recordsPut++;
    }
    System.out.println("Records to final out: " + recordsPut);
  }

  private void feedBackSuperstepResult(DataInputView superStepResult, TypeSerializer serializer) {
    inputs[getIterationInputIndex()] = new InputViewIterator(superStepResult, serializer);
  }

  private void sendEventToAllIterationOutputs(AbstractTaskEvent event) throws IOException, InterruptedException {
    if (log.isInfoEnabled()) {
      log.info(formatLogString("sending " + event.getClass().getSimpleName() + " to all iteration outputs"));
    }
    //TODO remove implicit assumption
    for (int outputIndex = 0; outputIndex < eventualOutputs.size() - 2; outputIndex++) {
      flushAndPublishEvent(eventualOutputs.get(outputIndex), event);
    }
  }

  private void sendEventToSync(AbstractTaskEvent event) throws IOException, InterruptedException {
    if (log.isInfoEnabled()) {
      log.info(formatLogString("sending " + event.getClass().getSimpleName() + " to sync"));
    }
    flushAndPublishEvent(getSyncOutput(), event);
  }

}
