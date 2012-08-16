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
import eu.stratosphere.pact.common.generic.types.TypeComparator;
import eu.stratosphere.pact.common.generic.types.TypePairComparatorFactory;
import eu.stratosphere.pact.common.generic.types.TypeSerializer;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.Stub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.common.util.InstantiationUtil;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.runtime.hash.MutableHashTable;
import eu.stratosphere.pact.runtime.io.InputViewIterator;
import eu.stratosphere.pact.runtime.iterative.concurrent.BlockingBackChannel;
import eu.stratosphere.pact.runtime.iterative.concurrent.BlockingBackChannelBroker;
import eu.stratosphere.pact.runtime.iterative.concurrent.Broker;
import eu.stratosphere.pact.runtime.iterative.concurrent.SolutionSetBroker;
import eu.stratosphere.pact.runtime.iterative.concurrent.SuperstepBarrier;
import eu.stratosphere.pact.runtime.iterative.event.AllWorkersDoneEvent;
import eu.stratosphere.pact.runtime.iterative.event.EndOfSuperstepEvent;
import eu.stratosphere.pact.runtime.iterative.event.TerminationEvent;
import eu.stratosphere.pact.runtime.iterative.io.SerializedUpdateBuffer;
import eu.stratosphere.pact.runtime.plugable.PactRecordComparator;
import eu.stratosphere.pact.runtime.plugable.PactRecordPairComparatorFactory;
import eu.stratosphere.pact.runtime.plugable.PactRecordSerializer;
import eu.stratosphere.pact.runtime.shipping.PactRecordOutputCollector;
import eu.stratosphere.pact.runtime.task.PactTaskContext;
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

    // compute the size of the memory available to the backchannel
    long completeMemorySize = config.getMemorySize();
    long backChannelMemorySize = (long) (completeMemorySize * config.getBackChannelMemoryFraction());
    config.setMemorySize(completeMemorySize - backChannelMemorySize);

    // allocate the memory available to the backchannel
    List<MemorySegment> segments = Lists.newArrayList();
    int segmentSize = getMemoryManager().getPageSize();
    getMemoryManager().allocatePages(this, segments, backChannelMemorySize);

    // instantiate the backchannel
    BlockingBackChannel backChannel = new BlockingBackChannel(new SerializedUpdateBuffer(segments, segmentSize,
        getIOManager()));

    // hand the backchannel over to the iteration tail
    Broker<BlockingBackChannel> broker = BlockingBackChannelBroker.instance();
    broker.handIn(brokerKey(), backChannel);

    return backChannel;
  }

  //TODO type safety
  private <IT1, IT2> MutableHashTable initHashJoin() throws Exception {

    /* steal some memory */
    long completeMemorySize = config.getMemorySize();
    //TODO make configurable
    long backChannelMemorySize = (long) (completeMemorySize * 0.5);
    config.setMemorySize(completeMemorySize - backChannelMemorySize);

    final List<MemorySegment> memorySegments = getMemoryManager().allocatePages(getOwningNepheleTask(),
        config.getMemorySize());

    //TODO make configurable
    final TypeSerializer<IT1> serializer1 = (TypeSerializer<IT1>) PactRecordSerializer.get();
    final TypeSerializer<IT2> serializer2 = (TypeSerializer<IT2>) PactRecordSerializer.get();
    final TypeComparator<IT1> comparator1 = (TypeComparator<IT1>) new PactRecordComparator(new int[] { 0 },
        new Class[] { PactLong.class });
    final TypeComparator<IT2> comparator2 = (TypeComparator<IT2>) new PactRecordComparator(new int[] { 0 },
        new Class[] { PactLong.class });

    final TypePairComparatorFactory<IT1, IT2> pairComparatorFactory;
    try {
      final Class<? extends TypePairComparatorFactory<IT1, IT2>> factoryClass =
          config.getPairComparatorFactory(getUserCodeClassLoader());

      if (factoryClass == null) {
        @SuppressWarnings("unchecked")
        TypePairComparatorFactory<IT1, IT2> pactRecordFactory =
            (TypePairComparatorFactory<IT1, IT2>) PactRecordPairComparatorFactory.get();
        pairComparatorFactory = pactRecordFactory;
      } else {
        @SuppressWarnings("unchecked")
        final Class<TypePairComparatorFactory<IT1, IT2>> clazz =
            (Class<TypePairComparatorFactory<IT1, IT2>>) (Class<?>) TypePairComparatorFactory.class;
        pairComparatorFactory = InstantiationUtil.instantiate(factoryClass, clazz);
      }
    } catch (ClassNotFoundException e) {
      throw new Exception("The class registered as TypePairComparatorFactory cloud not be loaded.", e);
    }

    MutableHashTable hashJoin = new MutableHashTable(serializer2, serializer1, comparator2, comparator1,
        pairComparatorFactory.createComparator12(comparator1, comparator2), memorySegments, getIOManager());

    Broker<MutableHashTable> solutionsetBroker = SolutionSetBroker.instance();

    System.out.println("HANDING IN JOIN: " + brokerKey());
    solutionsetBroker.handIn(brokerKey(), hashJoin);

    return hashJoin;
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

  //TODO type safety
  @Override
  public void invoke() throws Exception {

    /** used for receiving the current iteration result from iteration tail */
    BlockingBackChannel backChannel = initBackChannel();

    SuperstepBarrier barrier = initSuperstepBarrier();

    MutableHashTable hashJoin = config.usesWorkset() ? initHashJoin() : null;

    TypeSerializer serializer = getInputSerializer(getIterationInputIndex());
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
      DataInputView superstepResult = backChannel.getReadEndAfterSuperstepEnded();
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
        incrementIterationCounter();
      }

      feedBackSuperstepResult(superstepResult, serializer);
    }

    if (log.isInfoEnabled()) {
      log.info(formatLogString("streaming out final result after [" + currentIteration() + "] iterations"));
    }

    if (config.usesWorkset()) {
      streamOutFinalOutputWorkset(hashJoin);
    } else {
      streamOutFinalOutputBulk();
    }
  }

  // send output to all but the last two connected tasks while iterating
  private Collector<PactRecord> iterationCollector() {
    int numOutputs = eventualOutputs.size();
    Preconditions.checkState(numOutputs > 2);
    List<AbstractRecordWriter<PactRecord>> writers = Lists.newArrayListWithCapacity(numOutputs - 2);
    //TODO remove implicit assumption
    for (int outputIndex = 0; outputIndex < numOutputs - 2; outputIndex++) {
      //TODO type safety
      writers.add((AbstractRecordWriter<PactRecord>) eventualOutputs.get(outputIndex));
    }
    return new PactRecordOutputCollector(writers);
  }

  private void streamOutFinalOutputBulk() throws IOException, InterruptedException {
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

  private void streamOutFinalOutputWorkset(MutableHashTable hashJoin) throws IOException, InterruptedException {
    //TODO type safety
    Writer<PactRecord> writer = (Writer<PactRecord>) getFinalOutput();

    //TODO implement an iterator over MutableHashTable.partitionsBeingBuilt
    MutableObjectIterator<PactRecord> results = hashJoin.getPartitionEntryIterator();

    int recordsPut = 0;
    PactRecord record = new PactRecord();
    while (results.next(record)) {
      writer.emit(record);
      recordsPut++;
    }

    hashJoin.close();

    System.out.println("Records to final out (workset): " + recordsPut);
  }

  private void feedBackSuperstepResult(DataInputView superstepResult, TypeSerializer serializer) {
    inputs[getIterationInputIndex()] = new InputViewIterator(superstepResult, serializer);
  }

  private void sendEventToAllIterationOutputs(AbstractTaskEvent event) throws IOException, InterruptedException {
    if (log.isInfoEnabled()) {
      log.info(formatLogString("sending " + event.getClass().getSimpleName() + " to all iteration outputs"));
    }
    //TODO remove implicit assumption
    for (int outputIndex = 0; outputIndex < eventualOutputs.size() - 2; outputIndex++) {
      eventualOutputs.get(outputIndex).publishEvent(event);
    }
  }

  private void sendEventToSync(AbstractTaskEvent event) throws IOException, InterruptedException {
    if (log.isInfoEnabled()) {
      log.info(formatLogString("sending " + event.getClass().getSimpleName() + " to sync"));
    }
    getSyncOutput().publishEvent(event);
  }

}
