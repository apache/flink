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
import eu.stratosphere.nephele.io.MutableReader;
import eu.stratosphere.nephele.services.memorymanager.ListMemorySegmentSource;
import eu.stratosphere.nephele.services.memorymanager.MemoryAllocationException;
import eu.stratosphere.nephele.services.memorymanager.MemorySegment;
import eu.stratosphere.nephele.types.Record;
import eu.stratosphere.pact.common.generic.types.TypeSerializer;
import eu.stratosphere.pact.common.generic.types.TypeSerializerFactory;
import eu.stratosphere.pact.common.stubs.Stub;
import eu.stratosphere.pact.common.util.InstantiationUtil;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.runtime.io.SpillingBuffer;
import eu.stratosphere.pact.runtime.iterative.event.EndOfSuperstepEvent;
import eu.stratosphere.pact.runtime.iterative.event.TerminationEvent;
import eu.stratosphere.pact.runtime.iterative.io.CachingMutableObjectIterator;
import eu.stratosphere.pact.runtime.iterative.io.InterruptingMutableObjectIterator;
import eu.stratosphere.pact.runtime.plugable.PactRecordSerializerFactory;
import eu.stratosphere.pact.runtime.task.PactDriver;
import eu.stratosphere.pact.runtime.task.RegularPactTask;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/** base class for all tasks able to participate in an iteration */
public abstract class AbstractIterativePactTask<S extends Stub, OT> extends RegularPactTask<S, OT>
    implements Terminable {

  private MutableObjectIterator<?>[] wrappedInputs;

  private AtomicBoolean terminated = new AtomicBoolean(false);

  private static final Log log = LogFactory.getLog(AbstractIterativePactTask.class);

  protected String identifier() {
    return getEnvironment().getJobID() + "#" + getEnvironment().getIndexInSubtaskGroup();
  }

  protected void reinstantiateDriver() {
    Class<? extends PactDriver<S, OT>> driverClass = config.getDriver();
    driver = InstantiationUtil.instantiate(driverClass, PactDriver.class);
  }

  private SpillingBuffer reserveMemoryForCaching(double fraction) {
    //TODO ok to steal memory from match tasks, etc?
    long completeMemorySize = getTaskConfig().getMemorySize();
    long cacheMemorySize = (long) (completeMemorySize * fraction);
    getTaskConfig().setMemorySize(completeMemorySize - cacheMemorySize);

    try {
      List<MemorySegment> memory = getMemoryManager().allocatePages(getOwningNepheleTask(), cacheMemorySize);
      return new SpillingBuffer(getIOManager(), new ListMemorySegmentSource(memory), getMemoryManager().getPageSize());
    } catch (MemoryAllocationException e) {
      throw new IllegalStateException("Unable to allocate memory for input caching", e);
    }
  }

  @Override
  public boolean isTerminated() {
    return terminated.get();
  }

  @Override
  public void terminate() {
    if (log.isInfoEnabled()) {
      log.info(formatLogString("marked as terminated."));
    }
    terminated.set(true);
  }

  @Override
  protected void initInputs() throws Exception {
    super.initInputs();
    wrappedInputs = new MutableObjectIterator<?>[getEnvironment().getNumberOfInputGates()];
  }

  @Override
  public <X> MutableObjectIterator<X> getInput(int inputGateIndex) {

    if (wrappedInputs[inputGateIndex] != null) {

      if (getTaskConfig().isCachedInputGate(inputGateIndex)) {
        CachingMutableObjectIterator<X> cachingInput = (CachingMutableObjectIterator<X>) wrappedInputs[inputGateIndex];
        try {
          cachingInput.enableReading();
        } catch (IOException e) {
          throw new IllegalStateException("Unable to enable reading on cached input [" + inputGateIndex + "]");
        }
      }

      return (MutableObjectIterator<X>) wrappedInputs[inputGateIndex];
    }

    String name = getEnvironment().getTaskName() + " (" + (getEnvironment().getIndexInSubtaskGroup() + 1) + '/' +
        getEnvironment().getCurrentNumberOfSubtasks() + ")";

    if (getTaskConfig().isCachedInputGate(inputGateIndex)) {

      if (log.isInfoEnabled()) {
        log.info(formatLogString("wrapping input [" + inputGateIndex + "] with a caching iterator"));
      }

      SpillingBuffer spillingBuffer = reserveMemoryForCaching(getTaskConfig().getInputGateCacheMemoryFraction());
      //TODO type safety
      MutableObjectIterator<X> cachedInput = new CachingMutableObjectIterator<X>((MutableObjectIterator<X>)
          super.getInput(inputGateIndex), spillingBuffer, (TypeSerializer<X>) getInputSerializer(inputGateIndex), name);

      wrappedInputs[inputGateIndex] = cachedInput;

      return cachedInput;
    }

    if (getTaskConfig().isIterativeInputGate(inputGateIndex)) {

      int numberOfEventsUntilInterrupt = getTaskConfig().getNumberOfEventsUntilInterruptInIterativeGate(inputGateIndex);

      //TODO type safety
      InterruptingMutableObjectIterator<X> interruptingIterator = new InterruptingMutableObjectIterator<X>(
          (MutableObjectIterator<X>) super.getInput(inputGateIndex), numberOfEventsUntilInterrupt, name, this);

      MutableReader<Record> inputReader = getReader(inputGateIndex);
      inputReader.subscribeToEvent(interruptingIterator, EndOfSuperstepEvent.class);
      inputReader.subscribeToEvent(interruptingIterator, TerminationEvent.class);

      if (log.isInfoEnabled()) {
        log.info(formatLogString("wrapping input [" + inputGateIndex + "] with an interrupting iterator that waits " +
            "for [" + numberOfEventsUntilInterrupt + "] event(s)"));
      }

      wrappedInputs[inputGateIndex] = interruptingIterator;

      return interruptingIterator;
    }

    return super.getInput(inputGateIndex);
  }

  // TODO check whether flush could be removed
  protected void flushAndPublishEvent(AbstractRecordWriter<?> writer, AbstractTaskEvent event)
      throws IOException, InterruptedException {
    writer.flush();
    writer.publishEvent(event);
    writer.flush();
  }

  //TODO move up to RegularPactTask
  protected TypeSerializer<OT> createOutputTypeSerializer() {
    // get the factory for the serializer
    final Class<? extends TypeSerializerFactory<OT>> serializerFactoryClass;
    try {
      serializerFactoryClass = config.getSerializerFactoryForOutput(userCodeClassLoader);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException("The class registered as output serializer factory could not be loaded.", e);
    }
    final TypeSerializerFactory<OT> serializerFactory;

    if (serializerFactoryClass == null) {
      @SuppressWarnings("unchecked")
      TypeSerializerFactory<OT> pf = (TypeSerializerFactory<OT>) PactRecordSerializerFactory.get();
      serializerFactory = pf;
    } else {
      serializerFactory = InstantiationUtil.instantiate(serializerFactoryClass, TypeSerializerFactory.class);
    }
    return serializerFactory.getSerializer();
  }
}
