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

import eu.stratosphere.nephele.io.MutableReader;
import eu.stratosphere.nephele.types.Record;
import eu.stratosphere.pact.common.generic.types.TypeSerializer;
import eu.stratosphere.pact.common.generic.types.TypeSerializerFactory;
import eu.stratosphere.pact.common.stubs.Stub;
import eu.stratosphere.pact.common.util.InstantiationUtil;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.runtime.iterative.driver.AbstractRepeatableMatchDriver;
import eu.stratosphere.pact.runtime.iterative.event.EndOfSuperstepEvent;
import eu.stratosphere.pact.runtime.iterative.event.TerminationEvent;
import eu.stratosphere.pact.runtime.iterative.io.InterruptingMutableObjectIterator;
import eu.stratosphere.pact.runtime.iterative.monitoring.IterationMonitoring;
import eu.stratosphere.pact.runtime.plugable.PactRecordSerializerFactory;
import eu.stratosphere.pact.runtime.task.PactDriver;
import eu.stratosphere.pact.runtime.task.RegularPactTask;
import eu.stratosphere.pact.runtime.task.util.ReaderInterruptionBehavior;
import eu.stratosphere.pact.runtime.task.util.ReaderInterruptionBehaviors;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.concurrent.atomic.AtomicBoolean;

/** base class for all tasks able to participate in an iteration */
public abstract class AbstractIterativePactTask<S extends Stub, OT> extends RegularPactTask<S, OT>
    implements Terminable {

  private MutableObjectIterator<?>[] wrappedInputs;

  private AtomicBoolean terminationRequested = new AtomicBoolean(false);

  private int numIterations = 1;

  private static final Log log = LogFactory.getLog(AbstractIterativePactTask.class);

  protected boolean inFirstIteration() {
    return numIterations == 1;
  }

  protected boolean isJoinOnConstantDataPath() {
    return driver instanceof AbstractRepeatableMatchDriver;
  }

  protected int currentIteration() {
    return numIterations;
  }

  protected void incrementIterationCounter() {
    numIterations++;
  }

  protected void notifyMonitor(IterationMonitoring.Event event) {
    if (log.isInfoEnabled()) {
      log.info(IterationMonitoring.logLine(getEnvironment().getJobID(), event, currentIteration(),
          getEnvironment().getIndexInSubtaskGroup()));
    }
  }

  protected String brokerKey() {
    return getEnvironment().getJobID() + "#" + getEnvironment().getIndexInSubtaskGroup();
  }

  protected String identifier() {
    return getEnvironment().getTaskName() + " (" + (getEnvironment().getIndexInSubtaskGroup() + 1) + '/' +
        getEnvironment().getCurrentNumberOfSubtasks() + ")";
  }

  protected void reinstantiateDriver() {
    Class<? extends PactDriver<S, OT>> driverClass = config.getDriver();
    driver = InstantiationUtil.instantiate(driverClass, PactDriver.class);
  }

  @Override
  public boolean terminationRequested() {
    return terminationRequested.get();
  }

  @Override
  public void requestTermination() {
    if (log.isInfoEnabled()) {
      log.info(formatLogString("requesting termination."));
    }
    terminationRequested.set(true);
  }

  @Override
  protected void initInputs() throws Exception {
    super.initInputs();
    wrappedInputs = new MutableObjectIterator<?>[getEnvironment().getNumberOfInputGates()];
  }

  @Override
  protected ReaderInterruptionBehavior readerInterruptionBehavior(int inputGateIndex) {
    return getTaskConfig().isIterativeInputGate(inputGateIndex) ?
        ReaderInterruptionBehaviors.RELEASE_ON_INTERRUPT : ReaderInterruptionBehaviors.EXCEPTION_ON_INTERRUPT;
  }

  @Override
  public <X> MutableObjectIterator<X> getInput(int inputGateIndex) {

    if (wrappedInputs[inputGateIndex] != null) {
      return (MutableObjectIterator<X>) wrappedInputs[inputGateIndex];
    }

    if (getTaskConfig().isIterativeInputGate(inputGateIndex)) {
      return wrapWithInterruptingIterator(inputGateIndex);
    }

    return super.getInput(inputGateIndex);
  }

  private <X> MutableObjectIterator<X> wrapWithInterruptingIterator(int inputGateIndex) {
    int numberOfEventsUntilInterrupt = getTaskConfig().getNumberOfEventsUntilInterruptInIterativeGate(inputGateIndex);

    //TODO type safety
    InterruptingMutableObjectIterator<X> interruptingIterator = new InterruptingMutableObjectIterator<X>(
        (MutableObjectIterator<X>) super.getInput(inputGateIndex), numberOfEventsUntilInterrupt, identifier(), this,
        inputGateIndex);

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
