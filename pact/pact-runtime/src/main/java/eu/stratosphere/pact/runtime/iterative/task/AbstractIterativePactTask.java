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
import eu.stratosphere.nephele.event.task.EventListener;
import eu.stratosphere.nephele.io.AbstractRecordWriter;
import eu.stratosphere.pact.common.generic.types.TypeSerializer;
import eu.stratosphere.pact.common.generic.types.TypeSerializerFactory;
import eu.stratosphere.pact.common.stubs.Stub;
import eu.stratosphere.pact.common.util.InstantiationUtil;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.runtime.iterative.event.Callback;
import eu.stratosphere.pact.runtime.iterative.event.EndOfSuperstepEvent;
import eu.stratosphere.pact.runtime.iterative.event.TerminationEvent;
import eu.stratosphere.pact.runtime.iterative.io.InterruptingMutableObjectIterator;
import eu.stratosphere.pact.runtime.plugable.PactRecordSerializerFactory;
import eu.stratosphere.pact.runtime.task.PactDriver;
import eu.stratosphere.pact.runtime.task.RegularPactTask;

import java.io.IOException;

/** base class for all tasks able to participate in an iteration */
public abstract class AbstractIterativePactTask<S extends Stub, OT> extends RegularPactTask<S, OT> {

  protected String identifier() {
    return getEnvironment().getJobID() + "#" + getEnvironment().getIndexInSubtaskGroup();
  }

  protected void reinstantiateDriver() {
    Class<? extends PactDriver<S, OT>> driverClass = config.getDriver();
    driver = InstantiationUtil.instantiate(driverClass, PactDriver.class);
  }

  @Override
  public <X> MutableObjectIterator<X> getInput(int index) {

    String owner = getEnvironment().getTaskName() + " (" + (getEnvironment().getIndexInSubtaskGroup() + 1) + '/' +
        getEnvironment().getCurrentNumberOfSubtasks() + ")";
    //TODO check whether this is an iteration input!!!
    //TODO type safety
    InterruptingMutableObjectIterator<X> interruptingIterator = new InterruptingMutableObjectIterator<X>(
        (MutableObjectIterator<X>) super.getInput(index), numberOfEventsUntilInterrupt(), owner);

    //TODO might not work for unioned inputs
    getEnvironment().getInputGate(index).subscribeToEvent(interruptingIterator, EndOfSuperstepEvent.class);

    return interruptingIterator;
  }

  protected int numberOfEventsUntilInterrupt() {
    //TODO return number of iteration inputs
    return 1;
  }

  protected void listenToTermination(Callback<TerminationEvent> callback) {
    listenToEvent(TerminationEvent.class, callback);
  }

  protected void listenToEndOfSuperstep(Callback<EndOfSuperstepEvent> callback) {
    listenToEvent(EndOfSuperstepEvent.class, callback);
  }

  private <E extends AbstractTaskEvent> void listenToEvent(Class<E> eventClass, final Callback<E> callback) {
    //TODO use correct input gate
    getEnvironment().getInputGate(0).subscribeToEvent(new EventListener() {
      @Override
      public void eventOccurred(AbstractTaskEvent event) {
        try {
          callback.execute((E) event);
        } catch (Exception e) {
          //TODO do something meaningful here
          e.printStackTrace(System.out);
        }
      }
    }, eventClass);
  }

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
