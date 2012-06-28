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
import eu.stratosphere.pact.common.generic.types.TypeSerializer;
import eu.stratosphere.pact.common.generic.types.TypeSerializerFactory;
import eu.stratosphere.pact.common.stubs.Stub;
import eu.stratosphere.pact.common.util.InstantiationUtil;
import eu.stratosphere.pact.runtime.iterative.event.Callback;
import eu.stratosphere.pact.runtime.iterative.event.EndOfSuperstepEvent;
import eu.stratosphere.pact.runtime.plugable.PactRecordSerializerFactory;
import eu.stratosphere.pact.runtime.task.PactDriver;
import eu.stratosphere.pact.runtime.task.RegularPactTask;

import java.io.IOException;

public abstract class IterativePactTask<S extends Stub, OT> extends RegularPactTask<S, OT> {

  protected String identifier() {
    return getEnvironment().getJobID() + "#" + getEnvironment().getIndexInSubtaskGroup();
  }

  protected void reinstantiateDriver() {
    Class<? extends PactDriver<S, OT>> driverClass = config.getDriver();
    driver = InstantiationUtil.instantiate(driverClass, PactDriver.class);
  }

  protected void listenToEndOfSuperstep(final Callback callback) {
    //TODO use correct input gate
    getEnvironment().getInputGate(0).subscribeToEvent(new EventListener() {
      @Override
      public void eventOccurred(AbstractTaskEvent event) {
        callback.execute();
      }
    }, EndOfSuperstepEvent.class);
  }

  protected void signalEndOfSuperStep() throws IOException, InterruptedException {
    //TODO remove implicit assumption
    // signal to connected tasks that we are done with the superstep
    for (int outputIndex = 0; outputIndex < eventualOutputs.size() - 1; outputIndex++) {
      eventualOutputs.get(outputIndex).publishEvent(new EndOfSuperstepEvent());
    }
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
