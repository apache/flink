/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.pact.runtime.task;

import eu.stratosphere.api.common.typeutils.TypeComparator;
import eu.stratosphere.api.common.typeutils.TypeSerializerFactory;
import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.template.AbstractInvokable;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;
import eu.stratosphere.util.Collector;
import eu.stratosphere.util.MutableObjectIterator;


/**
 * A runtime task is the task that is executed by the nephele engine inside a task vertex.
 * It typically has a {@link PactDriver}, and optionally multiple chained drivers. In addition, it
 * deals with the runtime setup and teardown and the control-flow logic. The later appears especially
 * in the case of iterations.
 *
 * @param <S> The UDF type.
 * @param <OT> The produced data type.
 *
 * @see PactDriver
 */
public interface PactTaskContext<S, OT> {
	
	TaskConfig getTaskConfig();
	
	ClassLoader getUserCodeClassLoader();
	
	MemoryManager getMemoryManager();
	
	IOManager getIOManager();
	
	<X> MutableObjectIterator<X> getInput(int index);
	
	<X> TypeSerializerFactory<X> getInputSerializer(int index);
	
	<X> TypeComparator<X> getInputComparator(int index);
	
	S getStub();
	
	Collector<OT> getOutputCollector();
	
	AbstractInvokable getOwningNepheleTask();
	
	String formatLogString(String message);
}
