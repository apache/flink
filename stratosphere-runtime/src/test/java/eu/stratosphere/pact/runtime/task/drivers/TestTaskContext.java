/***********************************************************************************************************************
 *
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
 *
 **********************************************************************************************************************/

package eu.stratosphere.pact.runtime.task.drivers;

import eu.stratosphere.api.common.typeutils.TypeComparator;
import eu.stratosphere.api.common.typeutils.TypeSerializer;
import eu.stratosphere.api.common.typeutils.TypeSerializerFactory;
import eu.stratosphere.api.java.typeutils.runtime.RuntimeStatefulSerializerFactory;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.services.memorymanager.spi.DefaultMemoryManager;
import eu.stratosphere.nephele.template.AbstractInvokable;
import eu.stratosphere.pact.runtime.task.DriverStrategy;
import eu.stratosphere.pact.runtime.task.PactTaskContext;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;
import eu.stratosphere.pact.runtime.test.util.DummyInvokable;
import eu.stratosphere.util.Collector;
import eu.stratosphere.util.MutableObjectIterator;

public class TestTaskContext<S, T> implements PactTaskContext<S, T> {
	
	private final AbstractInvokable owner = new DummyInvokable();
	
	private MutableObjectIterator<?> input1;
	
	private MutableObjectIterator<?> input2;
	
	private TypeSerializerFactory<?> serializer1;
	
	private TypeSerializerFactory<?> serializer2;
	
	private TypeComparator<?> comparator1;
	
	private TypeComparator<?> comparator2;
	
	private TaskConfig config = new TaskConfig(new Configuration());
	
	private S udf;
	
	private Collector<T> outputCollector;
	
	private MemoryManager memoryManager;

	// --------------------------------------------------------------------------------------------
	//  Constructors
	// --------------------------------------------------------------------------------------------
	
	public TestTaskContext() {}
	
	public TestTaskContext(long memoryInBytes) {
		this.memoryManager = new DefaultMemoryManager(memoryInBytes, 32 * 1024);
	}
	
	// --------------------------------------------------------------------------------------------
	//  Setters
	// --------------------------------------------------------------------------------------------
	
	public <X> void setInput1(MutableObjectIterator<X> input, TypeSerializerFactory<X> serializer) {
		this.input1 = input;
		this.serializer1 = serializer;
	}

	public <X> void setInput2(MutableObjectIterator<X> input, TypeSerializerFactory<X> serializer) {
		this.input2 = input;
		this.serializer2 = serializer;
	}
	
	@SuppressWarnings("unchecked")
	public <X> void setInput1(MutableObjectIterator<X> input, TypeSerializer<X> serializer) {
		this.input1 = input;
		this.serializer1 = new RuntimeStatefulSerializerFactory<X>(serializer, (Class<X>) serializer.createInstance().getClass());
	}

	@SuppressWarnings("unchecked")
	public <X> void setInput2(MutableObjectIterator<X> input, TypeSerializer<X> serializer) {
		this.input2 = input;
		this.serializer2 = new RuntimeStatefulSerializerFactory<X>(serializer, (Class<X>) serializer.createInstance().getClass());
	}
	
	public void setComparator1(TypeComparator<?> comparator) {
		this.comparator1 = comparator;
	}

	public void setComparator2(TypeComparator<?> comparator) {
		this.comparator2 = comparator;
	}
	
	public void setConfig(TaskConfig config) {
		this.config = config;
	}
	
	public void setUdf(S udf) {
		this.udf = udf;
	}
	
	public void setCollector(Collector<T> collector) {
		this.outputCollector = collector;
	}
	
	public void setDriverStrategy(DriverStrategy strategy) {
		this.config.setDriverStrategy(strategy);
	}
	
	// --------------------------------------------------------------------------------------------
	//  Context Methods
	// --------------------------------------------------------------------------------------------
	
	@Override
	public TaskConfig getTaskConfig() {
		return this.config;
	}

	@Override
	public ClassLoader getUserCodeClassLoader() {
		return getClass().getClassLoader();
	}

	@Override
	public MemoryManager getMemoryManager() {
		return this.memoryManager;
	}

	@Override
	public IOManager getIOManager() {
		return null;
	}

	@Override
	@SuppressWarnings("unchecked")
	public <X> MutableObjectIterator<X> getInput(int index) {
		switch (index) {
		case 0:
			return (MutableObjectIterator<X>) this.input1;
		case 1:
			return (MutableObjectIterator<X>) this.input2;
		default:
			throw new RuntimeException();
		}
	}

	@Override
	@SuppressWarnings("unchecked")
	public <X> TypeSerializerFactory<X> getInputSerializer(int index) {
		switch (index) {
		case 0:
			return (TypeSerializerFactory<X>) this.serializer1;
		case 1:
			return (TypeSerializerFactory<X>) this.serializer2;
		default:
			throw new RuntimeException();
		}
	}

	@Override
	@SuppressWarnings("unchecked")
	public <X> TypeComparator<X> getInputComparator(int index) {
		switch (index) {
		case 0:
			return (TypeComparator<X>) this.comparator1;
		case 1:
			return (TypeComparator<X>) this.comparator2;
		default:
			throw new RuntimeException();
		}
	}

	@Override
	public S getStub() {
		return this.udf;
	}

	@Override
	public Collector<T> getOutputCollector() {
		return this.outputCollector;
	}

	@Override
	public AbstractInvokable getOwningNepheleTask() {
		return this.owner;
	}

	@Override
	public String formatLogString(String message) {
		return message;
	}
}
