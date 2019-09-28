/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.operators.drivers;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerFactory;
import org.apache.flink.api.java.typeutils.runtime.RuntimeSerializerFactory;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.MemoryType;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.operators.DriverStrategy;
import org.apache.flink.runtime.operators.TaskContext;
import org.apache.flink.runtime.operators.testutils.DummyInvokable;
import org.apache.flink.runtime.operators.util.TaskConfig;
import org.apache.flink.runtime.taskmanager.TaskManagerRuntimeInfo;
import org.apache.flink.runtime.util.TestingTaskManagerRuntimeInfo;
import org.apache.flink.util.Collector;
import org.apache.flink.util.MutableObjectIterator;

public class TestTaskContext<S, T> implements TaskContext<S, T> {
	
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

	private ExecutionConfig executionConfig = new ExecutionConfig();

	private TaskManagerRuntimeInfo taskManageInfo;

	// --------------------------------------------------------------------------------------------
	//  Constructors
	// --------------------------------------------------------------------------------------------
	
	public TestTaskContext() {}
	
	public TestTaskContext(long memoryInBytes) {
		this.memoryManager = new MemoryManager(memoryInBytes, 1, 32 * 1024, MemoryType.HEAP, true);
		this.taskManageInfo = new TestingTaskManagerRuntimeInfo();
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
		this.serializer1 = new RuntimeSerializerFactory<X>(serializer, (Class<X>) serializer.createInstance().getClass());
	}

	@SuppressWarnings("unchecked")
	public <X> void setInput2(MutableObjectIterator<X> input, TypeSerializer<X> serializer) {
		this.input2 = input;
		this.serializer2 = new RuntimeSerializerFactory<X>(serializer, (Class<X>) serializer.createInstance().getClass());
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
	
	public void setMutableObjectMode(boolean mutableObjectMode) {
		this.config.setMutableObjectMode(mutableObjectMode);
	}
	
	// --------------------------------------------------------------------------------------------
	//  Context Methods
	// --------------------------------------------------------------------------------------------
	
	@Override
	public TaskConfig getTaskConfig() {
		return this.config;
	}

	@Override
	public ExecutionConfig getExecutionConfig() {
		return executionConfig;
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
	public TaskManagerRuntimeInfo getTaskManagerInfo() {
		return this.taskManageInfo;
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
	public <X> TypeComparator<X> getDriverComparator(int index) {
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
	public AbstractInvokable getContainingTask() {
		return this.owner;
	}

	@Override
	public String formatLogString(String message) {
		return message;
	}

	@Override
	public OperatorMetricGroup getMetricGroup() {
		return UnregisteredMetricGroups.createUnregisteredOperatorMetricGroup();
	}
}
