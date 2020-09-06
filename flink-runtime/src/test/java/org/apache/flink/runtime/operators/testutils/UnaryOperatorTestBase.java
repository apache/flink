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

package org.apache.flink.runtime.operators.testutils;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerFactory;
import org.apache.flink.api.java.typeutils.runtime.RuntimeSerializerFactory;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.memory.MemoryManagerBuilder;
import org.apache.flink.runtime.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.operators.Driver;
import org.apache.flink.runtime.operators.TaskContext;
import org.apache.flink.runtime.operators.ResettableDriver;
import org.apache.flink.runtime.operators.sort.UnilateralSortMerger;
import org.apache.flink.runtime.operators.util.TaskConfig;
import org.apache.flink.runtime.taskmanager.TaskManagerRuntimeInfo;
import org.apache.flink.runtime.util.TestingTaskManagerRuntimeInfo;
import org.apache.flink.util.Collector;
import org.apache.flink.util.MutableObjectIterator;

import org.apache.flink.util.TestLogger;
import org.junit.After;
import org.junit.Assert;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

@RunWith(Parameterized.class)
public abstract class UnaryOperatorTestBase<S extends Function, IN, OUT> extends TestLogger implements TaskContext<S, OUT> {
	
	protected static final long DEFAULT_PER_SORT_MEM = 16 * 1024 * 1024;
	
	protected static final int PAGE_SIZE = 32 * 1024;

	private final TaskManagerRuntimeInfo taskManageInfo;
	
	private final IOManager ioManager;
	
	private final MemoryManager memManager;
	
	private MutableObjectIterator<IN> input;
	
	private TypeSerializer<IN> inputSerializer;
	
	private List<TypeComparator<IN>> comparators;
	
	private UnilateralSortMerger<IN> sorter;
	
	private final AbstractInvokable owner;

	private final TaskConfig taskConfig;
	
	protected final long perSortMem;

	protected final double perSortFractionMem;
	
	private Collector<OUT> output;
	
	protected int numFileHandles;
	
	private S stub;
	
	private Driver<S, OUT> driver;
	
	private volatile boolean running;

	private ExecutionConfig executionConfig;
	
	protected UnaryOperatorTestBase(ExecutionConfig executionConfig, long memory, int maxNumSorters) {
		this(executionConfig, memory, maxNumSorters, DEFAULT_PER_SORT_MEM);
	}
	
	protected UnaryOperatorTestBase(ExecutionConfig executionConfig, long memory, int maxNumSorters, long perSortMemory) {
		if (memory < 0 || maxNumSorters < 0 || perSortMemory < 0) {
			throw new IllegalArgumentException();
		}
		
		final long totalMem = Math.max(memory, 0) + (Math.max(maxNumSorters, 0) * perSortMemory);
		
		this.perSortMem = perSortMemory;
		this.perSortFractionMem = (double)perSortMemory/totalMem;
		this.ioManager = new IOManagerAsync();
		this.memManager = totalMem > 0 ? MemoryManagerBuilder.newBuilder().setMemorySize(totalMem).build() : null;
		this.owner = new DummyInvokable();

		Configuration config = new Configuration();
		this.taskConfig = new TaskConfig(config);

		this.executionConfig = executionConfig;
		this.comparators = new ArrayList<TypeComparator<IN>>(2);

		this.taskManageInfo = new TestingTaskManagerRuntimeInfo();
	}

	@Parameterized.Parameters
	public static Collection<Object[]> getConfigurations() {
		ExecutionConfig withReuse = new ExecutionConfig();
		withReuse.enableObjectReuse();

		ExecutionConfig withoutReuse = new ExecutionConfig();
		withoutReuse.disableObjectReuse();

		Object[] a = { withoutReuse };
		Object[] b = { withReuse };
		return Arrays.asList(a, b);
	}

	public void setInput(MutableObjectIterator<IN> input, TypeSerializer<IN> serializer) {
		this.input = input;
		this.inputSerializer = serializer;
		this.sorter = null;
	}
	
	public void addInputSorted(MutableObjectIterator<IN> input,
								TypeSerializer<IN> serializer,
								TypeComparator<IN> comp) throws Exception
	{
		this.input = null;
		this.inputSerializer = serializer;
		this.sorter = new UnilateralSortMerger<IN>(
				this.memManager, this.ioManager, input, this.owner,
				this.<IN>getInputSerializer(0),
				comp,
				this.perSortFractionMem, 32, 0.8f,
				true /*use large record handler*/,
				false);
	}
	
	public void addDriverComparator(TypeComparator<IN> comparator) {
		this.comparators.add(comparator);
	}

	public void setOutput(Collector<OUT> output) {
		this.output = output;
	}
	public void setOutput(List<OUT> output, TypeSerializer<OUT> outSerializer) {
		this.output = new ListOutputCollector<OUT>(output, outSerializer);
	}
	
	public int getNumFileHandlesForSort() {
		return numFileHandles;
	}

	
	public void setNumFileHandlesForSort(int numFileHandles) {
		this.numFileHandles = numFileHandles;
	}

	@SuppressWarnings("rawtypes")
	public void testDriver(Driver driver, Class stubClass) throws Exception {
		testDriverInternal(driver, stubClass);
	}

	@SuppressWarnings({"unchecked","rawtypes"})
	public void testDriverInternal(Driver driver, Class stubClass) throws Exception {

		this.driver = driver;
		driver.setup(this);

		this.stub = (S)stubClass.newInstance();

		// regular running logic
		this.running = true;
		boolean stubOpen = false;

		try {
			// run the data preparation
			try {
				driver.prepare();
			}
			catch (Throwable t) {
				throw new Exception("The data preparation caused an error: " + t.getMessage(), t);
			}

			// open stub implementation
			try {
				FunctionUtils.openFunction(this.stub, getTaskConfig().getStubParameters());
				stubOpen = true;
			}
			catch (Throwable t) {
				throw new Exception("The user defined 'open()' method caused an exception: " + t.getMessage(), t);
			}

			// run the user code
			driver.run();

			// close. We close here such that a regular close throwing an exception marks a task as failed.
			if (this.running) {
				FunctionUtils.closeFunction (this.stub);
				stubOpen = false;
			}

			this.output.close();
		}
		catch (Exception ex) {
			// close the input, but do not report any exceptions, since we already have another root cause
			if (stubOpen) {
				try {
					FunctionUtils.closeFunction(this.stub);
				}
				catch (Throwable t) {
					// ignore
				}
			}

			// if resettable driver invoke tear-down
			if (this.driver instanceof ResettableDriver) {
				final ResettableDriver<?, ?> resDriver = (ResettableDriver<?, ?>) this.driver;
				try {
					resDriver.teardown();
				} catch (Throwable t) {
					throw new Exception("Error while shutting down an iterative operator: " + t.getMessage(), t);
				}
			}

			// drop exception, if the task was canceled
			if (this.running) {
				throw ex;
			}

		}
		finally {
			driver.cleanup();
		}
	}

	@SuppressWarnings({"unchecked","rawtypes"})
	public void testResettableDriver(ResettableDriver driver, Class stubClass, int iterations) throws Exception {
		driver.setup(this);
		
		for (int i = 0; i < iterations; i++) {
			if (i == 0) {
				driver.initialize();
			}
			else {
				driver.reset();
			}
			testDriver(driver, stubClass);
		}
		
		driver.teardown();
	}
	
	public void cancel() throws Exception {
		this.running = false;
		this.driver.cancel();
	}

	// --------------------------------------------------------------------------------------------

	@Override
	public TaskConfig getTaskConfig() {
		return this.taskConfig;
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
	public IOManager getIOManager() {
		return this.ioManager;
	}
	
	@Override
	public MemoryManager getMemoryManager() {
		return this.memManager;
	}

	@Override
	public TaskManagerRuntimeInfo getTaskManagerInfo() {
		return this.taskManageInfo;
	}
	
	@Override
	public <X> MutableObjectIterator<X> getInput(int index) {
		MutableObjectIterator<IN> in = this.input;
		if (in == null) {
			// waiting from sorter
			try {
				in = this.sorter.getIterator();
			}
			catch (InterruptedException e) {
				throw new RuntimeException("Interrupted");
			}
			this.input = in;
		}
		
		@SuppressWarnings("unchecked")
		MutableObjectIterator<X> input = (MutableObjectIterator<X>) this.input;
		return input;
	}

	@Override
	public <X> TypeSerializerFactory<X> getInputSerializer(int index) {
		if (index != 0) {
			throw new IllegalArgumentException();
		}
		
		@SuppressWarnings("unchecked")
		TypeSerializer<X> ser = (TypeSerializer<X>) inputSerializer;
		return new RuntimeSerializerFactory<X>(ser, (Class<X>) ser.createInstance().getClass());
	}

	@Override
	public <X> TypeComparator<X> getDriverComparator(int index) {
		@SuppressWarnings("unchecked")
		TypeComparator<X> comparator = (TypeComparator<X>) this.comparators.get(index);
		return comparator;
	}

	@Override
	public S getStub() {
		return this.stub;
	}

	@Override
	public Collector<OUT> getOutputCollector() {
		return this.output;
	}

	@Override
	public AbstractInvokable getContainingTask() {
		return this.owner;
	}

	@Override
	public String formatLogString(String message) {
		return "Driver Tester: " + message;
	}
	
	@Override
	public OperatorMetricGroup getMetricGroup() {
		return UnregisteredMetricGroups.createUnregisteredOperatorMetricGroup();
	}

	// --------------------------------------------------------------------------------------------
	
	@After
	public void shutdownAll() throws Exception {
		// 1st, shutdown sorters
		if (this.sorter != null) {
			sorter.close();
		}
		
		// 2nd, shutdown I/O
		this.ioManager.close();

		// last, verify all memory is returned and shutdown mem manager
		MemoryManager memMan = getMemoryManager();
		if (memMan != null) {
			Assert.assertTrue("Memory Manager managed memory was not completely freed.", memMan.verifyEmpty());
			memMan.shutdown();
		}
	}
	
	// --------------------------------------------------------------------------------------------
	
	private static final class ListOutputCollector<OUT> implements Collector<OUT> {
		
		private final List<OUT> output;
		private final TypeSerializer<OUT> serializer;
		
		public ListOutputCollector(List<OUT> outputList, TypeSerializer<OUT> serializer) {
			this.output = outputList;
			this.serializer = serializer;
		}
		

		@Override
		public void collect(OUT record) {
			this.output.add(serializer.copy(record));
		}

		@Override
		public void close() {}
	}
	
	public static final class CountingOutputCollector<OUT> implements Collector<OUT> {
		
		private int num;

		@Override
		public void collect(OUT record) {
			this.num++;
		}

		@Override
		public void close() {}
		
		public int getNumberOfRecords() {
			return this.num;
		}
	}
}
