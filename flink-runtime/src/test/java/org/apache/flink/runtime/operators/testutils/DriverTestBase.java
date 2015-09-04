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

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.runtime.taskmanager.TaskManagerRuntimeInfo;
import org.apache.flink.util.TestLogger;
import org.junit.Assert;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializerFactory;
import org.apache.flink.api.common.typeutils.record.RecordComparator;
import org.apache.flink.api.common.typeutils.record.RecordSerializerFactory;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.operators.PactDriver;
import org.apache.flink.runtime.operators.PactTaskContext;
import org.apache.flink.runtime.operators.ResettablePactDriver;
import org.apache.flink.runtime.operators.sort.UnilateralSortMerger;
import org.apache.flink.runtime.operators.util.TaskConfig;
import org.apache.flink.types.Record;
import org.apache.flink.util.Collector;
import org.apache.flink.util.MutableObjectIterator;
import org.junit.After;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class DriverTestBase<S extends Function> extends TestLogger implements PactTaskContext<S, Record> {
	
	protected static final long DEFAULT_PER_SORT_MEM = 16 * 1024 * 1024;
	
	protected static final int PAGE_SIZE = 32 * 1024; 
	
	private final IOManager ioManager;
	
	private final MemoryManager memManager;

	private final List<MutableObjectIterator<Record>> inputs;
	
	private final List<TypeComparator<Record>> comparators;
	
	private final List<UnilateralSortMerger<Record>> sorters;
	
	private final AbstractInvokable owner;

	private final TaskConfig taskConfig;
	
	private final TaskManagerRuntimeInfo taskManageInfo;
	
	protected final long perSortMem;

	protected final double perSortFractionMem;
	
	private Collector<Record> output;
	
	protected int numFileHandles;
	
	private S stub;
	
	private PactDriver<S, Record> driver;
	
	private volatile boolean running = true;

	private ExecutionConfig executionConfig;
	
	protected DriverTestBase(ExecutionConfig executionConfig, long memory, int maxNumSorters) {
		this(executionConfig, memory, maxNumSorters, DEFAULT_PER_SORT_MEM);
	}
	
	protected DriverTestBase(ExecutionConfig executionConfig, long memory, int maxNumSorters, long perSortMemory) {
		if (memory < 0 || maxNumSorters < 0 || perSortMemory < 0) {
			throw new IllegalArgumentException();
		}
		
		final long totalMem = Math.max(memory, 0) + (Math.max(maxNumSorters, 0) * perSortMemory);
		
		this.perSortMem = perSortMemory;
		this.perSortFractionMem = (double)perSortMemory/totalMem;
		this.ioManager = new IOManagerAsync();
		this.memManager = totalMem > 0 ? new MemoryManager(totalMem,1) : null;

		this.inputs = new ArrayList<MutableObjectIterator<Record>>();
		this.comparators = new ArrayList<TypeComparator<Record>>();
		this.sorters = new ArrayList<UnilateralSortMerger<Record>>();
		
		this.owner = new DummyInvokable();
		this.taskConfig = new TaskConfig(new Configuration());
		this.executionConfig = executionConfig;
		this.taskManageInfo = new TaskManagerRuntimeInfo("localhost", new Configuration());
	}

	@Parameterized.Parameters
	public static Collection<Object[]> getConfigurations() {

		LinkedList<Object[]> configs = new LinkedList<Object[]>();

		ExecutionConfig withReuse = new ExecutionConfig();
		withReuse.enableObjectReuse();

		ExecutionConfig withoutReuse = new ExecutionConfig();
		withoutReuse.disableObjectReuse();

		Object[] a = { withoutReuse };
		configs.add(a);
		Object[] b = { withReuse };
		configs.add(b);

		return configs;
	}

	public void addInput(MutableObjectIterator<Record> input) {
		this.inputs.add(input);
		this.sorters.add(null);
	}
	
	public void addInputSorted(MutableObjectIterator<Record> input, RecordComparator comp) throws Exception {
		UnilateralSortMerger<Record> sorter = new UnilateralSortMerger<Record>(
				this.memManager, this.ioManager, input, this.owner, RecordSerializerFactory.get(), comp,
				this.perSortFractionMem, 32, 0.8f);
		this.sorters.add(sorter);
		this.inputs.add(null);
	}
	
	public void addDriverComparator(RecordComparator comparator) {
		this.comparators.add(comparator);
	}

	public void setOutput(Collector<Record> output) {
		this.output = output;
	}
	public void setOutput(List<Record> output) {
		this.output = new ListOutputCollector(output);
	}
	
	public int getNumFileHandlesForSort() {
		return numFileHandles;
	}

	
	public void setNumFileHandlesForSort(int numFileHandles) {
		this.numFileHandles = numFileHandles;
	}

	@SuppressWarnings("rawtypes")
	public void testDriver(PactDriver driver, Class stubClass) throws Exception {
		testDriverInternal(driver, stubClass);
	}

	@SuppressWarnings({"unchecked","rawtypes"})
	public void testDriverInternal(PactDriver driver, Class stubClass) throws Exception {

		this.driver = driver;
		driver.setup(this);

		this.stub = (S)stubClass.newInstance();

		// regular running logic
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

			if (!running) {
				return;
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
				catch (Throwable ignored) {}
			}

			// if resettable driver invoke tear down
			if (this.driver instanceof ResettablePactDriver) {
				final ResettablePactDriver<?, ?> resDriver = (ResettablePactDriver<?, ?>) this.driver;
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
	public void testResettableDriver(ResettablePactDriver driver, Class stubClass, int iterations) throws Exception {

		driver.setup(this);
		
		for(int i = 0; i < iterations; i++) {
			
			if(i == 0) {
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
		
		// compensate for races, where cancel is called before the driver is set
		// not that this is an artifact of a bad design of this test base, where the setup
		// of the basic properties is not separated from the invocation of the execution logic 
		while (this.driver == null) {
			Thread.sleep(200);
		}
		this.driver.cancel();
	}

	// --------------------------------------------------------------------------------------------

	@Override
	public TaskConfig getTaskConfig() {
		return this.taskConfig;
	}

	@Override
	public TaskManagerRuntimeInfo getTaskManagerInfo() {
		return this.taskManageInfo;
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
	public <X> MutableObjectIterator<X> getInput(int index) {
		MutableObjectIterator<Record> in = this.inputs.get(index);
		if (in == null) {
			// waiting from sorter
			try {
				in = this.sorters.get(index).getIterator();
			} catch (InterruptedException e) {
				throw new RuntimeException("Interrupted");
			}
			this.inputs.set(index, in);
		}
		
		@SuppressWarnings("unchecked")
		MutableObjectIterator<X> input = (MutableObjectIterator<X>) this.inputs.get(index);
		return input;
	}

	@Override
	public <X> TypeSerializerFactory<X> getInputSerializer(int index) {
		@SuppressWarnings("unchecked")
		TypeSerializerFactory<X> factory = (TypeSerializerFactory<X>) RecordSerializerFactory.get();
		return factory;
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
	public Collector<Record> getOutputCollector() {
		return this.output;
	}

	@Override
	public AbstractInvokable getOwningNepheleTask() {
		return this.owner;
	}

	@Override
	public String formatLogString(String message) {
		return "Driver Tester: " + message;
	}
	
	// --------------------------------------------------------------------------------------------
	
	@After
	public void shutdownAll() throws Exception {
		// 1st, shutdown sorters
		for (UnilateralSortMerger<?> sorter : this.sorters) {
			if (sorter != null) {
				sorter.close();
			}
		}
		this.sorters.clear();
		
		// 2nd, shutdown I/O
		this.ioManager.shutdown();
		Assert.assertTrue("I/O Manager has not properly shut down.", this.ioManager.isProperlyShutDown());

		// last, verify all memory is returned and shutdown mem manager
		MemoryManager memMan = getMemoryManager();
		if (memMan != null) {
			Assert.assertTrue("Memory Manager managed memory was not completely freed.", memMan.verifyEmpty());
			memMan.shutdown();
		}
	}
	
	// --------------------------------------------------------------------------------------------
	
	private static final class ListOutputCollector implements Collector<Record> {
		
		private final List<Record> output;
		
		public ListOutputCollector(List<Record> outputList) {
			this.output = outputList;
		}
		

		@Override
		public void collect(Record record) {
			this.output.add(record.createCopy());
		}

		@Override
		public void close() {}
	}
	
	public static final class CountingOutputCollector implements Collector<Record> {
		
		private int num;

		@Override
		public void collect(Record record) {
			this.num++;
		}

		@Override
		public void close() {}
		
		public int getNumberOfRecords() {
			return this.num;
		}
	}
}
