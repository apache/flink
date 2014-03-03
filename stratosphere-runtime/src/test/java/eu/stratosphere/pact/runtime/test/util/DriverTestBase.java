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

package eu.stratosphere.pact.runtime.test.util;

import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.apache.log4j.Level;
import org.junit.After;
import org.junit.BeforeClass;

import eu.stratosphere.api.common.functions.Function;
import eu.stratosphere.api.common.typeutils.TypeComparator;
import eu.stratosphere.api.common.typeutils.TypeSerializer;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.services.memorymanager.spi.DefaultMemoryManager;
import eu.stratosphere.nephele.template.AbstractInvokable;
import eu.stratosphere.pact.runtime.plugable.pactrecord.RecordComparator;
import eu.stratosphere.pact.runtime.plugable.pactrecord.RecordSerializer;
import eu.stratosphere.pact.runtime.sort.UnilateralSortMerger;
import eu.stratosphere.pact.runtime.task.PactDriver;
import eu.stratosphere.pact.runtime.task.PactTaskContext;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;
import eu.stratosphere.types.Record;
import eu.stratosphere.util.Collector;
import eu.stratosphere.util.LogUtils;
import eu.stratosphere.util.MutableObjectIterator;

public class DriverTestBase<S extends Function> implements PactTaskContext<S, Record> {
	
	protected static final long DEFAULT_PER_SORT_MEM = 16 * 1024 * 1024;
	
	protected static final int PAGE_SIZE = 32 * 1024; 
	
	private final IOManager ioManager;
	
	private final MemoryManager memManager;
	
	private final List<MutableObjectIterator<Record>> inputs;
	
	private final List<TypeComparator<Record>> comparators;
	
	private final List<UnilateralSortMerger<Record>> sorters;
	
	private final AbstractInvokable owner;
	
	private final Configuration config;
	
	private final TaskConfig taskConfig;
	
	protected final long perSortMem;
	
	private Collector<Record> output;
	
	protected int numFileHandles;
	
	private S stub;
	
	private PactDriver<S, Record> driver;
	
	private volatile boolean running;
	
	
	@BeforeClass
	public static void setupLog() {
		/// suppress log output, as this class produces errors on purpose to test exception handling
		LogUtils.initializeDefaultConsoleLogger(Level.OFF);
	}
	
	
	protected DriverTestBase(long memory, int maxNumSorters) {
		this(memory, maxNumSorters, DEFAULT_PER_SORT_MEM);
	}
	
	protected DriverTestBase(long memory, int maxNumSorters, long perSortMemory) {
		if (memory < 0 || maxNumSorters < 0 || perSortMemory < 0) {
			throw new IllegalArgumentException();
		}
		
		final long totalMem = Math.max(memory, 0) + (Math.max(maxNumSorters, 0) * perSortMemory);
		
		this.perSortMem = perSortMemory;
		this.ioManager = new IOManager();
		this.memManager = totalMem > 0 ? new DefaultMemoryManager(totalMem) : null;
		
		this.inputs = new ArrayList<MutableObjectIterator<Record>>();
		this.comparators = new ArrayList<TypeComparator<Record>>();
		this.sorters = new ArrayList<UnilateralSortMerger<Record>>();
		
		this.owner = new DummyInvokable();
		
		this.config = new Configuration();
		this.taskConfig = new TaskConfig(this.config);
	}

	public void addInput(MutableObjectIterator<Record> input) {
		this.inputs.add(input);
		this.sorters.add(null);
	}
	
	public void addInputSorted(MutableObjectIterator<Record> input, RecordComparator comp) throws Exception {
		UnilateralSortMerger<Record> sorter = new UnilateralSortMerger<Record>(
				this.memManager, this.ioManager, input, this.owner, RecordSerializer.get(), comp, this.perSortMem, 32, 0.8f);
		this.sorters.add(sorter);
		this.inputs.add(null);
	}
	
	public void addInputComparator(RecordComparator comparator) {
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

	public void testDriver(PactDriver<S, Record> driver, Class<? extends S> stubClass) throws Exception {
		
		this.driver = driver;
		driver.setup(this);
		
		// instantiate the stub
		this.stub = stubClass.newInstance();
		
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
				this.stub.open(getTaskConfig().getStubParameters());
				stubOpen = true;
			}
			catch (Throwable t) {
				throw new Exception("The user defined 'open()' method caused an exception: " + t.getMessage(), t);
			}
			
			// run the user code
			driver.run();
			
			// close. We close here such that a regular close throwing an exception marks a task as failed.
			if (this.running) {
				this.stub.close();
				stubOpen = false;
			}
			
			this.output.close();
		}
		catch (Exception ex) {
			// close the input, but do not report any exceptions, since we already have another root cause
			if (stubOpen) {
				try {
					this.stub.close();
				}
				catch (Throwable t) {}
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
	public <X> TypeSerializer<X> getInputSerializer(int index) {
		@SuppressWarnings("unchecked")
		TypeSerializer<X> serializer = (TypeSerializer<X>) RecordSerializer.get();
		return serializer;
	}

	@Override
	public <X> TypeComparator<X> getInputComparator(int index) {
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
			if (sorter != null)
				sorter.close();
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
