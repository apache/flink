/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.pact.runtime.test.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.junit.After;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.services.memorymanager.spi.DefaultMemoryManager;
import eu.stratosphere.nephele.template.AbstractInvokable;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.Stub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.generic.types.TypeComparator;
import eu.stratosphere.pact.generic.types.TypeSerializer;
import eu.stratosphere.pact.runtime.plugable.pactrecord.PactRecordComparator;
import eu.stratosphere.pact.runtime.plugable.pactrecord.PactRecordSerializer;
import eu.stratosphere.pact.runtime.sort.UnilateralSortMerger;
import eu.stratosphere.pact.runtime.task.PactDriver;
import eu.stratosphere.pact.runtime.task.PactTaskContext;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;

public class DriverTestBase<S extends Stub> implements PactTaskContext<S, PactRecord>
{
	protected static final long DEFAULT_PER_SORT_MEM = 16 * 1024 * 1024;
	
	protected static final int PAGE_SIZE = 32 * 1024; 
	
	private final IOManager ioManager;
	
	private final MemoryManager memManager;
	
	private final List<MutableObjectIterator<PactRecord>> inputs;
	
	private final List<TypeComparator<PactRecord>> comparators;
	
	private final List<UnilateralSortMerger<PactRecord>> sorters;
	
	private final AbstractInvokable owner;
	
	private final Configuration config;
	
	private final TaskConfig taskConfig;
	
	protected final long perSortMem;
	
	private Collector<PactRecord> output;
	
	protected int numFileHandles;
	
	private S stub;
	
	private PactDriver<S, PactRecord> driver;
	
	private volatile boolean running;
	
	protected DriverTestBase(long memory, int maxNumSorters) {
		this(memory, maxNumSorters, DEFAULT_PER_SORT_MEM);
	}
	
	protected DriverTestBase(long memory, int maxNumSorters, long perSortMemory)
	{
		if (memory < 0 || maxNumSorters < 0 || perSortMemory < 0) {
			throw new IllegalArgumentException();
		}
		
		final long totalMem = Math.max(memory, 0) + (Math.max(maxNumSorters, 0) * perSortMemory);
		
		this.perSortMem = perSortMemory;
		this.ioManager = new IOManager();
		this.memManager = totalMem > 0 ? new DefaultMemoryManager(totalMem) : null;
		
		this.inputs = new ArrayList<MutableObjectIterator<PactRecord>>();
		this.comparators = new ArrayList<TypeComparator<PactRecord>>();
		this.sorters = new ArrayList<UnilateralSortMerger<PactRecord>>();
		
		this.owner = new DummyInvokable();
		
		this.config = new Configuration();
		this.taskConfig = new TaskConfig(this.config);
	}

	public void addInput(MutableObjectIterator<PactRecord> input) {
		this.inputs.add(input);
		this.sorters.add(null);
	}
	
	public void addInputSorted(MutableObjectIterator<PactRecord> input, PactRecordComparator comp) throws Exception {
		UnilateralSortMerger<PactRecord> sorter = new UnilateralSortMerger<PactRecord>(
				this.memManager, this.ioManager, input, this.owner, PactRecordSerializer.get(), comp, this.perSortMem, 32, 0.8f);
		this.sorters.add(sorter);
		this.inputs.add(null);
	}
	
	public void addInputComparator(PactRecordComparator comparator) {
		this.comparators.add(comparator);
	}

	public void setOutput(Collector<PactRecord> output) {
		this.output = output;
	}
	public void setOutput(List<PactRecord> output) {
		this.output = new ListOutputCollector(output);
	}
	
	public int getNumFileHandlesForSort() {
		return numFileHandles;
	}

	
	public void setNumFileHandlesForSort(int numFileHandles) {
		this.numFileHandles = numFileHandles;
	}

	public void testDriver(PactDriver<S, PactRecord> driver, Class<? extends S> stubClass)
	throws Exception
	{
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
	
	public void cancel() throws Exception
	{
		this.running = false;
		this.driver.cancel();
	}

	// --------------------------------------------------------------------------------------------

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.PactTaskContext#getTaskConfig()
	 */
	@Override
	public TaskConfig getTaskConfig() {
		return this.taskConfig;
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.PactTaskContext#getUserCodeClassLoader()
	 */
	@Override
	public ClassLoader getUserCodeClassLoader() {
		return getClass().getClassLoader();
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.PactTaskContext#getIOManager()
	 */
	@Override
	public IOManager getIOManager() {
		return this.ioManager;
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.PactTaskContext#getMemoryManager()
	 */
	@Override
	public MemoryManager getMemoryManager() {
		return this.memManager;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.PactTaskContext#getInput(int)
	 */
	@Override
	public <X> MutableObjectIterator<X> getInput(int index) {
		MutableObjectIterator<PactRecord> in = this.inputs.get(index);
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

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.PactTaskContext#resetInput(int)
	 */
	@Override
	public void resetInput(int index) throws IOException, UnsupportedOperationException {
		throw new UnsupportedOperationException();
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.PactTaskContext#getInputSerializer(int)
	 */
	@Override
	public <X> TypeSerializer<X> getInputSerializer(int index) {
		@SuppressWarnings("unchecked")
		TypeSerializer<X> serializer = (TypeSerializer<X>) PactRecordSerializer.get();
		return serializer;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.PactTaskContext#getInputComparator(int)
	 */
	@Override
	public <X> TypeComparator<X> getInputComparator(int index) {
		@SuppressWarnings("unchecked")
		TypeComparator<X> comparator = (TypeComparator<X>) this.comparators.get(index);
		return comparator;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.PactTaskContext#getStub()
	 */
	@Override
	public S getStub() {
		return this.stub;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.PactTaskContext#getOutputCollector()
	 */
	@Override
	public Collector<PactRecord> getOutputCollector() {
		return this.output;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.PactTaskContext#getOwningNepheleTask()
	 */
	@Override
	public AbstractInvokable getOwningNepheleTask() {
		return this.owner;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.PactTaskContext#formatLogString(java.lang.String)
	 */
	@Override
	public String formatLogString(String message) {
		return "Driver Tester: " + message;
	}
	
	// --------------------------------------------------------------------------------------------
	
	@After
	public void shutdownSorters() throws Exception
	{
		for (UnilateralSortMerger<?> sorter : this.sorters) {
			if (sorter != null)
				sorter.close();
		}
		this.sorters.clear();
	}
	
	@After
	public void shutdownIOManager() throws Exception
	{
		this.ioManager.shutdown();
		Assert.assertTrue("I/O Manager has not properly shut down.", this.ioManager.isProperlyShutDown());
	}

	@After
	public void shutdownMemoryManager() throws Exception
	{
		final MemoryManager memMan = getMemoryManager();
		if (memMan != null) {
			Assert.assertTrue("Memory Manager managed memory was not completely freed.", memMan.verifyEmpty());
			memMan.shutdown();
		}
	}
	
	// --------------------------------------------------------------------------------------------
	
	private static final class ListOutputCollector implements Collector<PactRecord>
	{
		private final List<PactRecord> output;
		
		
		public ListOutputCollector(List<PactRecord> outputList) {
			this.output = outputList;
		}
		

		/* (non-Javadoc)
		 * @see eu.stratosphere.pact.common.stubs.Collector#collect(java.lang.Object)
		 */
		@Override
		public void collect(PactRecord record) {
			this.output.add(record.createCopy());
		}

		/* (non-Javadoc)
		 * @see eu.stratosphere.pact.common.stubs.Collector#close()
		 */
		@Override
		public void close() {}
	}
	
	public static final class CountingOutputCollector implements Collector<PactRecord>
	{
		private int num;

		@Override
		public void collect(PactRecord record) {
			this.num++;
		}

		@Override
		public void close() {}
		
		public int getNumberOfRecords() {
			return this.num;
		}
	}
}
