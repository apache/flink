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

package eu.stratosphere.pact.runtime.task;

import java.io.EOFException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.memorymanager.DataInputView;
import eu.stratosphere.nephele.services.memorymanager.ListMemorySegmentSource;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.services.memorymanager.MemorySegment;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.Stub;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.generic.types.TypeSerializer;
import eu.stratosphere.pact.runtime.io.SpillingBuffer;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;

/**
 * Temp task which is executed by a Nephele task manager. The task has a single
 * input and one outputs.
 * <p>
 * The TempTask collects all pairs from its input and dumps them on disk. After all pairs have been read and dumped,
 * they are read from disk and forwarded. The TempTask is automatically inserted by the PACT Compiler to avoid deadlocks
 * in Nephele's dataflow.
 * 
 * @author Stephan Ewen
 */
public class TempDriver<T> implements PactDriver<Stub, T>
{
	private static final Log LOG = LogFactory.getLog(TempDriver.class);

	private static final long MIN_REQUIRED_MEMORY = 512 * 1024;		// minimal memory for the task to operate

	private PactTaskContext<Stub, T> taskContext;
	
	private SpillingBuffer buffer;						// materialization barrier
	
	private List<MemorySegment> memory;
	
	private volatile boolean running;

	// ------------------------------------------------------------------------


	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.PactDriver#setup(eu.stratosphere.pact.runtime.task.PactTaskContext)
	 */
	@Override
	public void setup(PactTaskContext<Stub, T> context) {
		this.taskContext = context;
		this.running = true;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.AbstractPactTask#getNumberOfInputs()
	 */
	@Override
	public int getNumberOfInputs() {
		return 1;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.AbstractPactTask#getStubType()
	 */
	@Override
	public Class<Stub> getStubType() {
		return Stub.class;
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.AbstractPactTask#requiresComparatorOnInput()
	 */
	@Override
	public boolean requiresComparatorOnInput() {
		return false;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.AbstractPactTask#prepare()
	 */
	@Override
	public void prepare() throws Exception
	{
		final TaskConfig config = this.taskContext.getTaskConfig();
		
		// set up memory and I/O parameters
		final long availableMemory = config.getMemorySize();
		
		if (availableMemory < MIN_REQUIRED_MEMORY) {
			throw new RuntimeException("The temp task was initialized with too little memory: " + availableMemory +
				". Required is at least " + MIN_REQUIRED_MEMORY + " bytes.");
		}

		final MemoryManager memoryManager = this.taskContext.getMemoryManager();
		final IOManager ioManager = this.taskContext.getIOManager();
		
		this.memory = memoryManager.allocatePages(this.taskContext.getOwningNepheleTask(), availableMemory);
		this.buffer = new SpillingBuffer(ioManager, new ListMemorySegmentSource(this.memory), memoryManager.getPageSize());
	}


	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.AbstractPactTask#run()
	 */
	@Override
	public void run() throws Exception
	{
		if (LOG.isDebugEnabled())
			LOG.debug(this.taskContext.formatLogString("Preprocessing done, iterator obtained."));

		// cache references on the stack
		final MutableObjectIterator<T> input = this.taskContext.getInput(0);
		final SpillingBuffer buffer = this.buffer;
		final TypeSerializer<T> serializer = this.taskContext.getInputSerializer(0);
		final Collector<T> output = this.taskContext.getOutputCollector();
		
		final T record = serializer.createInstance();
		
		// first read everything
		while (this.running && input.next(record)) {
			serializer.serialize(record, buffer);
		}
		
		// forward pair to output writer
		final DataInputView inView = buffer.flip();
		try {
			while (true) {
				serializer.deserialize(record, inView);
				output.collect(record);
			}
		} catch (EOFException eofex) {
			// all good, we are done
		}
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.AbstractPactTask#cleanup()
	 */
	@Override
	public void cleanup() throws Exception
	{
		final MemoryManager memManager = this.taskContext.getMemoryManager();
		
		if (this.buffer != null) {	
			memManager.release(this.buffer.close());
			this.buffer = null;
		}
		
		memManager.release(this.memory);
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.PactDriver#cancel()
	 */
	@Override
	public void cancel() {
		this.running = false;
	}
}
