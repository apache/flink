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

import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.pact.common.stubs.Stub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.runtime.resettable.SpillingResettableMutableObjectIterator;
import eu.stratosphere.pact.runtime.task.util.OutputCollector;

/**
 * Temp task which is executed by a Nephele task manager. The task has a single
 * input and one outputs.
 * <p>
 * The TempTask collects all pairs from its input and dumps them on disk. After all pairs have been read and dumped,
 * they are read from disk and forwarded. The TempTask is automatically inserted by the PACT Compiler to avoid deadlocks
 * in Nepheles dataflow.
 * 
 * @author Fabian Hueske
 * @author Matthias Ringwald
 */
public class TempTask extends AbstractPactTask<Stub>
{
	// the minimal amount of memory required for the temp to work
	private static final long MIN_REQUIRED_MEMORY = 512 * 1024;

	// spilling thread
	private SpillingResettableMutableObjectIterator tempIterator;
	

	// ------------------------------------------------------------------------


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
	 * @see eu.stratosphere.pact.runtime.task.AbstractPactTask#prepare()
	 */
	@Override
	public void prepare() throws Exception
	{
		// set up memory and I/O parameters
		final long availableMemory = this.config.getMemorySize();
		
		if (availableMemory < MIN_REQUIRED_MEMORY) {
			throw new RuntimeException("The temp task was initialized with too little memory: " + availableMemory +
				". Required is at least " + MIN_REQUIRED_MEMORY + " bytes.");
		}

		// obtain the TaskManager's MemoryManager
		final MemoryManager memoryManager = getEnvironment().getMemoryManager();
		// obtain the TaskManager's IOManager
		final IOManager ioManager = getEnvironment().getIOManager();
		
		tempIterator = new SpillingResettableMutableObjectIterator(memoryManager, ioManager, 
				inputs[0], availableMemory, this);
		
		tempIterator.open();
		
	}


	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.AbstractPactTask#run()
	 */
	@Override
	public void run() throws Exception
	{
		if (LOG.isDebugEnabled())
			LOG.debug(getLogString("Preprocessing done, iterator obtained."));

		// cache references on the stack
		final SpillingResettableMutableObjectIterator iter = this.tempIterator;
		final OutputCollector output = this.output;
		
		PactRecord record = new PactRecord();
		// run stub implementation
		while (this.running && iter.next(record))
		{
			// forward pair to output writer
			output.collect(record);
		}
			
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.AbstractPactTask#cleanup()
	 */
	@Override
	public void cleanup() throws Exception {
		if (this.tempIterator != null) {
			this.tempIterator.close();
			this.tempIterator = null;
		}
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.AbstractPactTask#cancel()
	 */
	@Override
	public void cancel() throws Exception
	{
		super.cancel();
		if (this.tempIterator != null) {
			tempIterator.abort();
		}
	}
}
