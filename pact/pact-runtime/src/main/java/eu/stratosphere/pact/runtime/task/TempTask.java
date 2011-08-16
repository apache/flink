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

import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.execution.librarycache.LibraryCacheManager;
import eu.stratosphere.nephele.io.BipartiteDistributionPattern;
import eu.stratosphere.nephele.io.DistributionPattern;
import eu.stratosphere.nephele.io.PointwiseDistributionPattern;
import eu.stratosphere.nephele.io.RecordDeserializer;
import eu.stratosphere.nephele.io.RecordReader;
import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.nephele.services.ServiceException;
import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.memorymanager.MemoryAllocationException;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.template.AbstractInvokable;
import eu.stratosphere.nephele.template.AbstractTask;
import eu.stratosphere.pact.common.stub.Stub;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.runtime.resettable.SpillingResettableIterator;
import eu.stratosphere.pact.runtime.serialization.KeyValuePairDeserializer;
import eu.stratosphere.pact.runtime.sort.CombiningUnilateralSortMerger;
import eu.stratosphere.pact.runtime.sort.UnilateralSortMerger;
import eu.stratosphere.pact.runtime.task.util.OutputCollector;
import eu.stratosphere.pact.runtime.task.util.OutputEmitter;
import eu.stratosphere.pact.runtime.task.util.SimpleCloseableInputProvider;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;
import eu.stratosphere.pact.runtime.task.util.TaskConfig.LocalStrategy;
import eu.stratosphere.pact.runtime.util.KeyComparator;
import eu.stratosphere.pact.runtime.util.KeyGroupedIterator;
import eu.stratosphere.pact.runtime.util.NepheleReaderIterator;

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
@SuppressWarnings({"unchecked", "rawtypes"})
public class TempTask extends AbstractPactTask
{
	// the minimal amount of memory required for the temp to work
	private static final long MIN_REQUIRED_MEMORY = 512 * 1024;

	// spilling thread
	private SpillingResettableIterator<PactRecord> tempIterator;
	

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
	public Class<ReduceStub> getStubType() {
		return ReduceStub.class;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.AbstractPactTask#prepare()
	 */
	@Override
	public void prepare() throws Exception
	{
		// set up memory and I/O parameters
		final long availableMemory = this.config.getMemorySize();
		final int maxFileHandles = this.config.getNumFilehandles();
		final float spillThreshold = this.config.getSortSpillingTreshold();
		
		availableMemory = this.config.getMemorySize();
		if (availableMemory < MIN_REQUIRED_MEMORY) {
			throw new RuntimeException("The temp task was initialized with too little memory: " + availableMemory +
				". Required is at least " + MIN_REQUIRED_MEMORY + " bytes.");
		}

		// obtain the TaskManager's MemoryManager
		final MemoryManager memoryManager = getEnvironment().getMemoryManager();
		// obtain the TaskManager's IOManager
		final IOManager ioManager = getEnvironment().getIOManager();
		
		tempIterator = new SpillingResettableIterator<PactRecord>(memoryManager, ioManager, 
				inputs[0], new PactRecord(), availableMemory, this);
		
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
		final SpillingResettableIterator<PactRecord> iter = this.tempIterator;
		final OutputCollector output = this.output;
		
		// run stub implementation
		while (this.running && iter.hasNext())
		{
			PactRecord record = iter.next();
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
}
