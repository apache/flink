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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.execution.librarycache.LibraryCacheManager;
import eu.stratosphere.nephele.io.BipartiteDistributionPattern;
import eu.stratosphere.nephele.io.DistributionPattern;
import eu.stratosphere.nephele.io.PointwiseDistributionPattern;
import eu.stratosphere.nephele.io.RecordDeserializer;
import eu.stratosphere.nephele.io.RecordReader;
import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.memorymanager.MemoryAllocationException;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.template.AbstractTask;
import eu.stratosphere.pact.common.stub.MatchStub;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.runtime.hash.BuildFirstHashMatchIterator;
import eu.stratosphere.pact.runtime.hash.BuildSecondHashMatchIterator;
import eu.stratosphere.pact.runtime.serialization.KeyValuePairDeserializer;
import eu.stratosphere.pact.runtime.sort.SortMergeMatchIterator;
import eu.stratosphere.pact.runtime.task.util.MatchTaskIterator;
import eu.stratosphere.pact.runtime.task.util.NepheleReaderIterator;
import eu.stratosphere.pact.runtime.task.util.OutputCollector;
import eu.stratosphere.pact.runtime.task.util.OutputEmitter;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;

/**
 * Match task which is executed by a Nephele task manager. The task has two
 * inputs and one or multiple outputs. It is provided with a MatchStub
 * implementation.
 * <p>
 * The MatchTask matches all two pairs that share the same key and come from different inputs. Each pair of pairs that
 * match are handed to the <code>match()</code> method of the MatchStub.
 * 
 * @see eu.stratosphere.pact.common.stub.MatchStub
 * @author Fabian Hueske
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class MatchTask extends AbstractTask
{	
	// obtain MatchTask logger
	private static final Log LOG = LogFactory.getLog(MatchTask.class);

	// the minimal amount of memory for the task to operate
	private static final long MIN_REQUIRED_MEMORY = 3 * 1024 * 1024;

	// reader of first input
	private RecordReader<KeyValuePair<Key, Value>> reader1;

	// reader of second input
	private RecordReader<KeyValuePair<Key, Value>> reader2;

	// output collector
	private OutputCollector output;

	// match stub implementation instance
	private MatchStub matchStub;

	// task config including stub parameters
	private TaskConfig config;
	
	// the iterator that does the actual matching
	private MatchTaskIterator matchIterator;
	
	// the memory dedicated to the sorter
	private long availableMemory;
	
	// maximum number of file handles
	private int maxFileHandles;
	
	// the fill fraction of the buffers that triggers the spilling
	private float spillThreshold;
	
	// cancel flag
	private volatile boolean taskRunning = true;

	// ------------------------------------------------------------------------
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public void registerInputOutput()
	{
		if (LOG.isDebugEnabled())
			LOG.debug(getLogString("Start registering input and output"));

		// initialize stub implementation
		initStub();

		// initialized input readers
		initInputReaders();

		// initialized output collector
		initOutputCollector();

		if (LOG.isDebugEnabled()) 
			LOG.debug(getLogString("Finished registering input and output"));
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void invoke() throws Exception
	{
		if (LOG.isInfoEnabled())
			LOG.info(getLogString("Start PACT code"));

		final MatchStub matchStub = this.matchStub;
		final OutputCollector collector = this.output;
		
		// obtain MatchTaskIterator
		try {
			this.matchIterator = getIterator(this.reader1, this.reader2);
			
			// open MatchTaskIterator
			this.matchIterator.open();
			
			if (LOG.isDebugEnabled())
				LOG.debug(getLogString("Iterator obtained"));

			// open match stub instance
			this.matchStub.open();
			
			// for each distinct key that is contained in both inputs
			while (this.taskRunning && matchIterator.callWithNextKey(matchStub, collector));
		}
		catch (Exception ex) {
			// drop, if the task was canceled
			if (this.taskRunning) {
				LOG.error(getLogString("Unexpected ERROR in PACT code"));
				throw ex;
			}
		}
		finally {
			// close MatchTaskIterator
			if (this.matchIterator != null) {
				this.matchIterator.close();
			}
			
			// close stub implementation.
			// when the stub is closed, anything will have been written, so any error will be logged but has no 
			// effect on the successful completion of the task
			try {
				this.matchStub.close();
			}
			catch (Throwable t) {
				LOG.error(getLogString("Error while closing the Match user function"), t);
			}

			// close output collector
			if (this.output != null) {
				this.output.close();
			}
		}

		// log a final end message
		if (this.taskRunning) {
			if (LOG.isInfoEnabled())
				LOG.info(getLogString("Finished PACT code"));
		}
		else {
			if (LOG.isWarnEnabled())
				LOG.warn(getLogString("PACT code cancelled"));
		}
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.nephele.template.AbstractInvokable#cancel()
	 */
	@Override
	public void cancel() throws Exception
	{
		this.taskRunning = false;
		if (this.matchIterator != null) {
			this.matchIterator.abort();
		}
		
		if (LOG.isWarnEnabled())
			LOG.warn(getLogString("Cancelling PACT code"));
	}
	
	// ------------------------------------------------------------------------

	/**
	 * Initializes the stub implementation and configuration.
	 * 
	 * @throws RuntimeException
	 *         Thrown if instance of stub implementation can not be
	 *         obtained.
	 */
	private void initStub()
	{
		// obtain task configuration (including stub parameters)
		this.config = new TaskConfig(getRuntimeConfiguration());

		// set up memory and I/O parameters
		this.availableMemory = config.getMemorySize();
		this.maxFileHandles = config.getNumFilehandles();
		this.spillThreshold = config.getSortSpillingTreshold();
		
		// test minimum memory requirements
		long strategyMinMem = 0;
		
		switch (config.getLocalStrategy()) {
			case SORT_BOTH_MERGE:
				strategyMinMem = MIN_REQUIRED_MEMORY * 2;
				break;
			case SORT_FIRST_MERGE: 
			case SORT_SECOND_MERGE:
			case MERGE:
			case HYBRIDHASH_FIRST:
			case HYBRIDHASH_SECOND:
			case MMHASH_FIRST:
			case MMHASH_SECOND:
				strategyMinMem = MIN_REQUIRED_MEMORY;
				break;
		}
		if (this.availableMemory < strategyMinMem) {
			throw new RuntimeException(
					"The Match task was initialized with too little memory for local strategy "+
					config.getLocalStrategy()+" : " + this.availableMemory + " bytes." +
				    "Required is at least " + strategyMinMem + " bytes.");
		}

		// obtain stub implementation class
		try {
			ClassLoader cl = LibraryCacheManager.getClassLoader(getEnvironment().getJobID());
			Class<? extends MatchStub> matchClass = config.getStubClass(MatchStub.class, cl);
			// obtain stub implementation instance
			this.matchStub = matchClass.newInstance();
			// configure stub instance
			this.matchStub.configure(config.getStubParameters());
		}
		catch (IOException ioe) {
			throw new RuntimeException("Library cache manager could not be instantiated.", ioe);
		}
		catch (ClassNotFoundException cnfe) {
			throw new RuntimeException("Stub implementation class was not found.", cnfe);
		}
		catch (InstantiationException ie) {
			throw new RuntimeException("Stub implementation could not be instanciated.", ie);
		}
		catch (IllegalAccessException iae) {
			throw new RuntimeException("Stub implementations nullary constructor is not accessible.", iae);
		}
	}

	/**
	 * Initializes the input readers of the MatchTask.
	 * 
	 * @throws RuntimeException
	 *         Thrown if no input ship strategy was provided.
	 */
	private void initInputReaders()
	{
		// create RecordDeserializers
		RecordDeserializer<KeyValuePair<Key, Value>> deserializer1 = new KeyValuePairDeserializer(
			this.matchStub.getFirstInKeyType(), this.matchStub.getFirstInValueType());
		RecordDeserializer<KeyValuePair<Key, Value>> deserializer2 = new KeyValuePairDeserializer(
			this.matchStub.getSecondInKeyType(), this.matchStub.getSecondInValueType());

		// determine distribution pattern for first reader from input ship strategy
		DistributionPattern dp1 = null;
		switch (config.getInputShipStrategy(0)) {
		case FORWARD:
			// forward requires Pointwise DP
			dp1 = new PointwiseDistributionPattern();
			break;
		case PARTITION_HASH:
			// partition requires Bipartite DP
			dp1 = new BipartiteDistributionPattern();
			break;
		case BROADCAST:
			// broadcast requires Bipartite DP
			dp1 = new BipartiteDistributionPattern();
			break;
		default:
			throw new RuntimeException("No input ship strategy provided for first input of MatchTask.");
		}

		// determine distribution pattern for second reader from input ship strategy
		DistributionPattern dp2 = null;
		switch (config.getInputShipStrategy(1)) {
		case FORWARD:
			// forward requires Pointwise DP
			dp2 = new PointwiseDistributionPattern();
			break;
		case PARTITION_HASH:
			// partition requires Bipartite DP
			dp2 = new BipartiteDistributionPattern();
			break;
		case BROADCAST:
			// broadcast requires Bipartite DP
			dp2 = new BipartiteDistributionPattern();
			break;
		default:
			throw new RuntimeException("No input ship strategy provided for second input of MatchTask.");
		}

		// create reader for first input
		this.reader1 = new RecordReader<KeyValuePair<Key, Value>>(this, deserializer1, dp1);
		// create reader for second input
		this.reader2 = new RecordReader<KeyValuePair<Key, Value>>(this, deserializer2, dp2);
	}

	/**
	 * Creates a writer for each output. Creates an OutputCollector which
	 * forwards its input to all writers.
	 */
	private void initOutputCollector()
	{	
		boolean fwdCopyFlag = false;
		
		// create output collector
		this.output = new OutputCollector<Key, Value>();
				
		// create a writer for each output
		for (int i = 0; i < this.config.getNumOutputs(); i++) {
			// obtain OutputEmitter from output ship strategy
			OutputEmitter oe = new OutputEmitter(this.config.getOutputShipStrategy(i));
			// create writer
			RecordWriter<KeyValuePair<Key, Value>> writer;
			writer = new RecordWriter<KeyValuePair<Key, Value>>(this,
				(Class<KeyValuePair<Key, Value>>) (Class<?>) KeyValuePair.class, oe);
			
			// add writer to output collector
			// the first writer does not need to send a copy
			// all following must send copies
			// TODO smarter decision is possible here, e.g. decide which channel may not need to copy, ...
			this.output.addWriter(writer, fwdCopyFlag);
			fwdCopyFlag = true;
		}
	}

	/**
	 * Returns a MatchTaskIterator according to the specified local strategy.
	 * 
	 * @param reader1 Input reader of the first input.
	 * @param reader2 Input reader of the second input.
	 * @return MatchTaskIterator The iterator implementation for the given local strategy.
	 * @throws RuntimeException Thrown if the local strategy is not supported.
	 */
	private MatchTaskIterator getIterator(RecordReader reader1, RecordReader reader2)
	throws MemoryAllocationException
	{
		// obtain task manager's memory manager
		final MemoryManager memoryManager = getEnvironment().getMemoryManager();
		// obtain task manager's IO manager
		final IOManager ioManager = getEnvironment().getIOManager();

		// create and return MatchTaskIterator according to provided local strategy.
		switch (config.getLocalStrategy())
		{
		case SORT_BOTH_MERGE:
		case SORT_FIRST_MERGE:
		case SORT_SECOND_MERGE:
		case MERGE:
			return new SortMergeMatchIterator(memoryManager, ioManager, reader1, reader2,
				matchStub.getFirstInKeyType(), matchStub.getFirstInValueType(), matchStub.getSecondInValueType(),
				this.availableMemory, this.maxFileHandles, this.spillThreshold,
				config.getLocalStrategy(), this);
		case HYBRIDHASH_FIRST:
			return new BuildFirstHashMatchIterator(
				new NepheleReaderIterator<KeyValuePair<Key, Value>>(this.reader1), 
				new NepheleReaderIterator<KeyValuePair<Key, Value>>(this.reader2),
				matchStub.getFirstInKeyType(), matchStub.getFirstInValueType(), matchStub.getSecondInValueType(),
				memoryManager, ioManager, this, this.availableMemory);
		case HYBRIDHASH_SECOND:
			return new BuildSecondHashMatchIterator(
				new NepheleReaderIterator<KeyValuePair<Key, Value>>(this.reader1), 
				new NepheleReaderIterator<KeyValuePair<Key, Value>>(this.reader2),
				matchStub.getFirstInKeyType(), matchStub.getFirstInValueType(), matchStub.getSecondInValueType(),
				memoryManager, ioManager, this, this.availableMemory);
		default:
			throw new RuntimeException("Unsupported local strategy for MatchTask: "+config.getLocalStrategy());
		}
	}
	
	
	/**
	 * Utility function that composes a string for logging purposes. The string includes the given message and
	 * the index of the task in its task group together with the number of tasks in the task group.
	 *  
	 * @param message The main message for the log.
	 * @return The string ready for logging.
	 */
	private String getLogString(String message)
	{
		StringBuilder bld = new StringBuilder(128);	
		bld.append(message);
		bld.append(':').append(' ');
		bld.append(this.getEnvironment().getTaskName());
		bld.append(' ').append('(');
		bld.append(this.getEnvironment().getIndexInSubtaskGroup() + 1);
		bld.append('/');
		bld.append(this.getEnvironment().getCurrentNumberOfSubtasks());
		bld.append(')');
		return bld.toString();
	}
}
