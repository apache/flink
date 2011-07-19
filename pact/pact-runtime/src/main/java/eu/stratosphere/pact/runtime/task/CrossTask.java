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
import eu.stratosphere.nephele.template.AbstractTask;
import eu.stratosphere.pact.common.stub.CrossStub;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.runtime.resettable.BlockResettableIterator;
import eu.stratosphere.pact.runtime.resettable.SpillingResettableIterator;
import eu.stratosphere.pact.runtime.serialization.KeyValuePairDeserializer;
import eu.stratosphere.pact.runtime.task.util.LastRepeatableIterator;
import eu.stratosphere.pact.runtime.task.util.NepheleReaderIterator;
import eu.stratosphere.pact.runtime.task.util.OutputCollector;
import eu.stratosphere.pact.runtime.task.util.OutputEmitter;
import eu.stratosphere.pact.runtime.task.util.SerializationCopier;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;
import eu.stratosphere.pact.runtime.task.util.TaskConfig.LocalStrategy;

/**
 * Cross task which is executed by a Nephele task manager. The task has two
 * inputs and one or multiple outputs. It is provided with a CrossStub
 * implementation.
 * <p>
 * The CrossTask builds the Cartesian product of the pairs of its two inputs. Each element (pair of pairs) is handed to
 * the <code>cross()</code> method of the CrossStub.
 * 
 * @see eu.stratosphere.pact.common.stub.CrossStub
 * @author Fabian Hueske
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class CrossTask extends AbstractTask
{
	// obtain CrossTask logger
	private static final Log LOG = LogFactory.getLog(CrossTask.class);

	// the minimal amount of memory for the task to operate
	private static final long MIN_REQUIRED_MEMORY = 1 * 1024 * 1024;
	
	// reader for first input
	private Iterator<KeyValuePair<Key, Value>> input1;

	// reader for second input
	private Iterator<KeyValuePair<Key, Value>> input2;

	// output collector
	private OutputCollector<Key, Value> output;

	// cross stub implementation instance
	private CrossStub stub;

	// task config including stub parameters
	private TaskConfig config;

	// spilling resettable iterator for inner input
	private SpillingResettableIterator<KeyValuePair<Key, Value>> spillingResetIt = null;
	private BlockResettableIterator<KeyValuePair<Key, Value>> blockResetIt = null;
	
	// the memory dedicated to the sorter
	private long availableMemory;
	
	// cancel flag
	private volatile boolean taskCanceled = false;
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public void registerInputOutput()
	{
		if (LOG.isDebugEnabled())
			LOG.debug(getLogString("Start registering input and output"));

		// Initialize stub implementation
		initStub();

		// Initialize input reader
		initInputReaders();

		// Initializes output writers and collector
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

		// inner reader for nested loops
		final Iterator<KeyValuePair<Key, Value>> innerInput;
		// outer reader for nested loops
		final Iterator<KeyValuePair<Key, Value>> outerInput;

		// assign inner and outer readers according to local strategy decision
		if (config.getLocalStrategy() == LocalStrategy.NESTEDLOOP_BLOCKED_OUTER_SECOND
			|| config.getLocalStrategy() == LocalStrategy.NESTEDLOOP_STREAMED_OUTER_SECOND) {
			innerInput = this.input1;
			outerInput = this.input2;
		} else if (config.getLocalStrategy() == LocalStrategy.NESTEDLOOP_BLOCKED_OUTER_FIRST
			|| config.getLocalStrategy() == LocalStrategy.NESTEDLOOP_STREAMED_OUTER_FIRST) {
			innerInput = this.input2;
			outerInput = this.input1;
		} else {
			throw new RuntimeException("Invalid local strategy for CROSS: " + config.getLocalStrategy());
		}

		// obtain memory manager from task manager
		final MemoryManager memoryManager = getEnvironment().getMemoryManager();
		// obtain IO manager from task manager
		final IOManager ioManager = getEnvironment().getIOManager();

		// run nested loops strategy according to local strategy decision
		if (config.getLocalStrategy() == LocalStrategy.NESTEDLOOP_BLOCKED_OUTER_FIRST
			|| config.getLocalStrategy() == LocalStrategy.NESTEDLOOP_BLOCKED_OUTER_SECOND) {
			// run blocked nested loop strategy
			runBlocked(memoryManager, ioManager, innerInput, outerInput);
		} else {
			// run streaming nested loop strategy (this is an opportunistic
			// choice!)
			runStreamed(memoryManager, ioManager, innerInput, outerInput);
		}

		if(!this.taskCanceled) {
			if (LOG.isInfoEnabled())
				LOG.info(getLogString("Finished PACT code"));
		} else {
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
		this.taskCanceled = true;
		
		if (this.spillingResetIt != null) {
			this.spillingResetIt.abort();
		}
		
		if (this.blockResetIt != null) {
			this.blockResetIt.close();
			this.blockResetIt = null;
		}
		
		if (LOG.isWarnEnabled())
			LOG.warn(getLogString("Cancelling PACT code"));
	}

	/**
	 * Initializes the stub implementation and configuration.
	 * 
	 * @throws RuntimeException
	 *         Throws if instance of stub implementation can not be
	 *         obtained.
	 */
	private void initStub() throws RuntimeException
	{
		// obtain task configuration (including stub parameters)
		this.config = new TaskConfig(getRuntimeConfiguration());

		// set up memory and I/O parameters
		this.availableMemory = this.config.getMemorySize();
		
		// test minimum memory requirements
		long strategyMinMem = 0;
		
		switch (this.config.getLocalStrategy()) {
			case NESTEDLOOP_BLOCKED_OUTER_FIRST:
			case NESTEDLOOP_BLOCKED_OUTER_SECOND: 
			case NESTEDLOOP_STREAMED_OUTER_FIRST: 
			case NESTEDLOOP_STREAMED_OUTER_SECOND: 
				strategyMinMem = MIN_REQUIRED_MEMORY;
				break;
		}
		
		if (this.availableMemory < strategyMinMem) {
			throw new RuntimeException(
					"The Cross task was initialized with too little memory for local strategy "+
					config.getLocalStrategy()+" : " + this.availableMemory + " bytes." +
				    "Required is at least " + strategyMinMem + " bytes.");
		}

		try {
			// obtain stub implementation class
			ClassLoader cl = LibraryCacheManager.getClassLoader(getEnvironment().getJobID());
			Class<? extends CrossStub> stubClass = config.getStubClass(CrossStub.class, cl);
			// obtain stub implementation instance
			this.stub = stubClass.newInstance();
			// configure stub instance
			this.stub.configure(this.config.getStubParameters());
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
	 * Initializes the input readers of the CrossTask.
	 * 
	 * @throws RuntimeException
	 *         Thrown if an input ship strategy was provided.
	 */
	private void initInputReaders() throws RuntimeException
	{
		// create RecordDeserializer for first input
		RecordDeserializer<KeyValuePair<Key, Value>> deserializer1 = new KeyValuePairDeserializer<Key, Value>(stub
			.getFirstInKeyType(), stub.getFirstInValueType());
		// create RecordDeserializer for second input
		RecordDeserializer<KeyValuePair<Key, Value>> deserializer2 = new KeyValuePairDeserializer<Key, Value>(stub
			.getSecondInKeyType(), stub.getSecondInValueType());

		// determine distribution pattern of first input (id=0)
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
		case SFR:
			// sfr requires Bipartite DP
			dp1 = new BipartiteDistributionPattern();
			break;
		default:
			throw new RuntimeException("No input ship strategy provided for first input of CrossTask.");
		}

		// determine distribution pattern of second input (id=1)
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
		case SFR:
			// sfr requires Bipartite DP
			dp2 = new BipartiteDistributionPattern();
			break;
		default:
			throw new RuntimeException("No input ship strategy provided for second input of CrossTask.");
		}

		// create reader of first input
		this.input1 = new NepheleReaderIterator<KeyValuePair<Key, Value>>(new RecordReader<KeyValuePair<Key, Value>>(this, deserializer1, dp1));
		// create reader of second input
		this.input2 = new NepheleReaderIterator<KeyValuePair<Key, Value>>(new RecordReader<KeyValuePair<Key, Value>>(this, deserializer2, dp2));
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
			// TODO smarter decision are possible here, e.g. decide which channel may not need to copy, ...
			this.output.addWriter(writer, fwdCopyFlag);
			fwdCopyFlag = true;
		}
	}

	/**
	 * Runs a blocked nested loop strategy to build the Cartesian product and
	 * call the <code>cross()</code> method of the CrossStub implementation. The
	 * outer side is read using a BlockResettableIterator. The inner side is
	 * read using a SpillingResettableIterator.
	 * 
	 * @see eu.stratosphere.pact.runtime.resettable.SpillingResettableIterator
	 * @see eu.stratosphere.pact.runtime.resettable.BlockResettableIterator
	 * @param memoryManager
	 *        The task manager's memory manager.
	 * @param ioManager
	 *        The task manager's IO manager
	 * @param innerReader
	 *        The inner reader of the nested loops.
	 * @param outerReader
	 *        The outer reader of the nested loops.
	 * @throws RuntimeException
	 *         Throws a RuntimeException if something fails during
	 *         execution.
	 */
	private void runBlocked(MemoryManager memoryManager, IOManager ioManager,
			Iterator<KeyValuePair<Key, Value>> innerReader, Iterator<KeyValuePair<Key, Value>> outerReader)
			throws Exception {

		// spilling iterator for inner side
		SpillingResettableIterator<KeyValuePair<Key, Value>> innerInput = null;
		// blocked iterator for outer side
		BlockResettableIterator<KeyValuePair<Key, Value>> outerInput = null;

		try {
			final boolean firstInputIsOuter;
		
			// obtain iterators according to local strategy decision
			if (this.config.getLocalStrategy() == LocalStrategy.NESTEDLOOP_BLOCKED_OUTER_SECOND) {
				// obtain spilling iterator (inner side) for first input
				try {
					innerInput = new SpillingResettableIterator<KeyValuePair<Key, Value>>(memoryManager, ioManager,
						innerReader, this.availableMemory / 2, new KeyValuePairDeserializer<Key, Value>(stub.getFirstInKeyType(), stub
							.getFirstInValueType()), this);
					this.spillingResetIt = innerInput;
				}
				catch (MemoryAllocationException mae) {
					throw new RuntimeException("Unable to obtain SpillingResettableIterator for first input", mae);
				}
				// obtain blocked iterator (outer side) for second input
				try {
					outerInput = new BlockResettableIterator<KeyValuePair<Key, Value>>(memoryManager,
							outerReader,
							this.availableMemory / 2, 1, new KeyValuePairDeserializer<Key, Value>(stub.getSecondInKeyType(), 
								stub.getSecondInValueType()), this);
					this.blockResetIt = outerInput;
				}
				catch (MemoryAllocationException mae) {
					throw new RuntimeException("Unable to obtain BlockResettableIterator for second input", mae);
				}
				firstInputIsOuter = false;
			}
			else if (this.config.getLocalStrategy() == LocalStrategy.NESTEDLOOP_BLOCKED_OUTER_FIRST) {
				// obtain spilling iterator (inner side) for second input
				try {
					innerInput = new SpillingResettableIterator<KeyValuePair<Key, Value>>(memoryManager, ioManager,
						innerReader, this.availableMemory / 2, new KeyValuePairDeserializer<Key, Value>(stub.getSecondInKeyType(),
							stub.getSecondInValueType()), this);
					this.spillingResetIt = innerInput;
				} catch (MemoryAllocationException mae) {
					throw new RuntimeException("Unable to obtain SpillingResettableIterator for second input", mae);
				}
				// obtain blocked iterator (outer side) for second input
				try {
					outerInput = new BlockResettableIterator<KeyValuePair<Key, Value>>(memoryManager, outerReader,
							this.availableMemory / 2, 1, new KeyValuePairDeserializer<Key, Value>(stub.getFirstInKeyType(), stub
							.getFirstInValueType()), this);
					this.blockResetIt = outerInput;
				}
				catch (MemoryAllocationException mae) {
					throw new RuntimeException("Unable to obtain BlockResettableIterator for first input", mae);
				}
				firstInputIsOuter = true;
			}
			else {
				throw new RuntimeException("Invalid local strategy for CrossTask: " + config.getLocalStrategy());
			}
	
			// open spilling resettable iterator
			try {
				innerInput.open();
			}
			catch (ServiceException se) {
				throw new RuntimeException("Unable to open SpillingResettableIterator", se);
			}
			catch (IOException ioe) {
				throw new RuntimeException("Unable to open SpillingResettableIterator", ioe);
			}
			catch (InterruptedException ie) {
				throw new RuntimeException("Unable to open SpillingResettableIterator", ie);
			}
			
			// check if task was canceled while data was read
			if (this.taskCanceled)
				return;
			
			// open blocked resettable iterator
			outerInput.open();
	
			if (LOG.isDebugEnabled()) {
				LOG.debug(getLogString("SpillingResettable iterator obtained"));
				LOG.debug(getLogString("BlockResettable iterator obtained"));
			}
	
			// open stub implementation
			this.stub.open();
	
			boolean moreOuterBlocks = false;

			if (innerInput.hasNext()) { // avoid painful work when one input is empty
				do {
					// loop over the spilled resettable iterator
					while (!this.taskCanceled && innerInput.hasNext()) {
						// get inner pair
						KeyValuePair<Key, Value> innerPair = innerInput.next();
						// loop over the pairs in the current memory block
						while (!this.taskCanceled && outerInput.hasNext()) {
							// get outer pair
							KeyValuePair<Key, Value> outerPair = outerInput.next();
		
							// call cross() method of CrossStub depending on local strategy
							if(firstInputIsOuter) {
								stub.cross(outerPair.getKey(), outerPair.getValue(), innerPair.getKey(), innerPair.getValue(), output);
							} else {
								stub.cross(innerPair.getKey(), innerPair.getValue(), outerPair.getKey(), outerPair.getValue(), output);
							}
	
							innerPair = innerInput.repeatLast();
						}
						// reset the memory block iterator to the beginning of the
						// current memory block (outer side)
						outerInput.reset();
					}
					// reset the spilling resettable iterator (inner side)
					moreOuterBlocks = outerInput.nextBlock();
					if(moreOuterBlocks) {
						innerInput.reset();
					}
				} while (!this.taskCanceled && moreOuterBlocks);
			}
				
			// close stub implementation
			this.stub.close();
		}
		catch (Exception ex) {
			// drop, if the task was canceled
			if (!this.taskCanceled) {
				LOG.error(getLogString("Unexpected ERROR in PACT code"));
				throw ex;
			}
		}
		finally {
			Throwable t1 = null, t2 = null;
			try {
				if(innerInput != null) {
					innerInput.close();
				}
			}
			catch (Throwable t) {
				LOG.warn(t);
				t1 = t;
			}
			
			try {
				if(outerInput != null) {
					outerInput.close();
				}
			}
			catch (Throwable t) {
				LOG.warn(t);
				t2 = t;
			}
			
			if(t1 != null) throw new RuntimeException("Error closing SpillingResettableIterator.", t1);
			if(t2 != null) throw new RuntimeException("Error closung BlockResettableIterator.", t2);
		}
	}

	/**
	 * Runs a streamed nested loop strategy to build the Cartesian product and
	 * call the <code>cross()</code> method of the CrossStub implementation.
	 * The outer side is read directly from the input reader. The inner side is
	 * read and reseted using a SpillingResettableIterator.
	 * 
	 * @see eu.stratosphere.pact.runtime.resettable.SpillingResettableIterator
	 * @param memoryManager
	 *        The task manager's memory manager.
	 * @param ioManager
	 *        The task manager's IO manager
	 * @param innerReader
	 *        The inner reader of the nested loops.
	 * @param outerReader
	 *        The outer reader of the nested loops.
	 * @throws RuntimeException
	 *         Throws a RuntimeException if something fails during
	 *         execution.
	 */
	private void runStreamed(MemoryManager memoryManager, IOManager ioManager,
			Iterator<KeyValuePair<Key, Value>> innerReader, final Iterator<KeyValuePair<Key, Value>> outerReader)
			throws Exception {

		// obtain streaming iterator for outer side
		// streaming is achieved by simply wrapping the input reader of the outer side
		
		// obtain SpillingResettableIterator for inner side
		SpillingResettableIterator<KeyValuePair<Key, Value>> innerInput = null;
		RepeatableIterator outerInput = null;
		
		try {
		
			final boolean firstInputIsOuter;
			
			if(config.getLocalStrategy() == LocalStrategy.NESTEDLOOP_STREAMED_OUTER_FIRST) {
				// obtain spilling resettable iterator for first input
				try {
					innerInput = new SpillingResettableIterator<KeyValuePair<Key, Value>>(memoryManager, ioManager,
						innerReader, this.availableMemory, new KeyValuePairDeserializer<Key, Value>(stub.getSecondInKeyType(), stub
							.getSecondInValueType()), this);
					spillingResetIt = innerInput;
				} catch (MemoryAllocationException mae) {
					throw new RuntimeException("Unable to obtain SpillingResettable iterator for inner side.", mae);
				}
				// obtain repeatable iterator for second input
				outerInput = new RepeatableIterator(outerReader, stub.getFirstInKeyType(), stub.getFirstInValueType());
				
				firstInputIsOuter = true;
			} else if(config.getLocalStrategy() == LocalStrategy.NESTEDLOOP_STREAMED_OUTER_SECOND) {
				// obtain spilling resettable iterator for second input
				try {
					innerInput = new SpillingResettableIterator<KeyValuePair<Key, Value>>(memoryManager, ioManager,
						innerReader, this.availableMemory, new KeyValuePairDeserializer<Key, Value>(stub.getFirstInKeyType(), stub
							.getFirstInValueType()), this);
					spillingResetIt = innerInput;
				} catch (MemoryAllocationException mae) {
					throw new RuntimeException("Unable to obtain SpillingResettable iterator for inner side.", mae);
				}
				// obtain repeatable iterator for first input
				outerInput = new RepeatableIterator(outerReader, stub.getSecondInKeyType(), stub.getSecondInValueType());
				
				firstInputIsOuter = false;
			} else {
				throw new RuntimeException("Invalid local strategy for CrossTask: " + config.getLocalStrategy());
			}
			
			// open spilling resettable iterator
			try {
				innerInput.open();
			} catch (ServiceException se) {
				throw new RuntimeException("Unable to open SpillingResettable iterator for inner side.", se);
			} catch (IOException ioe) {
				throw new RuntimeException("Unable to open SpillingResettable iterator for inner side.", ioe);
			} catch (InterruptedException ie) {
				throw new RuntimeException("Unable to open SpillingResettable iterator for inner side.", ie);
			}
			
			
			if (this.taskCanceled)
				return;
	
			if (LOG.isDebugEnabled())
				LOG.debug(getLogString("Resetable iterator obtained"));
	
			// open stub implementation
			stub.open();
			
			// if (config.getLocalStrategy() == LocalStrategy.NESTEDLOOP_STREAMED_OUTER_SECOND) {
			
			// read streamed iterator of outer side
			while (!this.taskCanceled && outerInput.hasNext()) {
				// get outer pair
				KeyValuePair outerPair = outerInput.next();
	
				// read spilled iterator of inner side
				while (!this.taskCanceled && innerInput.hasNext()) {
					// get inner pair
					KeyValuePair innerPair = innerInput.next();
	
					// call cross() method of CrossStub depending on local strategy
					if(firstInputIsOuter) {
						stub.cross(outerPair.getKey(), outerPair.getValue(), innerPair.getKey(), innerPair.getValue(), output);
					} else {
						stub.cross(innerPair.getKey(), innerPair.getValue(), outerPair.getKey(), outerPair.getValue(), output);
					}
					
					outerPair = outerInput.repeatLast();
				}
				// reset spilling resettable iterator of inner side
				if(!this.taskCanceled && outerInput.hasNext()) {
					innerInput.reset();
				}
			}
			
			// close stub implementation
			stub.close();
		
		}
		catch (Exception ex) {
			
			// drop, if the task was canceled
			if (!this.taskCanceled) {
				LOG.error(getLogString("Unexpected ERROR in PACT code"));
				throw ex;
			}
		}
		finally {
			// close spilling resettable iterator
			if(innerInput != null) {
				innerInput.close();
			}
		}
	}
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Utility class that turns a standard {@link java.util.Iterator} for key/value pairs into a
	 * {@link LastRepeatableIterator}. 
	 */
	private static final class RepeatableIterator implements LastRepeatableIterator<KeyValuePair<Key, Value>>
	{
		private final SerializationCopier<KeyValuePair<Key, Value>> copier = new SerializationCopier<KeyValuePair<Key,Value>>();
		
		private final KeyValuePairDeserializer<Key, Value> deserializer;
		
		private final Iterator<KeyValuePair<Key,Value>> input;
		
		public RepeatableIterator(Iterator<KeyValuePair<Key,Value>> input, Class<Key> keyClass, Class<Value> valClass) {
			this.input = input;
			this.deserializer = new KeyValuePairDeserializer<Key, Value>(keyClass, valClass);
		}
		
		@Override
		public boolean hasNext() {
			return this.input.hasNext();
		}

		@Override
		public KeyValuePair<Key, Value> next() {
			KeyValuePair<Key,Value> pair = this.input.next();
			// serialize pair
			this.copier.setCopy(pair);	
			return pair;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}

		@Override
		public KeyValuePair<Key, Value> repeatLast() {
			KeyValuePair<Key,Value> pair = this.deserializer.getInstance();
			this.copier.getCopy(pair);	
			return pair;
		}
	}
	
	// --------------------------------------------------------------------------------------------
	
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
		bld.append(' ').append('"');
		bld.append(this.getEnvironment().getIndexInSubtaskGroup() + 1);
		bld.append('/');
		bld.append(this.getEnvironment().getCurrentNumberOfSubtasks());
		bld.append(')');
		return bld.toString();
	}
}
