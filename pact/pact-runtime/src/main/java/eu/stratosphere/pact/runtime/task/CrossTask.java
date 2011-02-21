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
import eu.stratosphere.nephele.services.ServiceException;
import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.memorymanager.MemoryAllocationException;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.template.AbstractTask;
import eu.stratosphere.pact.common.stub.CrossStub;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.Pair;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.runtime.resettable.BlockResettableIterator;
import eu.stratosphere.pact.runtime.resettable.SpillingResettableIterator;
import eu.stratosphere.pact.runtime.serialization.KeyValuePairDeserializer;
import eu.stratosphere.pact.runtime.task.util.LastRepeatableIterator;
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
@SuppressWarnings("unchecked")
public class CrossTask extends AbstractTask {

	// memory to be used for IO buffering
	public int MEMORY_IO;

	// obtain CrossTask logger
	private static final Log LOG = LogFactory.getLog(CrossTask.class);

	// reader for first input
	private RecordReader<KeyValuePair<Key, Value>> reader1;

	// reader for second input
	private RecordReader<KeyValuePair<Key, Value>> reader2;

	// output collector
	private OutputCollector<Key, Value> output;

	// cross stub implementation instance
	private CrossStub stub;

	// task config including stub parameters
	private TaskConfig config;

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void registerInputOutput() {
		LOG.debug("Start registering input and output: " + this.getEnvironment().getTaskName() + " ("
			+ (this.getEnvironment().getIndexInSubtaskGroup() + 1) + "/"
			+ this.getEnvironment().getCurrentNumberOfSubtasks() + ")");

		// Initialize stub implementation
		initStub();

		// Initialize input reader
		initInputReaders();

		// Initializes output writers and collector
		initOutputCollector();

		LOG.debug("Finished registering input and output: " + this.getEnvironment().getTaskName() + " ("
			+ (this.getEnvironment().getIndexInSubtaskGroup() + 1) + "/"
			+ this.getEnvironment().getCurrentNumberOfSubtasks() + ")");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void invoke() throws Exception {

		LOG.info("Start PACT code: " + this.getEnvironment().getTaskName() + " ("
			+ (this.getEnvironment().getIndexInSubtaskGroup() + 1) + "/"
			+ this.getEnvironment().getCurrentNumberOfSubtasks() + ")");

		// inner reader for nested loops
		final RecordReader<KeyValuePair<Key, Value>> innerReader;
		// outer reader for nested loops
		final RecordReader<KeyValuePair<Key, Value>> outerReader;

		// assign inner and outer readers according to local strategy decision
		if (config.getLocalStrategy() == LocalStrategy.NESTEDLOOP_BLOCKED_OUTER_SECOND
			|| config.getLocalStrategy() == LocalStrategy.NESTEDLOOP_STREAMED_OUTER_SECOND) {
			innerReader = reader1;
			outerReader = reader2;
		} else if (config.getLocalStrategy() == LocalStrategy.NESTEDLOOP_BLOCKED_OUTER_FIRST
			|| config.getLocalStrategy() == LocalStrategy.NESTEDLOOP_STREAMED_OUTER_FIRST) {
			innerReader = reader2;
			outerReader = reader1;
		} else {
			throw new RuntimeException("Invalid local strategy for CROSS: " + config.getLocalStrategy());
		}

		// obtain memory manager from task manager
		final MemoryManager memoryManager = getEnvironment().getMemoryManager();
		// obtain IO manager from task manager
		final IOManager ioManager = getEnvironment().getIOManager();

		// run nested loops strategy accoring to local strategy decision
		if (config.getLocalStrategy() == LocalStrategy.NESTEDLOOP_BLOCKED_OUTER_FIRST
			|| config.getLocalStrategy() == LocalStrategy.NESTEDLOOP_BLOCKED_OUTER_SECOND) {
			// run blocked nested loop strategy
			runBlocked(memoryManager, ioManager, innerReader, outerReader);
		} else {
			// run streaming nested loop strategy (this is an opportunistic
			// choice!)
			runStreamed(memoryManager, ioManager, innerReader, outerReader);
		}

		LOG.info("Finished PACT code: " + this.getEnvironment().getTaskName() + " ("
			+ (this.getEnvironment().getIndexInSubtaskGroup() + 1) + "/"
			+ this.getEnvironment().getCurrentNumberOfSubtasks() + ")");
	}

	/**
	 * Initializes the stub implementation and configuration.
	 * 
	 * @throws RuntimeException
	 *         Throws if instance of stub implementation can not be
	 *         obtained.
	 */
	private void initStub() throws RuntimeException {

		// obtain task configuration (including stub parameters)
		config = new TaskConfig(getRuntimeConfiguration());

		// set up memory and io parameters
		MEMORY_IO = config.getIOBufferSize() * 1024 * 1024;

		try {
			// obtain stub implementation class
			ClassLoader cl = LibraryCacheManager.getClassLoader(getEnvironment().getJobID());
			Class<? extends CrossStub> stubClass = config.getStubClass(CrossStub.class, cl);
			// obtain stub implementation instance
			stub = stubClass.newInstance();
			// configure stub instance
			stub.configure(config.getStubParameters());
		} catch (IOException ioe) {
			throw new RuntimeException("Library cache manager could not be instantiated.", ioe);
		} catch (ClassNotFoundException cnfe) {
			throw new RuntimeException("Stub implementation class was not found.", cnfe);
		} catch (InstantiationException ie) {
			throw new RuntimeException("Stub implementation could not be instanciated.", ie);
		} catch (IllegalAccessException iae) {
			throw new RuntimeException("Stub implementations nullary constructor is not accessible.", iae);
		}
	}

	/**
	 * Initializes the input readers of the CrossTask.
	 * 
	 * @throws RuntimeException
	 *         Thrown if an input ship strategy was provided.
	 */
	private void initInputReaders() throws RuntimeException {

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
		reader1 = new RecordReader<KeyValuePair<Key, Value>>(this, deserializer1, dp1);
		// create reader of second input
		reader2 = new RecordReader<KeyValuePair<Key, Value>>(this, deserializer2, dp2);
	}

	/**
	 * Creates a writer for each output. Creates an OutputCollector which
	 * forwards its input to all writers.
	 */
	private void initOutputCollector() {

		boolean fwdCopyFlag = false;
		
		// create output collector
		output = new OutputCollector<Key, Value>();
		
		// create a writer for each output
		for (int i = 0; i < config.getNumOutputs(); i++) {
			// obtain OutputEmitter from output ship strategy
			OutputEmitter oe = new OutputEmitter(config.getOutputShipStrategy(i));
			// create writer
			RecordWriter<KeyValuePair<Key, Value>> writer;
			writer = new RecordWriter<KeyValuePair<Key, Value>>(this,
				(Class<KeyValuePair<Key, Value>>) (Class<?>) KeyValuePair.class, oe);
			
			// add writer to output collector
			// the first writer does not need to send a copy
			// all following must send copies
			// TODO smarter decision are possible here, e.g. decide which channel may not need to copy, ...
			output.addWriter(writer, fwdCopyFlag);
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
			RecordReader<KeyValuePair<Key, Value>> innerReader, RecordReader<KeyValuePair<Key, Value>> outerReader)
			throws RuntimeException {

		// spilling iterator for inner side
		SpillingResettableIterator<KeyValuePair<Key, Value>> innerInput = null;
		// blocked iterator for outer side
		BlockResettableIterator<KeyValuePair<Key, Value>> outerInput = null;

		try {
		
			// obtain iterators according to local strategy decision
			if (config.getLocalStrategy() == LocalStrategy.NESTEDLOOP_BLOCKED_OUTER_SECOND) {
				// obtain spilling iterator (inner side) for first input
				try {
					innerInput = new SpillingResettableIterator<KeyValuePair<Key, Value>>(memoryManager, ioManager,
						innerReader, MEMORY_IO / 2, new KeyValuePairDeserializer<Key, Value>(stub.getFirstInKeyType(), stub
							.getFirstInValueType()), this);
				} catch (MemoryAllocationException mae) {
					throw new RuntimeException("Unable to obtain SpillingResettableIterator for first input", mae);
				}
				// obtain blocked iterator (outer side) for second input
				try {
					outerInput = new BlockResettableIterator<KeyValuePair<Key, Value>>(memoryManager, outerReader,
						MEMORY_IO / 2, 1, new KeyValuePairDeserializer<Key, Value>(stub.getSecondInKeyType(), 
								stub.getSecondInValueType()), this);
				} catch (MemoryAllocationException mae) {
					throw new RuntimeException("Unable to obtain BlockResettableIterator for second input", mae);
				}
	
			} else if (config.getLocalStrategy() == LocalStrategy.NESTEDLOOP_BLOCKED_OUTER_FIRST) {
				// obtain spilling iterator (inner side) for second input
				try {
					innerInput = new SpillingResettableIterator<KeyValuePair<Key, Value>>(memoryManager, ioManager,
						innerReader, MEMORY_IO / 2, new KeyValuePairDeserializer<Key, Value>(stub.getSecondInKeyType(),
							stub.getSecondInValueType()), this);
				} catch (MemoryAllocationException mae) {
					throw new RuntimeException("Unable to obtain SpillingResettableIterator for second input", mae);
				}
				// obtain blocked iterator (outer side) for second input
				try {
					outerInput = new BlockResettableIterator<KeyValuePair<Key, Value>>(memoryManager, outerReader,
						MEMORY_IO / 2, 1, new KeyValuePairDeserializer<Key, Value>(stub.getFirstInKeyType(), stub
							.getFirstInValueType()), this);
				} catch (MemoryAllocationException mae) {
					throw new RuntimeException("Unable to obtain BlockResettableIterator for first input", mae);
				}
			} else {
				throw new RuntimeException("Invalid local strategy for CrossTask: " + config.getLocalStrategy());
			}
	
			// open spilling resettable iterator
			try {
				innerInput.open();
			} catch (ServiceException se) {
				throw new RuntimeException("Unable to open SpillingResettableIterator", se);
			} catch (IOException ioe) {
				throw new RuntimeException("Unable to open SpillingResettableIterator", ioe);
			} catch (InterruptedException ie) {
				throw new RuntimeException("Unable to open SpillingResettableIterator", ie);
			}
			// open blocked resettable iterator
			outerInput.open();
	
			LOG.debug("SpillingResettable iterator obtained: " + this.getEnvironment().getTaskName() + " ("
				+ (this.getEnvironment().getIndexInSubtaskGroup() + 1) + "/"
				+ this.getEnvironment().getCurrentNumberOfSubtasks() + ")");
			LOG.debug("BlockResettable iterator obtained: " + this.getEnvironment().getTaskName() + " ("
				+ (this.getEnvironment().getIndexInSubtaskGroup() + 1) + "/"
				+ this.getEnvironment().getCurrentNumberOfSubtasks() + ")");
	
			// open stub implementation
			stub.open();
	
			boolean moreOuterBlocks = false;
			do {
				// loop over the spilled resettable iterator
				while (innerInput.hasNext()) {
					// get inner pair
					Pair<Key, Value> innerPair = innerInput.next();
					// loop over the pairs in the current memory block
					while (outerInput.hasNext()) {
						// get outer pair
						Pair<Key, Value> outerPair = outerInput.next();
	
						// call cross() method of CrossStub depending on local
						// strategy
						if (config.getLocalStrategy() == LocalStrategy.NESTEDLOOP_BLOCKED_OUTER_SECOND) {
							// call stub with inner pair (first input) and outer
							// pair (second input)
							stub.cross(innerPair.getKey(), innerPair.getValue(), outerPair.getKey(), outerPair.getValue(),
								output);
						} else {
							// call stub with inner pair (second input) and outer
							// pair (first input)
							stub.cross(outerPair.getKey(), outerPair.getValue(), innerPair.getKey(), innerPair.getValue(),
								output);
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
			} while (moreOuterBlocks);
	
			// close stub implementation
			stub.close();
	
		} finally {
			ServiceException se1 = null, se2 = null;
			try {
				if(innerInput != null) innerInput.close();
			} catch (ServiceException se) {
				LOG.warn(se);
				se1 = se;
			}
			try {
				if(outerInput != null) outerInput.close();
			} catch (ServiceException se) {
				LOG.warn(se);
				se2 = se;
			}
			if(se1 != null) throw new RuntimeException("Unable to close SpillingResettableIterator.", se1);
			if(se2 != null) throw new RuntimeException("Unable to close BlockResettableIterator.", se2);
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
			RecordReader<KeyValuePair<Key, Value>> innerReader, final RecordReader<KeyValuePair<Key, Value>> outerReader)
			throws RuntimeException {

		// obtain streaming iterator for outer side
		// streaming is achieved by simply wrapping the input reader of the outer side
		LastRepeatableIterator<KeyValuePair<Key, Value>> outerInput = new LastRepeatableIterator<KeyValuePair<Key, Value>>() {

			SerializationCopier<KeyValuePair<Key, Value>> copier = new SerializationCopier<KeyValuePair<Key,Value>>();
			
			KeyValuePairDeserializer<Key, Value> deserializer = 
				new KeyValuePairDeserializer<Key, Value>(stub.getSecondInKeyType(), stub.getSecondInValueType());
			
			@Override
			public boolean hasNext() {
				return outerReader.hasNext();
			}

			@Override
			public KeyValuePair<Key, Value> next() {
				try {
					KeyValuePair<Key,Value> pair = outerReader.next();
					
					// serialize pair
					copier.setCopy(pair);
					
					return pair;
				} catch (IOException e) {
					throw new RuntimeException(e);
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}

			@Override
			public KeyValuePair<Key, Value> repeatLast() {
				KeyValuePair<Key,Value> pair = deserializer.getInstance();
				copier.getCopy(pair);
				
				return pair;
			}
			
		};

		// obtain SpillingResettableIterator for inner side
		SpillingResettableIterator<KeyValuePair<Key, Value>> innerInput = null;
		
		try {
		
			try {
				innerInput = new SpillingResettableIterator<KeyValuePair<Key, Value>>(memoryManager, ioManager,
					innerReader, MEMORY_IO, new KeyValuePairDeserializer<Key, Value>(stub.getFirstInKeyType(), stub
						.getFirstInValueType()), this);
			} catch (MemoryAllocationException mae) {
				throw new RuntimeException("Unable to obtain SpillingResettable iterator for inner side.", mae);
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
	
			LOG.debug("Resetable iterator obtained: " + this.getEnvironment().getTaskName() + " ("
				+ (this.getEnvironment().getIndexInSubtaskGroup() + 1) + "/"
				+ this.getEnvironment().getCurrentNumberOfSubtasks() + ")");
	
			// open stub implementation
			stub.open();
	
			// read streamed iterator of outer side
			while (outerInput.hasNext()) {
				// get outer pair
				Pair outerPair = outerInput.next();
	
				// read spilled iterator of inner side
				while (innerInput.hasNext()) {
					// get inner pair
					Pair innerPair = innerInput.next();
	
					// call cross() method of CrossStub depending on local
					// strategy
					if (config.getLocalStrategy() == LocalStrategy.NESTEDLOOP_STREAMED_OUTER_SECOND) {
						// call stub with inner pair (first input) and outer pair (second input)
						stub.cross(innerPair.getKey(), innerPair.getValue(), outerPair.getKey(), outerPair.getValue(),
							output);
					} else {
						// call stub with inner pair (second input) and outer pair (first input)
						stub.cross(outerPair.getKey(), outerPair.getValue(), innerPair.getKey(), innerPair.getValue(),
							output);
					}
					
					outerPair = outerInput.repeatLast();
				}
				// reset spilling resettable iterator of inner side
				if(outerInput.hasNext()) {
					innerInput.reset();
				}
			}
	
			// close stub implementation
			stub.close();
	
			// close spilling resettable iterator
		} finally {
			try {
				if(innerInput != null) innerInput.close();
			} catch (ServiceException se) {
				LOG.warn(se);
				throw new RuntimeException("Unable to close SpillingResettable iterator", se);
			}
		}
	}

}
