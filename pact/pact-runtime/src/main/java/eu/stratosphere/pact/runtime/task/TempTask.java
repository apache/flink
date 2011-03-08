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
import eu.stratosphere.pact.common.stub.Stub;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.runtime.resettable.SpillingResettableIterator;
import eu.stratosphere.pact.runtime.serialization.KeyValuePairDeserializer;
import eu.stratosphere.pact.runtime.task.util.OutputEmitter;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;

/**
 * Temp task which is executed by a Nephele task manager. The task has a single
 * input and one outputs.
 * <p>
 * The TempTask collects all pairs from its input and dumps them on disk. After all pairs have been read and dumped,
 * they are read from disk and forwarded. The TempTask is automatically inserted by the PACT Compiler to avoid deadlocks
 * in Nepheles dataflow.
 * 
 * @author Fabian Hueske
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class TempTask extends AbstractTask {

	// memory to be used for IO buffering
	public int MEMORY_IO;

	// obtain TempTask logger
	private static final Log LOG = LogFactory.getLog(TempTask.class);

	// input reader
	private RecordReader<KeyValuePair<Key, Value>> reader;

	// output writer
	private RecordWriter<KeyValuePair<Key, Value>> writer;

	// stub implementation of preceding PACT
	Stub stub;

	// task configuration
	private TaskConfig config;
	
	// spilling thread
	SpillingResettableIterator<KeyValuePair<Key, Value>> tempIterator;

	// cancel flag
	private volatile boolean taskCanceled = false;
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public void registerInputOutput() {
		LOG.debug("Start registering input and output: " + this.getEnvironment().getTaskName() + " ("
			+ (this.getEnvironment().getIndexInSubtaskGroup() + 1) + "/"
			+ this.getEnvironment().getCurrentNumberOfSubtasks() + ")");

		// init stub of preceding PACT
		initPrecedingStub();

		// init input reader
		initInputReader();

		// init output writer
		initOutputWriter();

		LOG.debug("Finished registering input and output: " + this.getEnvironment().getTaskName() + " ("
			+ (this.getEnvironment().getIndexInSubtaskGroup() + 1) + "/"
			+ this.getEnvironment().getCurrentNumberOfSubtasks() + ")");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void invoke() throws Exception {
		// TODO: Replace SpillingResetableIterator by a strategy with a reading
		// and a sending thread.
		// Sending should start while records are read and buffered
		// Order preserving and destroying strategies possible.

		LOG.info("Start PACT code: " + this.getEnvironment().getTaskName() + " ("
			+ (this.getEnvironment().getIndexInSubtaskGroup() + 1) + "/"
			+ this.getEnvironment().getCurrentNumberOfSubtasks() + ")");

		// obtain task manager's memory manager
		final MemoryManager memoryManager = getEnvironment().getMemoryManager();
		// obtain task manager's io manager
		final IOManager ioManager = getEnvironment().getIOManager();

		tempIterator = null;
		try {
			// obtain SpillingResettableIterator to dump pairs to disk and read again
			tempIterator = new SpillingResettableIterator<KeyValuePair<Key, Value>>(memoryManager, ioManager, reader,
				MEMORY_IO, new KeyValuePairDeserializer<Key, Value>(stub.getOutKeyType(), stub.getOutValueType()),
				this);

			LOG.debug("Start temping records: " + this.getEnvironment().getTaskName() + " ("
				+ (this.getEnvironment().getIndexInSubtaskGroup() + 1) + "/"
				+ this.getEnvironment().getCurrentNumberOfSubtasks() + ")");

			// open SpillingResettableIterator
			// all input pairs are consumed and written to disk.
			tempIterator.open();
			
			LOG.debug("Finished temping records: " + this.getEnvironment().getTaskName() + " ("
				+ (this.getEnvironment().getIndexInSubtaskGroup() + 1) + "/"
				+ this.getEnvironment().getCurrentNumberOfSubtasks() + ")");

			LOG.debug("Start serving records: " + this.getEnvironment().getTaskName() + " ("
				+ (this.getEnvironment().getIndexInSubtaskGroup() + 1) + "/"
				+ this.getEnvironment().getCurrentNumberOfSubtasks() + ")");

			// all read pairs from SpillingResettableIterator (from disk)
			while (tempIterator.hasNext() && !this.taskCanceled) {
				// read next pair
				KeyValuePair<Key, Value> pair = tempIterator.next();
				// forward pair to output writer
				writer.emit(pair);
			}

			if(!this.taskCanceled) {
				LOG.debug("Finished serving records: " + this.getEnvironment().getTaskName() + " ("
					+ (this.getEnvironment().getIndexInSubtaskGroup() + 1) + "/"
					+ this.getEnvironment().getCurrentNumberOfSubtasks() + ")");
			}

		} catch (MemoryAllocationException mae) {
			throw new RuntimeException("Unable to obtain SpillingResettableIterator for TempTask", mae);
		} catch (ServiceException se) {
			throw new RuntimeException(se);
		} catch (IOException ioe) {
			throw new RuntimeException(ioe);
		} catch (Exception ie) {
			if(!this.taskCanceled) {
				LOG.error("Unexpected ERROR in PACT code: " + this.getEnvironment().getTaskName() + " ("
					+ (this.getEnvironment().getIndexInSubtaskGroup() + 1) + "/"
					+ this.getEnvironment().getCurrentNumberOfSubtasks() + ")");
				throw ie;
			}
		} finally {

			// close SpillingResettableIterator
			tempIterator.close();
		}

		if(!this.taskCanceled) {
			LOG.info("Finished PACT code: " + this.getEnvironment().getTaskName() + " ("
				+ (this.getEnvironment().getIndexInSubtaskGroup() + 1) + "/"
				+ this.getEnvironment().getCurrentNumberOfSubtasks() + ")");
		} else {
			LOG.warn("Finished PACT code: " + this.getEnvironment().getTaskName() + " ("
				+ (this.getEnvironment().getIndexInSubtaskGroup() + 1) + "/"
				+ this.getEnvironment().getCurrentNumberOfSubtasks() + ")");
		}
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.nephele.template.AbstractInvokable#cancel()
	 */
	@Override
	public void cancel() throws Exception
	{
		// activate cancel flag
		this.taskCanceled = true;
		// abort temp iterator
		if(tempIterator != null) {
			tempIterator.abort();
		}
		LOG.warn("Cancelling PACT code: " + this.getEnvironment().getTaskName() + " ("
			+ (this.getEnvironment().getIndexInSubtaskGroup() + 1) + "/"
			+ this.getEnvironment().getCurrentNumberOfSubtasks() + ")");
	}

	/**
	 * Initializes the stub implementation of the preceding PACT.
	 * The stub is required to obtain the types of its output key and value.
	 * The types are necessary for serialization and deserialization of key-value pairs.
	 * 
	 * @throws RuntimeException
	 *         Thrown if instance of stub implementation can not be
	 *         obtained.
	 */
	private void initPrecedingStub() throws RuntimeException {

		// obtain task configuration
		config = new TaskConfig(getRuntimeConfiguration());

		// configure io buffer size using task config
		MEMORY_IO = config.getIOBufferSize() * 1024 * 1024;

		// obtain stub implementation class
		// this is required to obtain the data type of the keys and values.
		// Type are required for serialization and deserialization methods.
		ClassLoader cl;
		try {
			// obtain stub class
			cl = LibraryCacheManager.getClassLoader(getEnvironment().getJobID());
			Class<? extends Stub> stubClass = config.getStubClass(Stub.class, cl);
			// obtain stub instance
			stub = stubClass.newInstance();
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
	 * Initializes the input reader of the TempTask.
	 * 
	 * @throws RuntimeException
	 *         Thrown if an invalid input ship strategy was provided.
	 */
	private void initInputReader() {
		// create RecordDeserializer
		RecordDeserializer<KeyValuePair<Key, Value>> deserializer = new KeyValuePairDeserializer<Key, Value>(stub
			.getOutKeyType(), stub.getOutValueType());

		// determine distribution pattern from ship strategy
		DistributionPattern dp = null;
		switch (config.getInputShipStrategy(0)) {
		case FORWARD:
			// forward requires Pointwise DP
			dp = new PointwiseDistributionPattern();
			break;
		case PARTITION_HASH:
			// partition requires Bipartite DP
			dp = new BipartiteDistributionPattern();
			break;
		case BROADCAST:
			// broadcast requires Bipartite DP
			dp = new BipartiteDistributionPattern();
			break;
		case SFR:
			// SFR requires Bipartite DP
			dp = new BipartiteDistributionPattern();
			break;
		default:
			throw new RuntimeException("Unsupported ship strategy in TempTask.");
		}

		// create reader
		reader = new RecordReader<KeyValuePair<Key, Value>>(this, deserializer, dp);
	}

	/**
	 * Creates the TempTask's output writer
	 */
	private void initOutputWriter() {
		// obtain output emitter
		OutputEmitter oe = new OutputEmitter(config.getOutputShipStrategy(0));

		// create Writer
		writer = new RecordWriter<KeyValuePair<Key, Value>>(this,
			(Class<KeyValuePair<Key, Value>>) (Class<?>) KeyValuePair.class, oe);
	}
}
