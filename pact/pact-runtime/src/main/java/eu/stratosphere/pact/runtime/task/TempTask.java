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
import eu.stratosphere.pact.common.io.InputFormat;
import eu.stratosphere.pact.common.stub.Stub;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.runtime.resettable.SpillingResettableIterator;
import eu.stratosphere.pact.runtime.serialization.KeyValuePairDeserializer;
import eu.stratosphere.pact.runtime.task.util.NepheleReaderIterator;
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
public class TempTask extends AbstractTask
{
	// obtain TempTask logger
	private static final Log LOG = LogFactory.getLog(TempTask.class);
	
	// the minimal amount of memory required for the temp to work
	private static final long MIN_REQUIRED_MEMORY = 512 * 1024;

	// input reader
	private RecordReader<KeyValuePair<Key, Value>> reader;

	// output writer
	private RecordWriter<KeyValuePair<Key, Value>> writer;

	// task configuration
	private TaskConfig config;
	
	// spilling thread
	private SpillingResettableIterator<KeyValuePair<Key, Value>> tempIterator;

	// the memory dedicated to the sorter
	private long availableMemory;
	
	// cancel flag
	private volatile boolean taskCanceled = false;

	private Class<Key> keyType;

	private Class<Value> valueType;
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public void registerInputOutput()
	{
		if (LOG.isDebugEnabled())
			LOG.debug(getLogString("Start registering input and output"));

		// init stub of preceding PACT
		initPrecedingStub();

		// init input reader
		initInputReader();

		// init output writer
		initOutputWriter();

		if (LOG.isDebugEnabled())
			LOG.debug(getLogString("Finished registering input and output"));
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void invoke() throws Exception
	{
		// TODO: Replace SpillingResetableIterator by a strategy with a reading
		// and a sending thread.
		// --> A viable solution would be to have the spilling resettable iterator changed to receive and buffer records on the fly.
		// Sending should start while records are read and buffered
		// Order preserving and destroying strategies possible.

		if (LOG.isInfoEnabled())
			LOG.info(getLogString("Start PACT code"));

		// obtain task manager's memory manager
		final MemoryManager memoryManager = getEnvironment().getMemoryManager();
		// obtain task manager's io manager
		final IOManager ioManager = getEnvironment().getIOManager();

		tempIterator = null;
		try {
			// obtain SpillingResettableIterator to dump pairs to disk and read again
			tempIterator = new SpillingResettableIterator<KeyValuePair<Key, Value>>(memoryManager, ioManager, 
					new NepheleReaderIterator<KeyValuePair<Key, Value>>(this.reader),
				this.availableMemory, new KeyValuePairDeserializer<Key, Value>(keyType, valueType),
				this);

			if (LOG.isDebugEnabled())
				LOG.debug(getLogString("Start temping records"));

			// open SpillingResettableIterator
			// all input pairs are consumed and written to disk.
			tempIterator.open();
			
			if (LOG.isDebugEnabled()) {
				LOG.debug(getLogString("Finished temping records"));
				LOG.debug(getLogString("Start serving records"));
			}

			// all read pairs from SpillingResettableIterator (from disk)
			while (!this.taskCanceled && tempIterator.hasNext()) {
				// read next pair
				KeyValuePair<Key, Value> pair = tempIterator.next();
				// forward pair to output writer
				writer.emit(pair);
			}

			if (!this.taskCanceled) {
				if (LOG.isDebugEnabled())
					LOG.debug(getLogString("Finished serving records"));
			}

		}
		catch (MemoryAllocationException mae) {
			throw new RuntimeException("Unable to obtain SpillingResettableIterator for TempTask", mae);
		}
		catch (ServiceException se) {
			throw new RuntimeException(se);
		}
		catch (IOException ioe) {
			throw new RuntimeException(ioe);
		}
		catch (Exception ie) {
			if (!this.taskCanceled) {
				LOG.error("Unexpected ERROR in PACT code: " + this.getEnvironment().getTaskName() + " ("
					+ (this.getEnvironment().getIndexInSubtaskGroup() + 1) + "/"
					+ this.getEnvironment().getCurrentNumberOfSubtasks() + ")");
				throw ie;
			}
		}
		finally {
			if (tempIterator != null) {
				// close SpillingResettableIterator
				tempIterator.close();
			}
		}

		if (!this.taskCanceled) {
			if (LOG.isInfoEnabled())
				LOG.info(getLogString("Finished PACT code"));
		}
		else {
			if (LOG.isWarnEnabled())
				LOG.warn(getLogString("Finished PACT code"));
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
		if (LOG.isWarnEnabled())
			LOG.warn(getLogString("Cancelling PACT code"));
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
		this.config = new TaskConfig(getRuntimeConfiguration());

		// configure io buffer size using task config
		this.availableMemory = this.config.getMemorySize();
		if (this.availableMemory < MIN_REQUIRED_MEMORY) {
			throw new RuntimeException("The temp task was initialized with too little memory: " + this.availableMemory +
				". Required is at least " + MIN_REQUIRED_MEMORY + " bytes.");
		}

		// obtain stub implementation class
		// this is required to obtain the data type of the keys and values.
		// Type are required for serialization and deserialization methods.
		try {
			// obtain stub class
			ClassLoader cl = LibraryCacheManager.getClassLoader(getEnvironment().getJobID());
			Class<?> userClass = config.getStubClass(Object.class, cl);
			if(Stub.class.isAssignableFrom(userClass)) {
				Stub stub = (Stub) userClass.newInstance();
				keyType = stub.getOutKeyType();
				valueType = stub.getOutValueType();
			}
			else if(InputFormat.class.isAssignableFrom(userClass)) {
				InputFormat format = (InputFormat) userClass.newInstance();
				KeyValuePair pair = format.createPair();
				keyType = (Class<Key>) pair.getKey().getClass();
				valueType = (Class<Value>) pair.getValue().getClass();
			} else {
				throw new RuntimeException("Unsupported task type " + userClass);
			}
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
		RecordDeserializer<KeyValuePair<Key, Value>> deserializer = new KeyValuePairDeserializer<Key, Value>(
				keyType, valueType);

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
		this.reader = new RecordReader<KeyValuePair<Key, Value>>(this, deserializer, dp);
	}

	/**
	 * Creates the TempTask's output writer
	 */
	private void initOutputWriter() {
		// obtain output emitter
		OutputEmitter oe = new OutputEmitter(config.getOutputShipStrategy(0));

		// create Writer
		this.writer = new RecordWriter<KeyValuePair<Key, Value>>(this,
			(Class<KeyValuePair<Key, Value>>) (Class<?>) KeyValuePair.class, oe);
	}
	
	// ============================================================================================
	
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
