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
import eu.stratosphere.nephele.io.DistributionPattern;
import eu.stratosphere.nephele.io.PointwiseDistributionPattern;
import eu.stratosphere.nephele.io.RecordDeserializer;
import eu.stratosphere.nephele.io.RecordReader;
import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.iomanager.SerializationFactory;
import eu.stratosphere.nephele.services.memorymanager.MemoryAllocationException;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.template.AbstractTask;
import eu.stratosphere.pact.common.stub.ReduceStub;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.runtime.serialization.KeyValuePairDeserializer;
import eu.stratosphere.pact.runtime.serialization.WritableSerializationFactory;
import eu.stratosphere.pact.runtime.sort.AsynchronousPartialSorter;
import eu.stratosphere.pact.runtime.sort.SortMerger;
import eu.stratosphere.pact.runtime.task.util.CloseableInputProvider;
import eu.stratosphere.pact.runtime.task.util.KeyGroupedIterator;
import eu.stratosphere.pact.runtime.task.util.OutputCollector;
import eu.stratosphere.pact.runtime.task.util.OutputEmitter;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;

/**
 * Combine task which is executed by a Nephele task manager.
 * <p>
 * The task is inserted into a PACT program before a ReduceTask. The combine task has a single input and one output. It
 * is provided with a ReduceStub that implemented the <code>combine()</code> method.
 * <p>
 * The CombineTask uses a combining iterator over all key-value pairs of its input. The output of the iterator is
 * emitted.
 * 
 * @see eu.stratosphere.pact.common.stub.ReduceStub
 * @author Fabian Hueske
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class CombineTask extends AbstractTask {

	// obtain CombineTask logger
	private static final Log LOG = LogFactory.getLog(CombineTask.class);

	// the minimal amount of memory for the task to operate
	private static final long MIN_REQUIRED_MEMORY = 1 * 1024 * 1024;
	
	// input reader
	private RecordReader<KeyValuePair<Key, Value>> reader;

	// output collector
	private ReduceStub stub;

	// reduce stub implementation instance
	private TaskConfig config;

	// task config including stub parameters
	private OutputCollector output;
	
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

		// open stub implementation
		initStub();

		// Initialize input reader
		initInputReader();

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

		if (LOG.isDebugEnabled())
			LOG.debug(getLogString("Start obtaining iterator"));
		
		// obtain combining iterator
		CloseableInputProvider<KeyValuePair<Key, Value>> sortedInputProvider = null;
		try {
			sortedInputProvider = obtainInput();
			Iterator<KeyValuePair<Key, Value>> iterator = sortedInputProvider.getIterator();
			KeyGroupedIterator<Key, Value> kgIterator = new KeyGroupedIterator<Key, Value>(iterator);

			if (LOG.isDebugEnabled())
				LOG.debug(getLogString("Iterator obtained"));
			
			ReduceStub stub = this.stub;
			OutputCollector output = this.output;

			// iterate over combined pairs
			while (!this.taskCanceled && kgIterator.nextKey()) {
				stub.combine(kgIterator.getKey(), kgIterator.getValues(), output);
			}
		
		}
		catch (Exception ex) {
			// drop, if the task was canceled
			if (!this.taskCanceled) {
				if (LOG.isErrorEnabled())
					LOG.error(getLogString("Unexpected ERROR in PACT code"));
				throw ex;
			}
		}
		finally {
			if (sortedInputProvider != null) {
				sortedInputProvider.close();
			}
		}
		
		if (!this.taskCanceled) {
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
		this.taskCanceled = true;
		if (LOG.isWarnEnabled())
			LOG.warn(getLogString("Cancelling PACT code"));
	}

	/**
	 * Initializes the stub implementation and configuration.
	 * 
	 * @throws RuntimeException
	 *         Throws if instance of stub implementation can not be obtained.
	 */
	private void initStub() throws RuntimeException {

		// obtain task configuration (including stub parameters)
		this.config = new TaskConfig(getRuntimeConfiguration());

		// set up memory and I/O parameters
		this.availableMemory = config.getMemorySize();
		
		// test minimum memory requirements
		long strategyMinMem = 0;
		
		switch (config.getLocalStrategy()) {
			case COMBININGSORT:
				strategyMinMem = MIN_REQUIRED_MEMORY;
				break;
		}
		
		if (this.availableMemory < strategyMinMem) {
			throw new RuntimeException(
					"The Combine task was initialized with too little memory for local strategy "+
					config.getLocalStrategy()+" : " + this.availableMemory + " bytes." +
				    "Required is at least " + strategyMinMem + " bytes.");
		}

		try {
			// obtain stub implementation class
			ClassLoader cl = LibraryCacheManager.getClassLoader(getEnvironment().getJobID());
			Class<? extends ReduceStub> stubClass = config.getStubClass(ReduceStub.class, cl);
			// obtain stub implementation instance
			stub = stubClass.newInstance();
			// configure stub instance
			stub.configure(config.getStubParameters());
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
	 * Initializes the input reader of the CombineTask.
	 * 
	 * @throws RuntimeException
	 *         Thrown if no input ship strategy was provided.
	 */
	private void initInputReader() {
		// create RecordDeserializer
		RecordDeserializer<KeyValuePair<Key, Value>> deserializer = new KeyValuePairDeserializer(stub.getInKeyType(),
			stub.getInValueType());

		// determine distribution pattern for reader from input ship strategy
		DistributionPattern dp = null;
		switch (config.getInputShipStrategy(0)) {
		case FORWARD:
			// forward requires Pointwise DP
			dp = new PointwiseDistributionPattern();
			break;
		default:
			throw new RuntimeException("No input ship strategy provided for ReduceTask.");
		}

		// create reader
		// map has only one input, so we create one reader (id=0).
		reader = new RecordReader<KeyValuePair<Key, Value>>(this, deserializer, dp);
	}

	/**
	 * Creates an OutputCollector which forwards its input to the combiner's single output writer.
	 */
	private void initOutputCollector() throws RuntimeException {
		// a combiner has only one output with id=0
		if (config.getNumOutputs() != 1) {
			throw new RuntimeException("CombineTask has more than one output");
		}

		// create output collector
		output = new OutputCollector<Key, Value>();
		
		// obtain OutputEmitter from output ship strategy
		OutputEmitter oe = new OutputEmitter(config.getOutputShipStrategy(0));
		// create writer
		RecordWriter<KeyValuePair<Key, Value>> writer = new RecordWriter<KeyValuePair<Key, Value>>(this,
			(Class<KeyValuePair<Key, Value>>) (Class<?>) KeyValuePair.class, oe);
		
		// add writer to output collector
		output.addWriter(writer, false);
	}

	/**
	 * Returns an iterator over all k-v pairs of the CombineTasks input.
	 * The pairs which are returned by the iterator were grouped by their keys and combined using the combine method of
	 * the ReduceStub.
	 * 
	 * @return A iterator that combined all input key-value pairs.
	 * @throws RuntimeException
	 *         Throws RuntimeException if it is not possible to obtain a combined iterator.
	 */
	private CloseableInputProvider<KeyValuePair<Key, Value>> obtainInput() {

		// obtain the MemoryManager of the TaskManager
		final MemoryManager memoryManager = getEnvironment().getMemoryManager();
		// obtain the IOManager of the TaskManager
		final IOManager ioManager = getEnvironment().getIOManager();

		// obtain input key type
		final Class<Key> keyClass = stub.getInKeyType();
		// obtain input value type
		final Class<Value> valueClass = stub.getInValueType();

		// obtain key serializer
		final SerializationFactory<Key> keySerialization = new WritableSerializationFactory<Key>(keyClass);
		// obtain value serializer
		final SerializationFactory<Value> valSerialization = new WritableSerializationFactory<Value>(valueClass);

		// obtain grouped iterator defined by local strategy
		switch (config.getLocalStrategy()) {

		// local strategy is COMBININGSORT
		// The Input is combined using a sort-merge strategy. Before spilling on disk, the data volume is reduced using
		// the combine() method of the ReduceStub.
		// An iterator on the sorted, grouped, and combined pairs is created and returned
		case COMBININGSORT:

			// create a comparator
			final Comparator<Key> keyComparator = new Comparator<Key>() {
				@Override
				public int compare(Key k1, Key k2) {
					return k1.compareTo(k2);
				}
			};

			try {
				// instantiate a combining sort-merger
				SortMerger<Key, Value> sortMerger = new AsynchronousPartialSorter(memoryManager,
					ioManager, this.availableMemory, keySerialization,
					valSerialization, keyComparator, reader, this);
				// obtain and return a grouped iterator from the combining sort-merger
				return sortMerger;
			}
			catch (MemoryAllocationException mae) {
				throw new RuntimeException(
					"MemoryManager is not able to provide the required amount of memory for ReduceTask", mae);
			}
			catch (IOException ioe) {
				throw new RuntimeException("IOException caught when obtaining SortMerger for ReduceTask", ioe);
			}

		default:
			throw new RuntimeException("Invalid local strategy provided for CombineTask.");
		}

	}

	// ------------------------------------------------------------------------
	//                               Utilities
	// ------------------------------------------------------------------------
	
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
