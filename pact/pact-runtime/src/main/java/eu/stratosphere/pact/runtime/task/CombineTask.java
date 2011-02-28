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
import eu.stratosphere.pact.runtime.sort.CombiningUnilateralSortMerger;
import eu.stratosphere.pact.runtime.sort.SortMerger;
import eu.stratosphere.pact.runtime.task.util.CloseableInputProvider;
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

	// number of sort buffers to use
	private int NUM_SORT_BUFFERS;

	// size of each sort buffer in MB
	private int SIZE_SORT_BUFFER;

	// memory to be used for IO buffering
	private int MEMORY_IO;

	// maximum number of file handles
	private int MAX_NUM_FILEHANLDES;

	// obtain CombineTask logger
	private static final Log LOG = LogFactory.getLog(CombineTask.class);

	// input reader
	private RecordReader<KeyValuePair<Key, Value>> reader;

	// output collector
	private ReduceStub stub;

	// reduce stub implementation instance
	private TaskConfig config;

	// task config including stub parameters
	private OutputCollector output;

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void registerInputOutput() {
		LOG.debug("Start registering input and output: " + this.getEnvironment().getTaskName() + " ("
			+ (this.getEnvironment().getIndexInSubtaskGroup() + 1) + "/"
			+ this.getEnvironment().getCurrentNumberOfSubtasks() + ")");

		// open stub implementation
		initStub();

		// Initialize input reader
		initInputReader();

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
	public void invoke() throws Exception 
	{
		LOG.info("Start PACT code: " + this.getEnvironment().getTaskName() + " ("
			+ (this.getEnvironment().getIndexInSubtaskGroup() + 1) + "/"
			+ this.getEnvironment().getCurrentNumberOfSubtasks() + ")");

		LOG.debug("Start obtaining iterator: " + this.getEnvironment().getTaskName() + " ("
			+ (this.getEnvironment().getIndexInSubtaskGroup() + 1) + "/"
			+ this.getEnvironment().getCurrentNumberOfSubtasks() + ")");
		
		// obtain combining iterator
		CloseableInputProvider<KeyValuePair<Key, Value>> sortedInputProvider = null;
		try {
			sortedInputProvider = obtainInput();
			Iterator<KeyValuePair<Key, Value>> iterator = sortedInputProvider.getIterator();

			LOG.debug("Iterator obtained: " + this.getEnvironment().getTaskName() + " ("
				+ (this.getEnvironment().getIndexInSubtaskGroup() + 1) + "/"
				+ this.getEnvironment().getCurrentNumberOfSubtasks() + ")");

			// iterate over combined pairs
			while (iterator.hasNext()) {
				// get next combined pair
				KeyValuePair<Key, Value> pair = iterator.next();
				// output combined pair
				output.collect(pair.getKey(), pair.getValue());
			}

			LOG.info("Finished PACT code: " + this.getEnvironment().getTaskName() + " ("
				+ (this.getEnvironment().getIndexInSubtaskGroup() + 1) + "/"
				+ this.getEnvironment().getCurrentNumberOfSubtasks() + ")");
		}
		finally {
			if (sortedInputProvider != null) {
				sortedInputProvider.close();
			}
		}
	}

	/**
	 * Initializes the stub implementation and configuration.
	 * 
	 * @throws RuntimeException
	 *         Throws if instance of stub implementation can not be obtained.
	 */
	private void initStub() throws RuntimeException {

		// obtain task configuration (including stub parameters)
		config = new TaskConfig(getRuntimeConfiguration());

		// set up memory and io parameters
		NUM_SORT_BUFFERS = config.getNumSortBuffer();
		SIZE_SORT_BUFFER = config.getSortBufferSize() * 1024 * 1024;
		MEMORY_IO = config.getIOBufferSize() * 1024 * 1024;
		MAX_NUM_FILEHANLDES = config.getMergeFactor();

		try {
			// obtain stub implementation class
			ClassLoader cl = LibraryCacheManager.getClassLoader(getEnvironment().getJobID());
			Class<? extends ReduceStub> stubClass = config.getStubClass(ReduceStub.class, cl);
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
				SortMerger<Key, Value> sortMerger = new CombiningUnilateralSortMerger<Key, Value>(stub, memoryManager,
					ioManager, NUM_SORT_BUFFERS, SIZE_SORT_BUFFER, MEMORY_IO, MAX_NUM_FILEHANLDES, keySerialization,
					valSerialization, keyComparator, reader, this, true);
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

}
