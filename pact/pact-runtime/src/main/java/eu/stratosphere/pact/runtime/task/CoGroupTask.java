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
import java.util.ArrayList;
import java.util.List;

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
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.template.AbstractTask;
import eu.stratosphere.nephele.template.IllegalConfigurationException;
import eu.stratosphere.nephele.types.Record;
import eu.stratosphere.pact.common.stub.CoGroupStub;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.runtime.serialization.KeyValuePairDeserializer;
import eu.stratosphere.pact.runtime.sort.SortMergeCoGroupIterator;
import eu.stratosphere.pact.runtime.task.util.CoGroupTaskIterator;
import eu.stratosphere.pact.runtime.task.util.OutputCollector;
import eu.stratosphere.pact.runtime.task.util.OutputEmitter;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;

/**
 * CoGroup task which is executed by a Nephele task manager. The task has two
 * inputs and one or multiple outputs. It is provided with a CoGroupStub
 * implementation.
 * <p>
 * The CoGroupTask group all pairs that share the same key from both inputs. Each for each key, the sets of values that
 * were pair with that key of both inputs are handed to the <code>coGroup()</code> method of the CoGroupStub.
 * 
 * @see eu.stratosphere.pact.common.stub.CoGroupStub
 * @author Fabian Hueske
 */
public class CoGroupTask extends AbstractTask {
	// number of sort buffers to use
	private int NUM_SORT_BUFFERS;

	// size of each sort buffer in MB
	private int SIZE_SORT_BUFFER;

	// memory to be used for IO buffering
	private int MEMORY_IO;

	// maximum number of file handles
	private int MAX_NUM_FILEHANLDES;

	// obtain CoGroupTask logger
	private static final Log LOG = LogFactory.getLog(CoGroupTask.class);

	// reader of first input
	private RecordReader<? extends Record> reader1;

	// reader of second input
	private RecordReader<? extends Record> reader2;

	// output collector
	private OutputCollector<? extends Key, ? extends Value> output;

	// coGroup stub implementation instance
	private CoGroupStub<? extends Key, ? extends Value, ? extends Value, ? extends Key, ? extends Value> coGroup;

	// task config including stub parameters
	private TaskConfig config;
	
	// cancel flag
	private volatile boolean taskCanceled = false;

	// ------------------------------------------------------------------------

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void registerInputOutput() {
		LOG.debug("Start registering input and output: " + this.getEnvironment().getTaskName() + " ("
			+ (this.getEnvironment().getIndexInSubtaskGroup() + 1) + "/"
			+ this.getEnvironment().getCurrentNumberOfSubtasks() + ")");

		// initialize stub implementation
		initStub();

		// initialized input readers
		typedInitReaders(coGroup.getFirstInKeyType(), coGroup.getFirstInValueType(), coGroup.getSecondInValueType());

		// initialize output collector
		typedInitOutputCollector(coGroup.getOutKeyType(), coGroup.getOutValueType());

		LOG.debug("Finished registering input and output: " + this.getEnvironment().getTaskName() + " ("
			+ (this.getEnvironment().getIndexInSubtaskGroup() + 1) + "/"
			+ this.getEnvironment().getCurrentNumberOfSubtasks() + ")");
	}

	/**
	 * {@inheritDoc} This method delegates the processing to the typed variant
	 * {@link #typedInvoke(Class, Class, Class, Class, Class)}.
	 */
	@Override
	public void invoke() throws Exception {
		typedInvoke(coGroup.getFirstInKeyType(), coGroup.getFirstInValueType(), coGroup.getSecondInValueType(), coGroup
			.getOutKeyType(), coGroup.getSecondInValueType());
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.nephele.template.AbstractInvokable#cancel()
	 */
	@Override
	public void cancel() throws Exception
	{
		this.taskCanceled = true;
		LOG.warn("Cancelling PACT code: " + this.getEnvironment().getTaskName() + " ("
			+ (this.getEnvironment().getIndexInSubtaskGroup() + 1) + "/"
			+ this.getEnvironment().getCurrentNumberOfSubtasks() + ")");
	}
	
	// ------------------------------------------------------------------------

	/**
	 * Initializes the stub implementation and configuration. Takes the stub class from the configuration,
	 * loads it and instantiates the stub.
	 * 
	 * @throws RuntimeException
	 *         Thrown, if the instance of stub implementation can not be obtained.
	 */
	private void initStub() throws RuntimeException {
		// obtain task configuration (including stub parameters)
		config = new TaskConfig(getRuntimeConfiguration());

		// set up memory and I/O parameters
		NUM_SORT_BUFFERS = config.getNumSortBuffer();
		SIZE_SORT_BUFFER = config.getSortBufferSize() * 1024 * 1024;
		MEMORY_IO = config.getIOBufferSize() * 1024 * 1024;
		MAX_NUM_FILEHANLDES = config.getMergeFactor();

		try {
			// obtain stub implementation class
			ClassLoader cl = LibraryCacheManager.getClassLoader(getEnvironment().getJobID());

			@SuppressWarnings("unchecked")
			Class<? extends CoGroupStub<? extends Key, ? extends Value, ? extends Value, ? extends Key, ? extends Value>> coGroupClass = (Class<? extends CoGroupStub<? extends Key, ? extends Value, ? extends Value, ? extends Key, ? extends Value>>) config
				.getStubClass(CoGroupStub.class, cl);

			// obtain stub implementation instance
			coGroup = coGroupClass.newInstance();

			// configure stub instance
			coGroup.configure(config.getStubParameters());
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
	 * Returns a CoGroupTaskIterator according to the specified local strategy. The iterator is typed
	 * according to the given classes.
	 * 
	 * @param <K>
	 *        The type of the input key.
	 * @param <V1>
	 *        The type of the first input's value.
	 * @param <V2>
	 *        The type of the second input's value.
	 * @param ikClass
	 *        The class of the input key.
	 * @param iv1Class
	 *        The class of the first input's value.
	 * @param iv2Class
	 *        The class of the second input's value.
	 * @return The iterator implementation for the given local strategy.
	 * @throws IllegalConfigurationException
	 *         Thrown if the local strategy is not supported.
	 */
	private <K extends Key, V1 extends Value, V2 extends Value> CoGroupTaskIterator<K, V1, V2> getIterator(
			Class<K> ikClass, Class<V1> iv1Class, Class<V2> iv2Class)
	{
		// obtain task manager's memory manager
		final MemoryManager memoryManager = getEnvironment().getMemoryManager();
		// obtain task manager's io manager
		final IOManager ioManager = getEnvironment().getIOManager();

		RecordReader<KeyValuePair<K, V1>> reader1 = getReader1();
		RecordReader<KeyValuePair<K, V2>> reader2 = getReader2();

		// create and return MatchTaskIterator according to provided local strategy.
		switch (config.getLocalStrategy()) {
		case SORTMERGE:
			return new SortMergeCoGroupIterator<K, V1, V2>(memoryManager, ioManager, reader1, reader2, ikClass,
				iv1Class, iv2Class, NUM_SORT_BUFFERS, SIZE_SORT_BUFFER, MEMORY_IO, MAX_NUM_FILEHANLDES, this);
		default:
			throw new RuntimeException("Unknown local strategy for CoGroupTask");
		}
	}

	/**
	 * Initializes the record readers. Instantiates the readers and associates the distribution patterns with
	 * the readers.
	 * <p>
	 * This method is typed with the classes of the input key and value types.
	 * 
	 * @param <K>
	 *        The type of the input key.
	 * @param <V1>
	 *        The type of the first input's value.
	 * @param <V2>
	 *        The type of the second input's value.
	 * @param ikClass
	 *        The class of the input key.
	 * @param iv1Class
	 *        The class of the first input's value.
	 * @param iv2Class
	 *        The class of the second input's value.
	 */
	private <K extends Key, V1 extends Value, V2 extends Value> void typedInitReaders(Class<K> ikClass,
			Class<V1> iv1Class, Class<V2> iv2Class) {
		// create RecordDeserializers
		RecordDeserializer<KeyValuePair<K, V1>> deserializer1 = new KeyValuePairDeserializer<K, V1>(ikClass, iv1Class);
		RecordDeserializer<KeyValuePair<K, V2>> deserializer2 = new KeyValuePairDeserializer<K, V2>(ikClass, iv2Class);

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
		default:
			throw new RuntimeException("No input ship strategy provided for second input of CoGroupTask.");
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
		default:
			throw new RuntimeException("No input ship strategy provided for second input of CoGroupTask.");
		}

		// create reader for first input
		reader1 = new RecordReader<KeyValuePair<K, V1>>(this, deserializer1, dp1);
		// create reader for second input
		reader2 = new RecordReader<KeyValuePair<K, V2>>(this, deserializer2, dp2);
	}

	/**
	 * Initializes the output collector, which accepts the produced keys and values and serializes them as
	 * pairs of key and value.
	 * <p>
	 * This method is typed to the stub's output key and value types.
	 * 
	 * @param <OK>
	 *        The type of the output key.
	 * @param <OV>
	 *        The type of the output value.
	 * @param okClass
	 *        The class of the output key.
	 * @param ovClass
	 *        The class of the output value.
	 */
	private <OK extends Key, OV extends Value> void typedInitOutputCollector(Class<OK> okClass, Class<OV> ovClass) {
		
		// create collection for writers
		List<RecordWriter<KeyValuePair<OK, OV>>> writers = new ArrayList<RecordWriter<KeyValuePair<OK, OV>>>();

		// create a writer for each output
		for (int i = 0; i < config.getNumOutputs(); i++) {
			// obtain OutputEmitter from output ship strategy
			OutputEmitter<OK, OV> oe = new OutputEmitter<OK, OV>(config.getOutputShipStrategy(i));
			// create writer

			@SuppressWarnings("unchecked")
			Class<KeyValuePair<OK, OV>> pairClass = (Class<KeyValuePair<OK, OV>>) (Class<?>) KeyValuePair.class;

			RecordWriter<KeyValuePair<OK, OV>> writer = new RecordWriter<KeyValuePair<OK, OV>>(this, pairClass, oe);

			// add writer to collection
			writers.add(writer);
			
		}
		// create collector and register all writers
		// all writers forward copies, except the first one
		// TODO smarter decision is possible here, e.g. decide which channel may not need to copy, ...
		output = new OutputCollector<OK, OV>(writers, (int)(Math.pow(2, writers.size())-1));
	}

	/**
	 * This method runs the pact code for the CoGroup contract. It instantiates the sorters and iterators
	 * and calls the stub logic with each subset of the data.
	 * <p>
	 * This method is typed to the stub's input and output key and value types.
	 * 
	 * @param <IK>
	 *        The type of the input key.
	 * @param <IV1>
	 *        The type of the first input's value.
	 * @param <IV2>
	 *        The type of the second input's value.
	 * @param <OK>
	 *        The type of the output key.
	 * @param <OV>
	 *        The type of the output value.
	 * @param ikClass
	 *        The class of the input key.
	 * @param iv1Class
	 *        The class of the first input's value.
	 * @param iv2Class
	 *        The class of the second input's value.
	 * @param okClass
	 *        The class of the output key.
	 * @param ovClass
	 *        The class of the output value.
	 */
	private <IK extends Key, IV1 extends Value, IV2 extends Value, OK extends Key, OV extends Value> void typedInvoke(
			Class<IK> ikClass, Class<IV1> iv1Class, Class<IV2> iv2Class, Class<OK> okClass, Class<OV> ovClass)
	throws Exception
	{
		LOG.info("Start PACT code: " + this.getEnvironment().getTaskName() + " ("
			+ (this.getEnvironment().getIndexInSubtaskGroup() + 1) + "/"
			+ this.getEnvironment().getCurrentNumberOfSubtasks() + ")");

		final CoGroupStub<IK, IV1, IV2, OK, OV> coGroup = getCoGroupStub();
		final OutputCollector<OK, OV> collector = getOutputCollector();

		CoGroupTaskIterator<IK, IV1, IV2> coGroupIterator = null; 

		try {
			coGroupIterator = getIterator(ikClass, iv1Class, iv2Class);
			
			// open CoGroupTaskIterator
			coGroupIterator.open();
			
			LOG.debug("Iterator obtained: " + this.getEnvironment().getTaskName() + " ("
				+ (this.getEnvironment().getIndexInSubtaskGroup() + 1) + "/"
				+ this.getEnvironment().getCurrentNumberOfSubtasks() + ")");

			LOG.debug("Start processing: " + this.getEnvironment().getTaskName() + " ("
				+ (this.getEnvironment().getIndexInSubtaskGroup() + 1) + "/"
				+ this.getEnvironment().getCurrentNumberOfSubtasks() + ")");

			// open stub implementation
			coGroup.open();
			
			// for each distinct key in both inputs (not necessarily shared by both inputs)
			while (coGroupIterator.next() && !taskCanceled) {
				// call coGroup() method of stub implementation
				coGroup.coGroup(coGroupIterator.getKey(), coGroupIterator.getValues1(), coGroupIterator.getValues2(),
					collector);
			}
		}
		catch (Exception ex) {
			// drop, if the task was canceled
			if (!this.taskCanceled) {
				LOG.error("Unexpected ERROR in PACT code: " + this.getEnvironment().getTaskName() + " ("
					+ (this.getEnvironment().getIndexInSubtaskGroup() + 1) + "/"
					+ this.getEnvironment().getCurrentNumberOfSubtasks() + ")");
				throw ex;
			}
		}
		finally {
			// close CoGroupTaskIterator
			// this is important to release the memory segments
			if (coGroupIterator != null) {
				coGroupIterator.close();
			}
			
			// close stub implementation.
			// when the stub is closed, anything will have been written, so any error will be logged but has no 
			// effect on the successful completion of the task
			try {
				coGroup.close();
			}
			catch (Throwable t) {
				LOG.error("Error while closing the CoGroup user function " 
					+ this.getEnvironment().getTaskName() + " ("
					+ (this.getEnvironment().getIndexInSubtaskGroup() + 1) + "/"
					+ this.getEnvironment().getCurrentNumberOfSubtasks() + ")", t);
			}
			
			// close output collector
			collector.close();
		}

		if(!this.taskCanceled) {
			LOG.info("Finished PACT code: " + this.getEnvironment().getTaskName() + " ("
				+ (this.getEnvironment().getIndexInSubtaskGroup() + 1) + "/"
				+ this.getEnvironment().getCurrentNumberOfSubtasks() + ")");
		} else {
			LOG.warn("PACT code cancelled: " + this.getEnvironment().getTaskName() + " ("
				+ (this.getEnvironment().getIndexInSubtaskGroup() + 1) + "/"
				+ this.getEnvironment().getCurrentNumberOfSubtasks() + ")");
		}
	}

	// ------------------------------------------------------------------------
	//                        Typed access methods
	// ------------------------------------------------------------------------

	@SuppressWarnings("unchecked")
	private final <IK extends Key, IV1 extends Value, IV2 extends Value, OK extends Key, OV extends Value> CoGroupStub<IK, IV1, IV2, OK, OV> getCoGroupStub() {
		return (CoGroupStub<IK, IV1, IV2, OK, OV>) coGroup;
	}

	@SuppressWarnings("unchecked")
	private final <K extends Key, V extends Value> RecordReader<KeyValuePair<K, V>> getReader1() {
		return (RecordReader<KeyValuePair<K, V>>) reader1;
	}

	@SuppressWarnings("unchecked")
	private final <K extends Key, V extends Value> RecordReader<KeyValuePair<K, V>> getReader2() {
		return (RecordReader<KeyValuePair<K, V>>) reader2;
	}

	@SuppressWarnings("unchecked")
	private final <K extends Key, V extends Value> OutputCollector<K, V> getOutputCollector() {
		return (OutputCollector<K, V>) output;
	}
}
