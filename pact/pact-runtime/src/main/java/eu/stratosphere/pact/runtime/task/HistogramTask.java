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
import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.iomanager.SerializationFactory;
import eu.stratosphere.nephele.services.memorymanager.MemoryAllocationException;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.template.AbstractTask;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.ReduceStub;
import eu.stratosphere.pact.common.stub.Stub;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.common.type.base.PactNull;
import eu.stratosphere.pact.runtime.serialization.KeyValuePairDeserializer;
import eu.stratosphere.pact.runtime.serialization.WritableSerializationFactory;
import eu.stratosphere.pact.runtime.sort.SortMerger;
import eu.stratosphere.pact.runtime.sort.UnilateralSortMerger;
import eu.stratosphere.pact.runtime.task.util.CloseableInputProvider;
import eu.stratosphere.pact.runtime.task.util.OutputCollector;
import eu.stratosphere.pact.runtime.task.util.OutputEmitter;
import eu.stratosphere.pact.runtime.task.util.SimpleCloseableInputProvider;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;

/**
 * COMMENTS: TODO
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class HistogramTask extends AbstractTask {

	// obtain ReduceTask logger
	private static final Log LOG = LogFactory.getLog(HistogramTask.class);

	// the minimal amount of memory for the task to operate
	private static final long MIN_REQUIRED_MEMORY = 3 * 1024 * 1024;

	public static final String NUMBER_OF_BUCKETS = "histogram.buckets.count";
	
	public static final String HISTOGRAM_MEMORY = "histogram.memory.amount";
	
	// input reader
	private CountingRecordReader reader;
	//private RecordReader reader;

	// output collector
	private OutputCollector output;

	// reduce stub implementation instance
	private Stub stub;

	// task config including stub parameters
	private TaskConfig config;
	
	// the memory dedicated to the sorter
	private long availableMemory;
	
	// maximum number of file handles
	private int maxFileHandles;
	
	// the fill fraction of the buffers that triggers the spilling
	private float spillThreshold;
	
	// cancel flag
	private volatile boolean taskCanceled = false;
	
	private int numBuckets;
	// ------------------------------------------------------------------------
	
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
		
		// obtain grouped iterator
		CloseableInputProvider<KeyValuePair<Key, Value>> sortedInputProvider = null;
		try {
			sortedInputProvider = obtainInput();
			
			LOG.debug("Iterator obtained: " + this.getEnvironment().getTaskName() + " ("
				+ (this.getEnvironment().getIndexInSubtaskGroup() + 1) + "/"
				+ this.getEnvironment().getCurrentNumberOfSubtasks() + ")");
	
			// open stub implementation
			//stub.open();
			
			// run stub implementation
			this.callStubWithGroups(sortedInputProvider.getIterator(), output);

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
			if (sortedInputProvider != null) {
				sortedInputProvider.close();
			}
			
			// close stub implementation.
			// when the stub is closed, anything will have been written, so any error will be logged but has no 
			// effect on the successful completion of the task
			try {
				//stub.close();
			}
			catch (Throwable t) {
				LOG.error("Error while closing the Reduce user function " 
					+ this.getEnvironment().getTaskName() + " ("
					+ (this.getEnvironment().getIndexInSubtaskGroup() + 1) + "/"
					+ this.getEnvironment().getCurrentNumberOfSubtasks() + ")", t);
			}
			
			// close output collector
			output.close();
		}
		
		if (this.taskCanceled) {
			LOG.warn("PACT code cancelled: " + this.getEnvironment().getTaskName() + " ("
				+ (this.getEnvironment().getIndexInSubtaskGroup() + 1) + "/"
				+ this.getEnvironment().getCurrentNumberOfSubtasks() + ")");
		} else {
			LOG.info("Finished PACT code: " + this.getEnvironment().getTaskName() + " ("
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
		this.taskCanceled = true;
		LOG.warn("Cancelling PACT code: " + this.getEnvironment().getTaskName() + " ("
			+ (this.getEnvironment().getIndexInSubtaskGroup() + 1) + "/"
			+ this.getEnvironment().getCurrentNumberOfSubtasks() + ")");
	}

	// ------------------------------------------------------------------------
	
	/**
	 * Initializes the stub implementation and configuration.
	 * 
	 * @throws RuntimeException
	 *         Thrown if instance of stub implementation can not be
	 *         obtained.
	 */
	private void initStub() throws RuntimeException {

		// obtain task configuration (including stub parameters)
		config = new TaskConfig(getRuntimeConfiguration());
		
		// set up memory and I/O parameters
		this.availableMemory = config.getMemorySize();
		this.maxFileHandles = config.getNumFilehandles();
		this.spillThreshold = config.getSortSpillingTreshold();
		numBuckets = config.getStubParameters().getInteger(NUMBER_OF_BUCKETS, -1);
		
		// test minimum memory requirements
		long strategyMinMem = 0;
		
		switch (config.getLocalStrategy()) {
			case SORT:
				strategyMinMem = MIN_REQUIRED_MEMORY;
				break;
			case COMBININGSORT: 
				strategyMinMem = MIN_REQUIRED_MEMORY;
				break;
			case NONE:
				strategyMinMem = 0;
				break;
		}
		
		if (this.availableMemory < strategyMinMem) {
			throw new RuntimeException(
					"The Reduce task was initialized with too little memory for local strategy "+
					config.getLocalStrategy()+" : " + this.availableMemory + " bytes." +
				    "Required is at least " + strategyMinMem + " bytes.");
		}

		try {
			// obtain stub implementation class
			ClassLoader cl = LibraryCacheManager.getClassLoader(getEnvironment().getJobID());
			Class<? extends Stub> stubClass = config.getStubClass(Stub.class, cl);
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
	 * Initializes the input reader of the ReduceTask.
	 * 
	 * @throws RuntimeException
	 *         Thrown if no input ship strategy was provided.
	 */
	private void initInputReader() throws RuntimeException {

		// create RecordDeserializer
		RecordDeserializer<KeyValuePair<Key, Value>> deserializer = new KeyValuePairDeserializer(
			stub.getOutKeyType(),
			PactNull.class);

		// determine distribution pattern for reader from input ship strategy
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
			dp = new BipartiteDistributionPattern();
			break;
		default:
			throw new RuntimeException("No input ship strategy provided for ReduceTask.");
		}

		// create reader
		// map has only one input, so we create one reader (id=0).
		reader = new CountingRecordReader(this, deserializer, dp);
		//reader = new RecordReader(this, deserializer, dp);
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
			// TODO smarter decision is possible here, e.g. decide which channel may not need to copy, ...
			output.addWriter(writer, true);
			fwdCopyFlag = true;
			
		}
	}

	/**
	 * Returns an iterator over all k-v pairs of the ReduceTasks input. The
	 * pairs which are returned by the iterator are grouped by their keys.
	 * 
	 * @return A key-grouped iterator over all input key-value pairs.
	 * @throws RuntimeException
	 *         Throws RuntimeException if it is not possible to obtain a
	 *         grouped iterator.
	 */
	private CloseableInputProvider<KeyValuePair<Key, Value>> obtainInput() {
		
		// obtain the MemoryManager of the TaskManager
		final MemoryManager memoryManager = getEnvironment().getMemoryManager();
		// obtain the IOManager of the TaskManager
		final IOManager ioManager = getEnvironment().getIOManager();

		// obtain input key type
		final Class<Key> keyClass = (Class<Key>) stub.getOutKeyType();
		// obtain input value type
		final Class<? extends Value> valueClass = PactNull.class;

		// obtain key serializer
		final SerializationFactory<Key> keySerialization = new WritableSerializationFactory<Key>(keyClass);
		// obtain value serializer
		final SerializationFactory<Value> valSerialization = new WritableSerializationFactory<Value>((Class<Value>) valueClass);

		// obtain grouped iterator defined by local strategy
		switch (config.getLocalStrategy()) {

		// local strategy is NONE
		// input is already grouped, an iterator that wraps the reader is
		// created and returned
		case NONE: {
			//throw new UnsupportedOperationException("Histogram input has to be sorted");
			// iterator wraps input reader
			Iterator<KeyValuePair<Key, Value>> iter = new Iterator<KeyValuePair<Key, Value>>() {

				@Override
				public boolean hasNext() {
					return reader.hasNext();
				}

				@Override
				public KeyValuePair<Key, Value> next() {
					try {
						return (KeyValuePair<Key, Value>) reader.next();
					} catch (Exception e) {
						throw new RuntimeException(e);
					}
				}

				@Override
				public void remove() {
				}

			};
			
			return new SimpleCloseableInputProvider<KeyValuePair<Key,Value>>(iter);
		}

			// local strategy is SORT
			// The input is grouped using a sort-merge strategy.
			// An iterator on the sorted pairs is created and returned.
		case SORT: {
			// create a key comparator
			final Comparator<Key> keyComparator = new Comparator<Key>() {
				@Override
				public int compare(Key k1, Key k2) {
					return k1.compareTo(k2);
				}
			};

			try {
				// instantiate a sort-merger
				SortMerger<Key, Value> sortMerger = new UnilateralSortMerger<Key, Value>(memoryManager, ioManager,
					this.availableMemory, this.maxFileHandles, keySerialization,
					valSerialization, keyComparator, reader, this, this.spillThreshold);
				// obtain and return a grouped iterator from the sort-merger
				return sortMerger;
			} catch (MemoryAllocationException mae) {
				throw new RuntimeException(
					"MemoryManager is not able to provide the required amount of memory for ReduceTask", mae);
			} catch (IOException ioe) {
				throw new RuntimeException("IOException caught when obtaining SortMerger for ReduceTask", ioe);
			}
		}

			// local strategy is COMBININGSORT
			// The Input is grouped using a sort-merge strategy. Before spilling
			// on disk, the data volume is reduced using the combine() method of
			// the ReduceStub.
			// This strategy applies only to those ReduceTasks that have a
			// combining ReduceStub.
			// An iterator on the sorted and grouped pairs is created and
			// returned
//		case COMBININGSORT: {
//			// create a comparator
//			final Comparator<Key> keyComparator = new Comparator<Key>() {
//				@Override
//				public int compare(Key k1, Key k2) {
//					return k1.compareTo(k2);
//				}
//			};
//
//			try {
//				// instantiate a combining sort-merger
//				SortMerger<Key, Value> sortMerger = new CombiningUnilateralSortMerger<Key, Value>(stub, memoryManager,
//					ioManager, this.availableMemory, this.maxFileHandles, keySerialization,
//					valSerialization, keyComparator, reader, this, this.spillThreshold, false);
//				// obtain and return a grouped iterator from the combining
//				// sort-merger
//				return sortMerger;
//			} catch (MemoryAllocationException mae) {
//				throw new RuntimeException(
//					"MemoryManager is not able to provide the required amount of memory for ReduceTask", mae);
//			} catch (IOException ioe) {
//				throw new RuntimeException("IOException caught when obtaining SortMerger for ReduceTask", ioe);
//			}
//		}
		default:
			throw new RuntimeException("Invalid local strategy provided for ReduceTask.");
		}

	}
	
	/**
	 * This method goes over all keys and values that are to be processed by this ReduceTask and calls 
	 * {@link ReduceStub#reduce(Key, Iterator, Collector)} for each key with the key and an iterator over all 
	 * corresponding values. 
	 * 
	 * @param in
	 *        An iterator over all key/value pairs processed by this instance of the reducing code.
	 *        The pairs are grouped by key, such that equal keys are always in a contiguous sequence.
	 * @param out
	 *        The collector to write the results to.
	 */
	private final void callStubWithGroups(Iterator<KeyValuePair<Key, Value>> in, Collector<Key, Value> out) {
		//KeyGroupedIterator<Key, Value> iter = new KeyGroupedIterator<Key, Value>(in);
		boolean run = false;
		while (!this.taskCanceled && in.hasNext()) {
			if(run) {
				throw new RuntimeException("Blubb not working");
			} else {
				run = true;
			}
			
			int recordCount = reader.getCount();
			int bucketSize = recordCount / numBuckets;
			for (int i = 0; i < recordCount; i++) {
				Key value = in.next().getKey();
				if(i%bucketSize == 0 && i/bucketSize != 0 && i/bucketSize != numBuckets) {
					out.collect(value, PactNull.getInstance());
				}
			}
		}
	}
	
	private static class CountingRecordReader extends RecordReader<KeyValuePair<Key, Value>> {
		private int count = 0;
		public CountingRecordReader(AbstractTask taskBase, RecordDeserializer<KeyValuePair<Key, Value>> deserializer,
				DistributionPattern distributionPattern) {
			super(taskBase, deserializer, distributionPattern);
		}
		
		public int getCount() {
			return count;
		}

		@Override
		public KeyValuePair<Key, Value> next() throws IOException, InterruptedException {
			KeyValuePair<Key, Value> pair = super.next();
			if(pair != null) {
				count++;
			}
			return pair;
		}
		
		
		
		
	}
}
