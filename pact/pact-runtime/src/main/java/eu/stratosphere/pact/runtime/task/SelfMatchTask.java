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
import eu.stratosphere.pact.common.stub.MatchStub;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.runtime.resettable.SpillingResettableIterator;
import eu.stratosphere.pact.runtime.serialization.KeyValuePairDeserializer;
import eu.stratosphere.pact.runtime.serialization.ValueDeserializer;
import eu.stratosphere.pact.runtime.serialization.WritableSerializationFactory;
import eu.stratosphere.pact.runtime.sort.SortMerger;
import eu.stratosphere.pact.runtime.sort.UnilateralSortMerger;
import eu.stratosphere.pact.runtime.task.util.CloseableInputProvider;
import eu.stratosphere.pact.runtime.task.util.KeyGroupedIterator;
import eu.stratosphere.pact.runtime.task.util.NepheleReaderIterator;
import eu.stratosphere.pact.runtime.task.util.OutputCollector;
import eu.stratosphere.pact.runtime.task.util.OutputEmitter;
import eu.stratosphere.pact.runtime.task.util.SerializationCopier;
import eu.stratosphere.pact.runtime.task.util.SimpleCloseableInputProvider;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;

/**
 * SelfMatch task which is executed by a Nephele task manager. The task has a
 * single input and one or multiple outputs. It is provided with a MatchStub
 * implementation.
 * <p>
 * The SelfMatchTask creates a iterator over all key-value pairs of its input. 
 * The iterator returns all k-v pairs grouped by their key. A Cartesian product is build 
 * over pairs that share the same key. Each element of these Cartesian products is handed 
 * to the <code>match()</code> method of the MatchStub.
 * 
 * @see eu.stratosphere.pact.common.stub.MatchStub
 * @author Fabian Hueske
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class SelfMatchTask extends AbstractTask {

	// obtain SelfMatchTask logger
	private static final Log LOG = LogFactory.getLog(SelfMatchTask.class);

	// the minimal amount of memory for the task to operate
	private static final long MIN_REQUIRED_MEMORY = 3 * 1024 * 1024;
	
	// share ratio for resettable iterator
	private static final double MEMORY_SHARE_RATIO = 0.10;
	
	// size of value buffer in elements
	private static final int VALUE_BUFFER_SIZE = 10;
	
	// input reader
	private RecordReader<KeyValuePair<Key, Value>> reader;

	// output collector
	private OutputCollector output;

	// match stub implementation instance
	private MatchStub stub;

	// copier for key and values
	private final SerializationCopier<Key> keyCopier = new SerializationCopier<Key>();
	private final SerializationCopier<Value> innerValCopier = new SerializationCopier<Value>();
	
	// serialization factories for key and values
	private SerializationFactory<Key> keySerialization;
	private SerializationFactory<Value> valSerialization;
	
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

	// ------------------------------------------------------------------------
	
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
		
		// obtain grouped iterator
		CloseableInputProvider<KeyValuePair<Key, Value>> sortedInputProvider = null;
		try {
			sortedInputProvider = obtainInput();
			
			if (LOG.isDebugEnabled())
				LOG.debug(getLogString("Iterator obtained"));
	
			// open stub implementation
			stub.open();
			
			// cross pairs with identical keys
			KeyGroupedIterator<Key, Value> it = new KeyGroupedIterator<Key, Value>(sortedInputProvider.getIterator());
			while(!this.taskCanceled && it.nextKey()) {
				// cross all value of a certain key
				crossValues(it.getKey(), it.getValues(), output);
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
			
			// close stub implementation.
			// when the stub is closed, anything will have been written, so any error will be logged but has no 
			// effect on the successful completion of the task
			try {
				stub.close();
			}
			catch (Throwable t) {
				if (LOG.isErrorEnabled())
					LOG.error(getLogString("Error while closing the Match user function"), t);
			}
			// close output collector
			output.close();
		}
		
		if (this.taskCanceled) {
			if (LOG.isWarnEnabled())
				LOG.warn(getLogString("PACT code cancelled"));
		}
		else {
			if (LOG.isInfoEnabled())
				LOG.info(getLogString("Finished PACT code"));
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
		
		// test minimum memory requirements
		long strategyMinMem = 0;
		
		switch (config.getLocalStrategy()) {
			case SORT_SELF_NESTEDLOOP:
				strategyMinMem = MIN_REQUIRED_MEMORY*2;
				break;
			case SELF_NESTEDLOOP: 
				strategyMinMem = MIN_REQUIRED_MEMORY;
				break;
		}
		
		if (this.availableMemory < strategyMinMem) {
			throw new RuntimeException(
					"The SelfMatch task was initialized with too little memory for local strategy "+
					config.getLocalStrategy()+" : " + this.availableMemory + " bytes." +
				    "Required is at least " + strategyMinMem + " bytes.");
		}

		try {
			// obtain stub implementation class
			ClassLoader cl = LibraryCacheManager.getClassLoader(getEnvironment().getJobID());
			Class<? extends MatchStub> stubClass = config.getStubClass(MatchStub.class, cl);
			// obtain stub implementation instance
			stub = stubClass.newInstance();
			// configure stub instance
			config.getStubParameters().setInteger(TASK_ID, getEnvironment().getIndexInSubtaskGroup());
			stub.configure(config.getStubParameters());
			
			// initialize key and value serializer
			this.keySerialization = new WritableSerializationFactory<Key>(stub.getFirstInKeyType());
			this.valSerialization  = new WritableSerializationFactory<Value>(stub.getFirstInValueType());
			
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
	 * Initializes the input reader of the SelfMatchTask.
	 * 
	 * @throws RuntimeException
	 *         Thrown if no input ship strategy was provided.
	 */
	private void initInputReader() throws RuntimeException {

		// create RecordDeserializer
		// we need only one since both inputs are the same
		RecordDeserializer<KeyValuePair<Key, Value>> deserializer = new KeyValuePairDeserializer(stub.getFirstInKeyType(),
			stub.getFirstInValueType());

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
		default:
			throw new RuntimeException("No valid input ship strategy provided for SelfMatchTask: " + 
				config.getInputShipStrategy(0));
		}

		// create reader
		// map has only one input, so we create one reader (id=0).
		reader = new RecordReader<KeyValuePair<Key, Value>>(this, deserializer, dp);
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
			output.addWriter(writer, fwdCopyFlag);
			fwdCopyFlag = true;
			
		}
	}

	/**
	 * Returns an iterator over all k-v pairs of the SelfMatchTasks input. The
	 * pairs which are returned by the iterator are grouped by their keys.
	 * 
	 * @return A key-grouped iterator over all input key-value pairs.
	 * @throws RuntimeException
	 *         Throws RuntimeException if it is not possible to obtain a
	 *         grouped iterator.
	 */
	private CloseableInputProvider<KeyValuePair<Key, Value>> obtainInput()
	{	
		// obtain the MemoryManager of the TaskManager
		final MemoryManager memoryManager = getEnvironment().getMemoryManager();
		// obtain the IOManager of the TaskManager
		final IOManager ioManager = getEnvironment().getIOManager();

		// obtain input key type
		final Class<Key> keyClass = stub.getFirstInKeyType();
		// obtain input value type
		final Class<Value> valueClass = stub.getFirstInValueType();

		// obtain key serializer
		final SerializationFactory<Key> keySerialization = new WritableSerializationFactory<Key>(keyClass);
		// obtain value serializer
		final SerializationFactory<Value> valSerialization = new WritableSerializationFactory<Value>(valueClass);

		// obtain grouped iterator defined by local strategy
		switch (config.getLocalStrategy()) {

		// local strategy is NONE
		// input is already grouped, an iterator that wraps the reader is
		// created and returned
		case SELF_NESTEDLOOP:
			// iterator wraps input reader
			Iterator<KeyValuePair<Key, Value>> iter = new NepheleReaderIterator<KeyValuePair<Key,Value>>(this.reader);			
			return new SimpleCloseableInputProvider<KeyValuePair<Key,Value>>(iter);

			// local strategy is SORT
			// The input is grouped using a sort-merge strategy.
			// An iterator on the sorted pairs is created and returned.
		case SORT_SELF_NESTEDLOOP:
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
					(long)(this.availableMemory * (1.0 - MEMORY_SHARE_RATIO)), this.maxFileHandles, keySerialization,
					valSerialization, keyComparator, reader, this, this.spillThreshold);
				// obtain and return a grouped iterator from the sort-merger
				return sortMerger;
			}
			catch (MemoryAllocationException mae) {
				throw new RuntimeException(
					"MemoryManager is not able to provide the required amount of memory for SelfMatchTask", mae);
			}
			catch (IOException ioe) {
				throw new RuntimeException("IOException caught when obtaining SortMerger for SelfMatchTask", ioe);
			}
			
		default:
			throw new RuntimeException("Invalid local strategy provided for SelfMatchTask: " +
				config.getLocalStrategy());
		}

	}
	
	/**
	 * Crosses the values of all pairs that have the same key.
	 * The {@link MatchStub#match(Key, Iterator, Collector)} method is called for each element of the 
	 * Cartesian product. 
	 * 
	 * @param key 
	 *        The key of all values in the iterator.
	 * @param vals 
	 *        An iterator over values that share the same key.
	 * @param out
	 *        The collector to write the results to.
	 */
	private final void crossValues(Key key, final Iterator<Value> values, final Collector<Key, Value> out)
	{
		// allocate buffer
		final SerializationCopier<Value>[] valBuffer = new SerializationCopier[VALUE_BUFFER_SIZE];
		
		// set key copy
		this.keyCopier.setCopy(key);
		
		Key copyKey;
		Value outerVal;
		Value innerVal;
		
		// fill value buffer for the first time
		int bufferValCnt;
		for(bufferValCnt = 0; bufferValCnt < VALUE_BUFFER_SIZE; bufferValCnt++) {
			if(values.hasNext()) {
				// read value into buffer
				valBuffer[bufferValCnt] = new SerializationCopier<Value>();
				valBuffer[bufferValCnt].setCopy(values.next());
			} else {
				break;
			}
		}
		
		// cross values in buffer
		for (int i = 0;i < bufferValCnt; i++) {
			// check if task was canceled
			if (this.taskCanceled) return;
			
			for (int j = 0; j < bufferValCnt; j++) {
				// check if task was canceled
				if (this.taskCanceled) return;
				
				// get copies of key and values
				copyKey = keySerialization.newInstance();
				this.keyCopier.getCopy(copyKey);
				outerVal = valSerialization.newInstance();
				valBuffer[i].getCopy(outerVal);
				innerVal = valSerialization.newInstance();
				valBuffer[j].getCopy(innerVal);
 
				// match
				stub.match(copyKey, outerVal, innerVal, out);
			}
			
		}
		
		if(!this.taskCanceled && values.hasNext()) {
			// there are still value in the reader

			// wrap value iterator in a reader
			Iterator<Value> valReader = new Iterator<Value>() {

				@Override
				public boolean hasNext() {
					
					if (taskCanceled) 
						return false;
					else
						return values.hasNext();
				}

				@Override
				public Value next() {
						
					// get next value
					Value nextVal = values.next();
					
					// cross with value buffer
					Key copyKey;
					Value outerVal;
					Value innerVal;
					
					// set value copy
					innerValCopier.setCopy(nextVal);
					
					for(int i=0;i<VALUE_BUFFER_SIZE;i++) {
						
						copyKey = keySerialization.newInstance();
						keyCopier.getCopy(copyKey);
						outerVal = valSerialization.newInstance();
						valBuffer[i].getCopy(outerVal);
						innerVal = valSerialization.newInstance();
						innerValCopier.getCopy(innerVal);
						
						stub.match(copyKey,outerVal,innerVal,out);
					}
					
					// return value
					return nextVal;
				}
				
				@Override
				public void remove() {
					throw new UnsupportedOperationException();
				}
			};
			
			SpillingResettableIterator<Value> outerValResettableIterator = null;
			SpillingResettableIterator<Value> innerValResettableIterator = null;
			
			try {
				ValueDeserializer<Value> v1Deserializer = new ValueDeserializer<Value>(stub.getFirstInValueType());
				
				// read values into outer resettable iterator
				outerValResettableIterator = 
						new SpillingResettableIterator<Value>(getEnvironment().getMemoryManager(), getEnvironment().getIOManager(), 
								valReader, (long) (this.availableMemory * (MEMORY_SHARE_RATIO/2)), v1Deserializer, this);
				outerValResettableIterator.open();

				// iterator returns first buffer than outer resettable iterator (all values of the incoming iterator)
				BufferIncludingIterator bii = new BufferIncludingIterator(valBuffer, outerValResettableIterator);
				
				// read remaining values into inner resettable iterator
				if(!this.taskCanceled && outerValResettableIterator.hasNext()) {
					innerValResettableIterator =
						new SpillingResettableIterator<Value>(getEnvironment().getMemoryManager(), getEnvironment().getIOManager(),
								bii, (long) (this.availableMemory * (MEMORY_SHARE_RATIO/2)), v1Deserializer, this);
					innerValResettableIterator.open();
					
					// reset outer iterator
					outerValResettableIterator.reset();
				
					// cross remaining values
					while(!this.taskCanceled && outerValResettableIterator.hasNext()) {
						
						// fill buffer with next elements from outer resettable iterator
						bufferValCnt = 0;
						while(!this.taskCanceled && outerValResettableIterator.hasNext() && bufferValCnt < VALUE_BUFFER_SIZE) {
							valBuffer[bufferValCnt++].setCopy(outerValResettableIterator.next());
						}
						if(bufferValCnt == 0) break;
						
						// cross buffer with inner iterator
						while(!this.taskCanceled && innerValResettableIterator.hasNext()) {
							
							// read inner value
							innerVal = innerValResettableIterator.next();
							
							for(int i=0;i<bufferValCnt;i++) {
								
								// get copies
								copyKey = keySerialization.newInstance();
								keyCopier.getCopy(copyKey);
								outerVal = valSerialization.newInstance();
								valBuffer[i].getCopy(outerVal);
								
								stub.match(copyKey, outerVal, innerVal, out);
								
								if(i < bufferValCnt - 1)
									innerVal = innerValResettableIterator.repeatLast();
							}
						}
						innerValResettableIterator.reset();
					}
				}
				
			} catch (Exception e) {
				throw new RuntimeException(e);
			} finally {
				if(innerValResettableIterator != null) {
					innerValResettableIterator.close();
				}
				if(outerValResettableIterator != null) {
					outerValResettableIterator.close();
				}
			}
			
		}
		
	}
	
	private final class BufferIncludingIterator implements Iterator<Value> {

		int bufferIdx = 0;
		
		private SerializationCopier<Value>[] valBuffer;
		private Iterator<Value> valIterator;
		
		public BufferIncludingIterator(SerializationCopier<Value>[] valBuffer, Iterator<Value> valIterator) {
			this.valBuffer = valBuffer;
			this.valIterator = valIterator;
		}
		
		@Override
		public boolean hasNext() {
			if(taskCanceled) 
				return false;
			
			if(bufferIdx < VALUE_BUFFER_SIZE) 
				return true;
			
			return valIterator.hasNext();
		}

		@Override
		public Value next() {
			if(bufferIdx < VALUE_BUFFER_SIZE) {
				Value outVal = valSerialization.newInstance();
				valBuffer[bufferIdx++].getCopy(outVal);
				return outVal;
			} else {
				return valIterator.next();
			}
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
		
	};
	
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
		bld.append(' ').append('(');
		bld.append(this.getEnvironment().getIndexInSubtaskGroup() + 1);
		bld.append('/');
		bld.append(this.getEnvironment().getCurrentNumberOfSubtasks());
		bld.append(')');
		return bld.toString();
	}
}
