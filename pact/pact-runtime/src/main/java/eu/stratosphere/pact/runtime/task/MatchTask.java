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
import eu.stratosphere.nephele.io.Reader;
import eu.stratosphere.nephele.io.RecordDeserializer;
import eu.stratosphere.nephele.io.RecordReader;
import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.iomanager.SerializationFactory;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.template.AbstractTask;
import eu.stratosphere.nephele.template.IllegalConfigurationException;
import eu.stratosphere.pact.common.stub.MatchStub;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.runtime.resettable.BlockResettableIterator;
import eu.stratosphere.pact.runtime.resettable.SpillingResettableIterator;
import eu.stratosphere.pact.runtime.serialization.KeyValuePairDeserializer;
import eu.stratosphere.pact.runtime.serialization.ValueDeserializer;
import eu.stratosphere.pact.runtime.serialization.WritableSerializationFactory;
import eu.stratosphere.pact.runtime.sort.SortMergeMatchIterator;
import eu.stratosphere.pact.runtime.task.util.MatchTaskIterator;
import eu.stratosphere.pact.runtime.task.util.OutputCollector;
import eu.stratosphere.pact.runtime.task.util.OutputEmitter;
import eu.stratosphere.pact.runtime.task.util.SerializationCopier;
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
public class MatchTask extends AbstractTask {
	
	// obtain MatchTask logger
	private static final Log LOG = LogFactory.getLog(MatchTask.class);

	// the minimal amount of memory for the task to operate
	private static final long MIN_REQUIRED_MEMORY = 3 * 1024 * 1024;
	
	// share ratio for resettable iterator
	private static final double MEMORY_SHARE_RATIO = 0.05;
	
	// copier for key and values
	private final SerializationCopier<Key> keyCopier = new SerializationCopier<Key>();
	private final SerializationCopier<Value> v1Copier = new SerializationCopier<Value>();
	private final SerializationCopier<Value> v2Copier = new SerializationCopier<Value>();
	
	

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

	// serialization factories for key and values
	private SerializationFactory<Key> keySerialization;
	private SerializationFactory<Value> v1Serialization;
	private SerializationFactory<Value> v2Serialization;
	
	// the memory dedicated to the sorter
	private long availableMemory;
	
	// maximum number of file handles
	private int maxFileHandles;
	
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
		initInputReaders();

		// initialized output collector
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

		// obtain MatchTaskIterator
		MatchTaskIterator matchIterator = null;
		try {
			matchIterator = getIterator(reader1, reader2);
			
			// open MatchTaskIterator
			matchIterator.open();
			
			LOG.debug("Iterator obtained: " + this.getEnvironment().getTaskName() + " ("
				+ (this.getEnvironment().getIndexInSubtaskGroup() + 1) + "/"
				+ this.getEnvironment().getCurrentNumberOfSubtasks() + ")");

			// open match stub instance
			matchStub.open();
			
			// for each distinct key that is contained in both inputs
			while (matchIterator.next() && !taskCanceled) {
				// call run() method of match stub implementation
				crossValues(matchIterator.getKey(), matchIterator.getValues1(), matchIterator.getValues2());
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
			// close MatchTaskIterator
			if (matchIterator != null) {
				matchIterator.close();
			}
			
			// close stub implementation.
			// when the stub is closed, anything will have been written, so any error will be logged but has no 
			// effect on the successful completion of the task
			try {
				matchStub.close();
			}
			catch (Throwable t) {
				LOG.error("Error while closing the Match user function " 
					+ this.getEnvironment().getTaskName() + " ("
					+ (this.getEnvironment().getIndexInSubtaskGroup() + 1) + "/"
					+ this.getEnvironment().getCurrentNumberOfSubtasks() + ")", t);
			}

			// close output collector
			output.close();
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
	private void initStub() {

		// obtain task configuration (including stub parameters)
		this.config = new TaskConfig(getRuntimeConfiguration());

		// set up memory and I/O parameters
		this.availableMemory = config.getMemorySize();
		this.maxFileHandles = config.getNumFilehandles();
		
		// test minimum memory requirements
		long strategyMinMem = 0;
		
		switch (config.getLocalStrategy()) {
			case SORT_BOTH_MERGE:
				strategyMinMem = MIN_REQUIRED_MEMORY*2;
				break;
			case SORT_FIRST_MERGE: 
				strategyMinMem = MIN_REQUIRED_MEMORY;
				break;
			case SORT_SECOND_MERGE:
				strategyMinMem = MIN_REQUIRED_MEMORY;
				break;
			case MERGE:
				strategyMinMem = MIN_REQUIRED_MEMORY;
				break;
			case HYBRIDHASH_FIRST:
				strategyMinMem = MIN_REQUIRED_MEMORY;
				break;
			case HYBRIDHASH_SECOND:
				strategyMinMem = MIN_REQUIRED_MEMORY;
				break;
			case MMHASH_FIRST:
				strategyMinMem = MIN_REQUIRED_MEMORY;
				break;
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

		try {
			// obtain stub implementation class
			ClassLoader cl = LibraryCacheManager.getClassLoader(getEnvironment().getJobID());
			Class<? extends MatchStub> matchClass = config.getStubClass(MatchStub.class, cl);
			// obtain stub implementation instance
			matchStub = matchClass.newInstance();
			// configure stub instance
			matchStub.configure(config.getStubParameters());
			
			// initialize 
			this.keySerialization = new WritableSerializationFactory<Key>(matchStub.getFirstInKeyType());
			this.v1Serialization  = new WritableSerializationFactory<Value>(matchStub.getFirstInValueType());
			this.v2Serialization  = new WritableSerializationFactory<Value>(matchStub.getSecondInValueType());
			
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
	 * Initializes the input readers of the MatchTask.
	 * 
	 * @throws RuntimeException
	 *         Thrown if no input ship strategy was provided.
	 */
	private void initInputReaders() {

		// create RecordDeserializers
		RecordDeserializer<KeyValuePair<Key, Value>> deserializer1 = new KeyValuePairDeserializer(matchStub
			.getFirstInKeyType(), matchStub.getFirstInValueType());
		RecordDeserializer<KeyValuePair<Key, Value>> deserializer2 = new KeyValuePairDeserializer(matchStub
			.getSecondInKeyType(), matchStub.getSecondInValueType());

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
		reader1 = new RecordReader<KeyValuePair<Key, Value>>(this, deserializer1, dp1);
		// create reader for second input
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
			// TODO smarter decision is possible here, e.g. decide which channel may not need to copy, ...
			output.addWriter(writer, fwdCopyFlag);
			fwdCopyFlag = true;
			
		}
		
	}

	/**
	 * Returns a MatchTaskIterator according to the specified local strategy.
	 * 
	 * @param reader1
	 *        Input reader of the first input.
	 * @param reader2
	 *        Input reader of the second input.
	 * @return MatchTaskIterator The iterator implementation for the given local strategy.
	 * @throws IllegalConfigurationException
	 *         Thrown if the local strategy is not supported.
	 */
	private MatchTaskIterator getIterator(RecordReader reader1, RecordReader reader2) {
		// obtain task manager's memory manager
		final MemoryManager memoryManager = getEnvironment().getMemoryManager();
		// obtain task manager's IO manager
		final IOManager ioManager = getEnvironment().getIOManager();

		// create and return MatchTaskIterator according to provided local strategy.
		switch (config.getLocalStrategy()) {
		case SORT_BOTH_MERGE:
			return new SortMergeMatchIterator(memoryManager, ioManager, reader1, reader2, matchStub.getFirstInKeyType(),
				matchStub.getFirstInValueType(), matchStub.getSecondInValueType(),
				(long)(this.availableMemory * (1.0 - MEMORY_SHARE_RATIO)), this.maxFileHandles, config.getLocalStrategy(), this);
		case SORT_FIRST_MERGE:
			return new SortMergeMatchIterator(memoryManager, ioManager, reader1, reader2, matchStub.getFirstInKeyType(),
				matchStub.getFirstInValueType(), matchStub.getSecondInValueType(),
				(long)(this.availableMemory * (1.0 - MEMORY_SHARE_RATIO)), this.maxFileHandles, config.getLocalStrategy(), this);
		case SORT_SECOND_MERGE:
			return new SortMergeMatchIterator(memoryManager, ioManager, reader1, reader2, matchStub.getFirstInKeyType(),
				matchStub.getFirstInValueType(), matchStub.getSecondInValueType(),
				(long)(this.availableMemory * (1.0 - MEMORY_SHARE_RATIO)), this.maxFileHandles, config.getLocalStrategy(), this);
		case MERGE:
			return new SortMergeMatchIterator(memoryManager, ioManager, reader1, reader2, matchStub.getFirstInKeyType(),
				matchStub.getFirstInValueType(), matchStub.getSecondInValueType(),
				(long)(this.availableMemory * (1.0 - MEMORY_SHARE_RATIO)), this.maxFileHandles, config.getLocalStrategy(), this);
//		case HYBRIDHASH_FIRST:
//			return new HybridHashMatchIterator(memoryManager, ioManager, reader1, reader2, matchStub.getFirstInKeyType(),
//				matchStub.getFirstInValueType(), matchStub.getSecondInValueType(), InputRoles.BUILD_PROBE, 
//				((int)(MEMORY_IO*(1.0-MEMORY_SHARE_RATIO))), this);
//		case HYBRIDHASH_SECOND:
//			return new HybridHashMatchIterator(memoryManager, ioManager, reader1, reader2, matchStub.getFirstInKeyType(),
//				matchStub.getFirstInValueType(), matchStub.getSecondInValueType(), InputRoles.PROBE_BUILD,
//				((int)(MEMORY_IO*(1.0-MEMORY_SHARE_RATIO))), this);
//		case MMHASH_FIRST:
//			return new InMemoryHashMatchIterator(reader1, reader2);
//		case MMHASH_SECOND:
//			return new InMemoryHashMatchIterator(reader2, reader1);
		default:
			throw new RuntimeException("Unsupported local strategy for MatchTask: "+config.getLocalStrategy());
		}
	}
	
	
	/**
	 * <p>
	 * Calls the <code>MatchStub#match()</code> method for all two
	 * key-value pairs that share the same key and come from different inputs. The output of the <code>match()</code>
	 * method is forwarded.
	 * <p>
	 * This method is called with a key and two iterator (one for each input) over all values that share this key.
	 * <p>
	 * 
	 * @param key
	 *        A key.
	 * @param values1
	 *        An iterator on values of the first input that were paired with the key.
	 * @param values2
	 *        An iterator on values of the second input that were paired with the key.
	 * @param out
	 *        A collector that collects all output pairs.
	 */
	private void crossValues(Key key, final Iterator<Value> values1, final Iterator<Value> values2) throws RuntimeException {

		// get first value of each side
		final Value firstV1 = values1.next();
		final Value firstV2 = values2.next();
		
		if (firstV1 == null || firstV2 == null) {
			return;
		}
		
		final boolean v1HasNext = values1.hasNext();
		final boolean v2HasNext = values2.hasNext();

		// check if one side is already empty
		// this check could be omitted if we put this in MatchTask.
		// then we can derive the local strategy (with build side).
		if (!v1HasNext && !v2HasNext) {
			// both sides contain only one value
			matchStub.match(key, firstV1, firstV2, output);
			return;
		}
		
		if (!this.taskCanceled && !v1HasNext) {
			// only values1 contains only one value
			ValueIncludingIterator v2Iterator = new ValueIncludingIterator(firstV2, values2);
			cross1withNValues(key, firstV1, v2Iterator, false);

		} else if (!this.taskCanceled && !v2HasNext) {
			// only values2 contains only one value
			ValueIncludingIterator v1Iterator = new ValueIncludingIterator(firstV1, values1);
			cross1withNValues(key, firstV2, v1Iterator, true);

		} else {
			// both sides contain more than one value
			ValueIncludingReader v1Reader = new ValueIncludingReader(firstV1, values1);
			ValueIncludingReader v2Reader = new ValueIncludingReader(firstV2, values2);

			// TODO: Decide which side to spill and which to block!
			crossMwithNValues(key, v2Reader, v1Reader, true);
		}
	}
	
	/**
	 * Crosses a single value with N values all sharing a common key.
	 * 
	 * @param key 
	 * 			The key shared by all values
	 * @param val1
	 *          The single value
	 * @param valsN
	 *          Iterator over N values
	 * @param firstInputNValues
	 *          Set to true if the first input in N-value side, false otherwise.
	 *          
	 * @throws RuntimeException
	 *          Forwards all exceptions thrown by the stub.
	 */
	private void cross1withNValues(Key key, Value val1, Iterator<Value> valsN, final boolean firstInputNValues) throws RuntimeException {
		
		Value v1;
		Value vN;
		
		// set copies
		keyCopier.setCopy(key);
		this.v1Copier.setCopy(val1);
		
		// for each of N values
		while (!this.taskCanceled && valsN.hasNext()) {
			
			// get key copy
			key = this.keySerialization.newInstance();
			this.keyCopier.getCopy(key);
		
			// get N value
			vN = valsN.next();
			
			if(firstInputNValues) {
				// get value copy
				v1 = this.v2Serialization.newInstance();
				this.v1Copier.getCopy(v1);
				// match
				matchStub.match(key, vN, v1, output);
			} else {
				// get value copy
				v1 = this.v1Serialization.newInstance();
				this.v1Copier.getCopy(v1);
				// match
				matchStub.match(key, v1, vN, output);
			}
			
		}
		
	}
	
	private void crossMwithNValues(Key key, Reader<Value> blockVals, Reader<Value> spillVals, final boolean spillFirstInput) throws RuntimeException {
		
		Value spillVal;
		Value blockVal;
		
		keyCopier.setCopy(key);
		
		SpillingResettableIterator<Value> spillIt = null;
		BlockResettableIterator<Value> blockIt = null;
		
		try {
			// create block iterator on second input
			
			ValueDeserializer<Value> v2Deserializer = new ValueDeserializer<Value>(matchStub.getSecondInValueType());
			blockIt = new BlockResettableIterator<Value>(getEnvironment().getMemoryManager(), 
					blockVals, (long)(this.availableMemory * (MEMORY_SHARE_RATIO/2)), 2, v2Deserializer, this);
			blockIt.open();
			
			// TODO: check if v2 has more than one block to read from
			
			// create spilling iterator on first input
			
			ValueDeserializer<Value> v1Deserializer = new ValueDeserializer<Value>(matchStub.getFirstInValueType());
			spillIt = 
					new SpillingResettableIterator<Value>(getEnvironment().getMemoryManager(), getEnvironment().getIOManager(), 
							spillVals, (long) (this.availableMemory * (MEMORY_SHARE_RATIO/2)), v1Deserializer, this);
			spillIt.open();
			
			// as long as there are blocks of second input 
			do {
				
				// cross block values (second input) with resettable values (first input)
				while(!this.taskCanceled && spillIt.hasNext()) {
					
					// get value from resettable iterator
					spillVal = spillIt.next();
					
					// cross value with block values
					while(!this.taskCanceled && blockIt.hasNext()) {
						
						// get instances of key and block value
						key = this.keySerialization.newInstance();
						this.keyCopier.getCopy(key);
						blockVal = blockIt.next();
						
						// match
						if(spillFirstInput) {
							matchStub.match(key, spillVal, blockVal, output);
						} else {
							matchStub.match(key, blockVal, spillVal, output);
						}
						
						// get new instance of resettable value
						if(blockIt.hasNext())
							spillVal = spillIt.repeatLast();
					}
					// reset block iterator
					blockIt.reset();
				}
				// reset v1 iterator
				spillIt.reset();
				
				// move to next block
			} while (!this.taskCanceled && blockIt.nextBlock());
			
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			if(blockIt != null) {
				blockIt.close();
			}
			if(spillIt != null) {
				spillIt.close();
			}
		}
		
	}
	
	private static final class ValueIncludingReader implements Reader<Value> {

		private Value v;
		private Iterator<Value> it;
		private boolean first = true;
		
		public ValueIncludingReader(Value v, Iterator<Value> it) {
			this.v = v;
			this.it = it;
		}
		
		@Override
		public boolean hasNext() {
			if(first) 
				return true;
			else
				return it.hasNext();
		}

		@Override
		public Value next() throws IOException, InterruptedException {
			if(first) {
				first = false;
				return v;
			} else {
				return it.next();
			}
				
		}
	}
	
	private static final class ValueIncludingIterator implements Iterator<Value> {

		private Value v;
		private Iterator<Value> it;
		private boolean first = true;
		
		public ValueIncludingIterator(Value v, Iterator<Value> it) {
			this.v = v;
			this.it = it;
		}
		
		@Override
		public boolean hasNext() {
			if(first) 
				return true;
			else
				return it.hasNext();
		}

		@Override
		public Value next() {
			if(first) {
				first = false;
				return v;
			} else {
				return it.next();
			}
				
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
	}

}
