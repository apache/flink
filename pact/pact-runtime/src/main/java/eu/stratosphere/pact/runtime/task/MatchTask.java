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
import java.util.List;

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
import eu.stratosphere.nephele.io.channels.AbstractInputChannel;
import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.iomanager.SerializationFactory;
import eu.stratosphere.nephele.services.memorymanager.MemoryManager;
import eu.stratosphere.nephele.template.AbstractTask;
import eu.stratosphere.nephele.template.IllegalConfigurationException;
import eu.stratosphere.pact.common.stub.MatchStub;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.runtime.hash.HybridHashMatchIterator;
import eu.stratosphere.pact.runtime.hash.InMemoryHashMatchIterator;
import eu.stratosphere.pact.runtime.hash.HybridHashMatchIterator.InputRoles;
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
@SuppressWarnings("unchecked")
public class MatchTask extends AbstractTask {
	// number of sort buffers to use
	private int NUM_SORT_BUFFERS;

	// size of each sort buffer in MB
	private int SIZE_SORT_BUFFER;

	// memory to be used for IO buffering
	private int MEMORY_IO;

	// maximum number of file handles
	private int MAX_NUM_FILEHANLDES;
	
	// share ratio for resettable iterator
	private double MEMORY_SHARE_RATIO = 0.15;

	// obtain MatchTask logger
	private static final Log LOG = LogFactory.getLog(MatchTask.class);

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
	public void invoke() // throws Exception
	{
		LOG.info("Start PACT code: " + this.getEnvironment().getTaskName() + " ("
			+ (this.getEnvironment().getIndexInSubtaskGroup() + 1) + "/"
			+ this.getEnvironment().getCurrentNumberOfSubtasks() + ")");

		// obtain MatchTaskIterator
		final MatchTaskIterator matchIterator = getIterator(reader1, reader2);
		LOG.debug("Iterator obtained: " + this.getEnvironment().getTaskName() + " ("
			+ (this.getEnvironment().getIndexInSubtaskGroup() + 1) + "/"
			+ this.getEnvironment().getCurrentNumberOfSubtasks() + ")");

		// open match stub instance
		matchStub.open();

		try {
			// open MatchTaskIterator
			matchIterator.open();

			// for each distinct key that is contained in both inputs
			while (matchIterator.next()) {
				// call run() method of match stub implementation
				crossValues(matchIterator.getKey(), matchIterator.getValues1(), matchIterator.getValues2());
			}

		} catch (IOException ioe) {
			throw new RuntimeException("Error occured during processing MatchTask", ioe);
		} catch (InterruptedException ie) {
			throw new RuntimeException("Error occured during processing MatchTask", ie);
		} finally {

			// close match stub instance
			matchStub.close();

			// close MatchTaskIterator
			matchIterator.close();

			// close output collector
			output.close();
		}

		LOG.info("Finished PACT code: " + this.getEnvironment().getTaskName() + " ("
			+ (this.getEnvironment().getIndexInSubtaskGroup() + 1) + "/"
			+ this.getEnvironment().getCurrentNumberOfSubtasks() + ")");
	}

	/**
	 * Initializes the stub implementation and configuration.
	 * 
	 * @throws RuntimeException
	 *         Thrown if instance of stub implementation can not be
	 *         obtained.
	 */
	private void initStub() {

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
			Class<? extends MatchStub> matchClass = config.getStubClass(MatchStub.class, cl);
			// obtain stub implementation instance
			matchStub = matchClass.newInstance();
			// configure stub instance
			matchStub.configure(config.getStubParameters());
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
	private MatchTaskIterator getIterator(RecordReader reader1, RecordReader reader2) throws RuntimeException {
		// obtain task manager's memory manager
		final MemoryManager memoryManager = getEnvironment().getMemoryManager();
		// obtain task manager's IO manager
		final IOManager ioManager = getEnvironment().getIOManager();

		// create and return MatchTaskIterator according to provided local strategy.
		switch (config.getLocalStrategy()) {
		case SORTMERGE:
			return new SortMergeMatchIterator(memoryManager, ioManager, reader1, reader2, matchStub.getFirstInKeyType(),
				matchStub.getFirstInValueType(), matchStub.getSecondInValueType(), NUM_SORT_BUFFERS, SIZE_SORT_BUFFER,
				((int)(MEMORY_IO*(1.0-MEMORY_SHARE_RATIO))), MAX_NUM_FILEHANLDES, this);
		case HYBRIDHASH_FIRST:
			return new HybridHashMatchIterator(memoryManager, ioManager, reader1, reader2, matchStub.getFirstInKeyType(),
				matchStub.getFirstInValueType(), matchStub.getSecondInValueType(), InputRoles.BUILD_PROBE, ((int)(MEMORY_IO*(1.0-MEMORY_SHARE_RATIO))));
		case HYBRIDHASH_SECOND:
			return new HybridHashMatchIterator(memoryManager, ioManager, reader1, reader2, matchStub.getFirstInKeyType(),
				matchStub.getFirstInValueType(), matchStub.getSecondInValueType(), InputRoles.PROBE_BUILD, ((int)(MEMORY_IO*(1.0-MEMORY_SHARE_RATIO))));
		case MMHASH_FIRST:
			return new InMemoryHashMatchIterator(reader1, reader2);
		case MMHASH_SECOND:
			return new InMemoryHashMatchIterator(reader2, reader1);
		default:
			throw new RuntimeException("Unknown local strategy for MatchTask");
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

		boolean v1HasNext = values1.hasNext();
		boolean v2HasNext = values2.hasNext();

		// check if one side is already empty
		// this check could be omitted if we put this in MatchTask.
		// then we can derive the local strategy (with build side).
		if (!v1HasNext && !v2HasNext) {
			// both sides contain only one value
			matchStub.match(key, firstV1, firstV2, output);
			return;
		}
		
		Value v1;
		Value v2;
		
		final SerializationFactory<Key> keySerialization = new WritableSerializationFactory<Key>(matchStub.getFirstInKeyType());
		final SerializationCopier<Key> keyCopier = new SerializationCopier<Key>();
		keyCopier.setCopy(key);
		
		if (!v1HasNext) {
			// only values1 contains only one value
			final SerializationFactory<Value> v1Serialization = new WritableSerializationFactory<Value>(matchStub.getFirstInValueType());
			final SerializationCopier<Value> v1Copier = new SerializationCopier<Value>();
			v1Copier.setCopy(firstV1);
			
			matchStub.match(key, firstV1, firstV2, output);
			while (v2HasNext) {
				key = keySerialization.newInstance();
				keyCopier.getCopy(key);
				v1 = v1Serialization.newInstance();
				v1Copier.getCopy(v1);
				
				v2 = values2.next();
				v2HasNext = values2.hasNext();
				matchStub.match(key, v1, v2, output);
			}

		} else if (!v2HasNext) {
			// only values2 contains only one value
			final SerializationFactory<Value> v2Serialization = new WritableSerializationFactory<Value>(matchStub.getSecondInValueType());
			final SerializationCopier<Value> v2Copier = new SerializationCopier<Value>();
			v2Copier.setCopy(firstV2);
			
			matchStub.match(key, firstV1, firstV2, output);
			while (v1HasNext) {
				key = keySerialization.newInstance();
				keyCopier.getCopy(key);
				v2 = v2Serialization.newInstance();
				v2Copier.getCopy(v2);
				
				v1 = values1.next();
				v1HasNext = values1.hasNext();
				matchStub.match(key, v1, v2, output);
			}

		} else {

			// both sides contain more than one value
			// TODO: Decide which side to store!
			
			Reader<Value> v1Reader = new Reader<Value>() {

				boolean firstValue = true;
				
				@Override
				public List<AbstractInputChannel<Value>> getInputChannels() {
					throw new UnsupportedOperationException();
				}

				@Override
				public boolean hasNext() {
					if(firstValue) return true;
					return values1.hasNext();
				}

				@Override
				public Value next() throws IOException, InterruptedException {
					if(firstValue) {
						firstValue = false;
						return firstV1;
					}
					return values1.next();
				}
				
			};
			
			final SpillingResettableIterator<Value> v1ResettableIterator;
			try {
				ValueDeserializer<Value> v1Deserializer = new ValueDeserializer<Value>(matchStub.getFirstInValueType());
				v1ResettableIterator = 
						new SpillingResettableIterator<Value>(getEnvironment().getMemoryManager(), getEnvironment().getIOManager(), 
								v1Reader, ((int)(MEMORY_IO*MEMORY_SHARE_RATIO)), v1Deserializer);
				v1ResettableIterator.open();
				
				final SerializationFactory<Value> v2Serialization = new WritableSerializationFactory<Value>(matchStub.getSecondInValueType());
				final SerializationCopier<Value> v2Copier = new SerializationCopier<Value>();
				v2Copier.setCopy(firstV2);
				
				// run through resettable iterator with firstV2
				while (v1ResettableIterator.hasNext()) {
					key = keySerialization.newInstance();
					keyCopier.getCopy(key);
					v2 = v2Serialization.newInstance();
					v2Copier.getCopy(v2);
					
					v1 = v1ResettableIterator.next();
					matchStub.match(key, v1, v2, output);
				}
				v1ResettableIterator.reset();
				
				// run through resettable iterator for each v2
				while(values2.hasNext()) {
					
					v2 = values2.next();
					v2Copier.setCopy(v2);
					
					while (v1ResettableIterator.hasNext()) {
						key = keySerialization.newInstance();
						keyCopier.getCopy(key);
						v2 = v2Serialization.newInstance();
						v2Copier.getCopy(v2);
						
						v1 = v1ResettableIterator.next();
						matchStub.match(key, v1, v2, output);
					}
					v1ResettableIterator.reset();
				}
				
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
					
		}
	}

}
