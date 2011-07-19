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
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.nephele.execution.librarycache.LibraryCacheManager;
import eu.stratosphere.nephele.io.BipartiteDistributionPattern;
import eu.stratosphere.nephele.io.ChannelSelector;
import eu.stratosphere.nephele.io.DistributionPattern;
import eu.stratosphere.nephele.io.PointwiseDistributionPattern;
import eu.stratosphere.nephele.io.RecordDeserializer;
import eu.stratosphere.nephele.io.RecordReader;
import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.nephele.template.AbstractTask;
import eu.stratosphere.pact.common.contract.DataDistribution;
import eu.stratosphere.pact.common.contract.Order;
import eu.stratosphere.pact.common.io.InputFormat;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.MapStub;
import eu.stratosphere.pact.common.stub.Stub;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.common.type.base.PactNull;
import eu.stratosphere.pact.runtime.serialization.KeyValuePairDeserializer;
import eu.stratosphere.pact.runtime.task.util.HistogramPartitionFunction;
import eu.stratosphere.pact.runtime.task.util.OutputCollector;
import eu.stratosphere.pact.runtime.task.util.OutputEmitter;
import eu.stratosphere.pact.runtime.task.util.PartitionFunction;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;

/**
 * Map task which is executed by a Nephele task manager. The task has a single
 * input and one or multiple outputs. It is provided with a MapStub
 * implementation.
 * <p>
 * The MapTask creates an iterator over all key-value pairs of its input and hands that 
 * to the <code>map()</code> method of the MapStub.
 * 
 * @see eu.stratosphere.pact.common.stub.MapStub
 * @author Fabian Hueske
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class PartitionTask extends AbstractTask {

	// obtain MapTask logger
	private static final Log LOG = LogFactory.getLog(PartitionTask.class);
	
	public static final String GLOBAL_PARTITIONING_ORDER = "partitioning.order";

	public static final String DATA_DISTRIBUTION_CLASS = "partitioning.distribution.class";

	public static final String PARTITION_BY_SAMPLING = "partitioning.sampling";
	
	public static final String NUMBER_OF_PARTITIONS = "partitioning.partitions.count";

	// input reader
	private RecordReader<KeyValuePair<Key, Value>> readerPartition;
	private RecordReader<KeyValuePair<Key, Value>> readerData;

	// output collector
	private OutputCollector<Key, Value> output;

	// task configuration (including stub parameters)
	private TaskConfig config;
	
	private OutputEmitter oe;

	// cancel flag
	private volatile boolean taskCanceled = false;
	
	private Class<Key> keyType;
	
	private Class<Value> valueType;
	
	// partitioning function
	private PartitionFunction func;
	
	// order used for partitioning
	private Order order;
	
	private boolean usesSample;
	
	private Key[] splitBorders;
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public void registerInputOutput() {
		LOG.debug("Start registering input and output: " + this.getEnvironment().getTaskName() + " ("
			+ (this.getEnvironment().getIndexInSubtaskGroup() + 1) + "/"
			+ this.getEnvironment().getCurrentNumberOfSubtasks() + ")");
		
		// Initialize stub implementation
		initPartitioner();

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
	public void invoke() throws Exception {

		LOG.info("Start PACT code: " + this.getEnvironment().getTaskName() + " ("
			+ (this.getEnvironment().getIndexInSubtaskGroup() + 1) + "/"
			+ this.getEnvironment().getCurrentNumberOfSubtasks() + ")");

		if(usesSample) {
			//Read partition and assign to OutputEmitter
			readerPartition.hasNext();
			
			ArrayList<Key> borders = new ArrayList<Key>();
			while(readerPartition.hasNext()) {
				borders.add(readerPartition.next().getKey());
			}
			
			splitBorders = borders.toArray(new Key[borders.size()]);
		}
		
		//Create partition function based on histogram
		func = new HistogramPartitionFunction(splitBorders, order);
		
		//Assign partition function to output emitter
		for (RecordWriter<KeyValuePair<Key, Value>> recWriter : output.getWriters()) {
			ChannelSelector<KeyValuePair<Key, Value>> selector = 
				recWriter.getOutputGate().getChannelSelector();
			if(selector instanceof OutputEmitter) {
				((OutputEmitter) selector).setPartitionFunction(func);
			}
		}
		
		/**
		 * Iterator over all input key-value pairs. The iterator wraps the input
		 * reader of the Nepehele task.
		 */
		Iterator<KeyValuePair<Key, Value>> input = new Iterator<KeyValuePair<Key, Value>>() {

			public boolean hasNext() {
				return readerData.hasNext();
			}

			@Override
			public KeyValuePair<Key, Value> next() {
				try {
					return readerData.next();
				} catch (IOException e) {
					throw new RuntimeException(e);
				} catch (InterruptedException e) {
					throw new RuntimeException(e);
				}
			}

			@Override
			public void remove() {

			}
		};

		try {
			// run stub implementation
			callStub(input, output);
		} catch (Exception ex) {
			// drop, if the task was canceled
			if (!this.taskCanceled) {
				LOG.error("Unexpected ERROR in PACT code: " + this.getEnvironment().getTaskName() + " ("
					+ (this.getEnvironment().getIndexInSubtaskGroup() + 1) + "/"
					+ this.getEnvironment().getCurrentNumberOfSubtasks() + ")");
				throw ex;
			}
		}
		// close output collector
		output.close();

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
	public void cancel() throws Exception {
		this.taskCanceled = true;
		LOG.warn("Cancelling PACT code: " + this.getEnvironment().getTaskName() + " ("
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
	private void initPartitioner() throws RuntimeException {
		
		// obtain task configuration (including stub parameters)
		config = new TaskConfig(getRuntimeConfiguration());

		order = Order.valueOf(config.getStubParameters().getString(GLOBAL_PARTITIONING_ORDER, ""));
		usesSample = config.getStubParameters().getBoolean(PARTITION_BY_SAMPLING, true);
		
		//If no sample is used load partition split borders from distribution
		if(!usesSample) {
			Class<DataDistribution> clsDstr = null;
			DataDistribution distr = null;
			try {
				ClassLoader cl = LibraryCacheManager.getClassLoader(getEnvironment().getJobID());
				String clsName = config.getStubParameters().getString(DATA_DISTRIBUTION_CLASS, null);
				clsDstr = (Class<DataDistribution>) cl.loadClass(clsName);
				distr = clsDstr.newInstance();
			} catch (Exception e) {
				throw new RuntimeException("DataDistribution could not be instantiated.", e);
			}
			
			//Generate split points
			int numPartitions = config.getStubParameters().getInteger(NUMBER_OF_PARTITIONS, -1);
			int numSplits = numPartitions - 1;
			splitBorders = new Key[numSplits];
			for (int i = 0; i < numSplits; i++) {
				splitBorders[i] = distr.getSplit(i, numSplits);
			}
		}
		
		try {
			// obtain stub implementation class
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
	 * Initializes the input reader of the MapTask.
	 * 
	 * @throws RuntimeException
	 *         Thrown if no input ship strategy was provided.
	 */
	private void initInputReader() throws RuntimeException {
		// create RecordDeserializer
		RecordDeserializer<KeyValuePair<Key, Value>> deserializerData = new KeyValuePairDeserializer<Key, Value>(keyType,
				valueType);
		
		// determine distribution pattern for reader from input ship strategy
		DistributionPattern dpData = null;
		switch (config.getInputShipStrategy(0)) {
		case FORWARD:
			// forward requires Pointwise DP
			dpData = new PointwiseDistributionPattern();
			break;
		default:
			throw new RuntimeException("No input ship strategy provided for Partition/Data.");
		}
		
		readerData = new RecordReader<KeyValuePair<Key, Value>>(this, deserializerData, dpData);
		
		if(usesSample) {
			// create RecordDeserializer
			RecordDeserializer<KeyValuePair<Key, Value>> deserializerHistogram = new KeyValuePairDeserializer<Key, Value>(
					keyType, (Class<Value>)((Class<? extends Value>)PactNull.class));
			
			// determine distribution pattern for reader from input ship strategy
			DistributionPattern dpHistogram = null;
			switch (config.getInputShipStrategy(1)) {
			case FORWARD:
				// forward requires Pointwise DP
				dpHistogram = new PointwiseDistributionPattern();
				throw new RuntimeException("EEE");
				//break;
			case PARTITION_HASH:
			case PARTITION_RANGE:
			case BROADCAST:
				// partition requires Bipartite DP
				dpHistogram = new BipartiteDistributionPattern();
				break;
			default:
				throw new RuntimeException("No input ship strategy provided for Partition/Sample.");
			}
			
			readerPartition = new RecordReader<KeyValuePair<Key, Value>>(this, deserializerHistogram, dpHistogram);
		}
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
			oe = new OutputEmitter(config.getOutputShipStrategy(i));
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
	 * This method is called with an iterator over all k-v pairs that this MapTask processes.
	 * It calls {@link MapStub#map(Key, Value, Collector)} for each pair. 
	 * 
	 * @param in
	 *        Iterator over all key-value pairs that this MapTask processes
	 * @param out
	 *        A collector for the output of the map() function.
	 */
	private void callStub(Iterator<KeyValuePair<Key, Value>> in, Collector<Key, Value> out)
	{
		while (!this.taskCanceled && in.hasNext()) {
			KeyValuePair<Key, Value> pair = in.next();
			out.collect(pair.getKey(), pair.getValue());
			//this.stub.map(pair.getKey(), pair.getValue(), out);
		}
	}
}
