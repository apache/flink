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

import java.util.ArrayList;

import eu.stratosphere.nephele.execution.librarycache.LibraryCacheManager;
import eu.stratosphere.nephele.io.ChannelSelector;
import eu.stratosphere.nephele.io.RecordWriter;
import eu.stratosphere.pact.common.contract.DataDistribution;
import eu.stratosphere.pact.common.contract.Order;
import eu.stratosphere.pact.common.stubs.Stub;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
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
 * The MapTask creates an iterator over all key-value pairs of its input and hands that to the <code>map()</code> method
 * of the MapStub.
 * 
 * @see eu.stratosphere.pact.common.stub.MapStub
 * @author Fabian Hueske
 */
@SuppressWarnings({ "unchecked", "rawtypes" })
public class PartitionTask extends AbstractPactTask {

	public static final String GLOBAL_PARTITIONING_ORDER = "partitioning.order";

	public static final String DATA_DISTRIBUTION_CLASS = "partitioning.distribution.class";

	public static final String PARTITION_BY_SAMPLING = "partitioning.sampling";

	public static final String NUMBER_OF_PARTITIONS = "partitioning.partitions.count";

	// partitioning function
	private PartitionFunction func;

	// order used for partitioning
	private Order order;

	private boolean usesSample;

	private PactRecord[] splitBorders;

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.AbstractPactTask#getNumberOfInputs()
	 */
	@Override
	public int getNumberOfInputs() {
		return 2;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.AbstractPactTask#getStubType()
	 */
	@Override
	public Class<Stub> getStubType() {
		return Stub.class;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.AbstractPactTask#prepare()
	 */
	@Override
	public void prepare() throws Exception {
		// obtain task configuration (including stub parameters)
		config = new TaskConfig(getTaskConfiguration());
	
		order = Order.valueOf(config.getStubParameters().getString(GLOBAL_PARTITIONING_ORDER, ""));
		usesSample = config.getStubParameters().getBoolean(PARTITION_BY_SAMPLING, true);
	
		// If no sample is used load partition split borders from distribution
		if (!usesSample) {
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
	
			// Generate split points
			int numPartitions = config.getStubParameters().getInteger(NUMBER_OF_PARTITIONS, -1);
			int numSplits = numPartitions - 1;
			splitBorders = new PactRecord[numSplits];
			for (int i = 0; i < numSplits; i++) {
				splitBorders[i] = distr.getSplit(i, numSplits);
			}
		}
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.AbstractPactTask#run()
	 */
	@Override
	public void run() throws Exception
	{
		// cache references on the stack
		final MutableObjectIterator<PactRecord> input = this.inputs[0];
		final MutableObjectIterator<PactRecord> histogramInput = this.inputs[1];
		
		if (!(this.output instanceof OutputCollector))
			throw new Exception("Error: Partition Task has a collector that is not an OutputCollector. " +
					"Another task was probably accidentally chained.");
		final OutputCollector output = (OutputCollector) this.output;
		
		final PactRecord record = new PactRecord();
		final PactRecord histogramRecord = new PactRecord();
		
		if (usesSample) {
			// Read partition and assign to OutputEmitter

			ArrayList<PactRecord> borders = new ArrayList<PactRecord>();
			while (histogramInput.next(histogramRecord)) {
				borders.add(histogramRecord.createCopy());
			}

			splitBorders = borders.toArray(new PactRecord[borders.size()]);
		}

		// Create partition function based on histogram
		func = new HistogramPartitionFunction(splitBorders, order);

		// Assign partition function to output emitter
		for (RecordWriter<PactRecord> recWriter : output.getWriters()) {
			ChannelSelector<PactRecord> selector =
				recWriter.getOutputGate().getChannelSelector();
			if (selector instanceof OutputEmitter) {
				((OutputEmitter) selector).setPartitionFunction(func);
			}
		}
		
		while (this.running && input.next(record)) {
			output.collect(record);
		}
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.AbstractPactTask#cleanup()
	 */
	@Override
	public void cleanup() throws Exception {
		// PartitionTasks need no cleanup, since no strategies are used.
	}
	

}
