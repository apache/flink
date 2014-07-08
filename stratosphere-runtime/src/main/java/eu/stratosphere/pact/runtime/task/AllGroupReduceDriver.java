/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.pact.runtime.task;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.api.common.functions.GenericCombine;
import eu.stratosphere.api.common.functions.GenericGroupReduce;
import eu.stratosphere.api.common.typeutils.TypeSerializer;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;
import eu.stratosphere.pact.runtime.util.MutableToRegularIteratorWrapper;
import eu.stratosphere.util.Collector;
import eu.stratosphere.util.MutableObjectIterator;

/**
 * GroupReduce task which is executed by a Nephele task manager. The task has a
 * single input and one or multiple outputs. It is provided with a GroupReduceFunction
 * implementation.
 * <p>
 * The GroupReduceTask creates a iterator over all records from its input. The iterator returns all records grouped by their
 * key. The iterator is handed to the <code>reduce()</code> method of the GroupReduceFunction.
 * 
 * @see GenericGroupReduce
 */
public class AllGroupReduceDriver<IT, OT> implements PactDriver<GenericGroupReduce<IT, OT>, OT> {
	
	private static final Log LOG = LogFactory.getLog(AllGroupReduceDriver.class);

	private PactTaskContext<GenericGroupReduce<IT, OT>, OT> taskContext;
	
	private MutableObjectIterator<IT> input;

	private TypeSerializer<IT> serializer;
	
	private DriverStrategy strategy;

	// ------------------------------------------------------------------------

	@Override
	public void setup(PactTaskContext<GenericGroupReduce<IT, OT>, OT> context) {
		this.taskContext = context;
	}
	
	@Override
	public int getNumberOfInputs() {
		return 1;
	}

	@Override
	public Class<GenericGroupReduce<IT, OT>> getStubType() {
		@SuppressWarnings("unchecked")
		final Class<GenericGroupReduce<IT, OT>> clazz = (Class<GenericGroupReduce<IT, OT>>) (Class<?>) GenericGroupReduce.class;
		return clazz;
	}

	@Override
	public boolean requiresComparatorOnInput() {
		return false;
	}

	// --------------------------------------------------------------------------------------------

	@Override
	public void prepare() throws Exception {
		final TaskConfig config = this.taskContext.getTaskConfig();
		this.strategy = config.getDriverStrategy();
		
		if (strategy == DriverStrategy.ALL_GROUP_COMBINE) {
			if (!(this.taskContext.getStub() instanceof GenericCombine)) {
				throw new Exception("Using combiner on a UDF that does not implement the combiner interface " + GenericCombine.class.getName());
			}
		}
		else if (strategy != DriverStrategy.ALL_GROUP_REDUCE) {
			throw new Exception("Unrecognized driver strategy for AllGroupReduce driver: " + config.getDriverStrategy().name());
		}
		this.serializer = this.taskContext.<IT>getInputSerializer(0).getSerializer();
		this.input = this.taskContext.getInput(0);
	}

	@Override
	public void run() throws Exception {
		if (LOG.isDebugEnabled()) {
			LOG.debug(this.taskContext.formatLogString("AllGroupReduce preprocessing done. Running Reducer code."));
		}

		final MutableToRegularIteratorWrapper<IT> inIter = new MutableToRegularIteratorWrapper<IT>(this.input, this.serializer);

		// single UDF call with the single group
		if (inIter.hasNext()) {
			if (strategy == DriverStrategy.ALL_GROUP_REDUCE) {
				final GenericGroupReduce<IT, OT> reducer = this.taskContext.getStub();
				final Collector<OT> output = this.taskContext.getOutputCollector();
				reducer.reduce(inIter, output);
			}
			else {
				@SuppressWarnings("unchecked")
				final GenericCombine<IT> combiner = (GenericCombine<IT>) this.taskContext.getStub();
				@SuppressWarnings("unchecked")
				final Collector<IT> output = (Collector<IT>) this.taskContext.getOutputCollector();
				combiner.combine(inIter, output);
			}
		}
	}

	@Override
	public void cleanup() {}

	@Override
	public void cancel() {}
}