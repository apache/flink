/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.flink.runtime.iterative.task;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.api.common.aggregators.Aggregator;
import org.apache.flink.api.common.aggregators.LongSumAggregator;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.IterationRuntimeContext;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerFactory;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.network.api.MutableReader;
import org.apache.flink.runtime.iterative.concurrent.BlockingBackChannel;
import org.apache.flink.runtime.iterative.concurrent.BlockingBackChannelBroker;
import org.apache.flink.runtime.iterative.concurrent.Broker;
import org.apache.flink.runtime.iterative.concurrent.IterationAggregatorBroker;
import org.apache.flink.runtime.iterative.concurrent.SolutionSetBroker;
import org.apache.flink.runtime.iterative.convergence.WorksetEmptyConvergenceCriterion;
import org.apache.flink.runtime.iterative.io.SolutionSetUpdateOutputCollector;
import org.apache.flink.runtime.iterative.io.WorksetUpdateOutputCollector;
import org.apache.flink.runtime.operators.PactDriver;
import org.apache.flink.runtime.operators.RegularPactTask;
import org.apache.flink.runtime.operators.ResettablePactDriver;
import org.apache.flink.runtime.operators.hash.CompactingHashTable;
import org.apache.flink.runtime.operators.udf.RuntimeUDFContext;
import org.apache.flink.runtime.operators.util.TaskConfig;
import org.apache.flink.types.Value;
import org.apache.flink.util.Collector;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.MutableObjectIterator;

import java.io.IOException;

/**
 * The base class for all tasks able to participate in an iteration.
 */
public abstract class AbstractIterativePactTask<S extends Function, OT> extends RegularPactTask<S, OT>
		implements Terminable
{
	private static final Log log = LogFactory.getLog(AbstractIterativePactTask.class);
	
	protected LongSumAggregator worksetAggregator;

	protected BlockingBackChannel worksetBackChannel;

	protected boolean isWorksetIteration;

	protected boolean isWorksetUpdate;

	protected boolean isSolutionSetUpdate;
	

	private RuntimeAggregatorRegistry iterationAggregators;

	private String brokerKey;

	private int superstepNum = 1;
	
	private volatile boolean terminationRequested;

	// --------------------------------------------------------------------------------------------
	// Main life cycle methods that implement the iterative behavior
	// --------------------------------------------------------------------------------------------

	@Override
	protected void initialize() throws Exception {
		super.initialize();

		// check if the driver is resettable
		if (this.driver instanceof ResettablePactDriver) {
			final ResettablePactDriver<?, ?> resDriver = (ResettablePactDriver<?, ?>) this.driver;
			// make sure that the according inputs are not reseted
			for (int i = 0; i < resDriver.getNumberOfInputs(); i++) {
				if (resDriver.isInputResettable(i)) {
					excludeFromReset(i);
				}
			}
		}
		
		TaskConfig config = getLastTasksConfig();
		isWorksetIteration = config.getIsWorksetIteration();
		isWorksetUpdate = config.getIsWorksetUpdate();
		isSolutionSetUpdate = config.getIsSolutionSetUpdate();

		if (isWorksetUpdate) {
			worksetBackChannel = BlockingBackChannelBroker.instance().getAndRemove(brokerKey());

			if (isWorksetIteration) {
				worksetAggregator = getIterationAggregators().getAggregator(WorksetEmptyConvergenceCriterion.AGGREGATOR_NAME);

				if (worksetAggregator == null) {
					throw new RuntimeException("Missing workset elements count aggregator.");
				}
			}
		}
	}

	@Override
	public void run() throws Exception {
		if (inFirstIteration()) {
			if (this.driver instanceof ResettablePactDriver) {
				// initialize the repeatable driver
				((ResettablePactDriver<?, ?>) this.driver).initialize();
			}
		} else {
			reinstantiateDriver();
			resetAllInputs();
			
			// re-read the iterative broadcast variables
			for (int i : this.iterativeBroadcastInputs) {
				final String name = getTaskConfig().getBroadcastInputName(i);
				readAndSetBroadcastInput(i, name, this.runtimeUdfContext);
			}
		}

		// call the parent to execute the superstep
		super.run();
	}

	@Override
	protected void closeLocalStrategiesAndCaches() {
		try {
			super.closeLocalStrategiesAndCaches();
		}
		finally {
			if (this.driver instanceof ResettablePactDriver) {
				final ResettablePactDriver<?, ?> resDriver = (ResettablePactDriver<?, ?>) this.driver;
				try {
					resDriver.teardown();
				} catch (Throwable t) {
					log.error("Error while shutting down an iterative operator.", t);
				}
			}
		}
	}

	@Override
	public RuntimeUDFContext createRuntimeContext(String taskName) {
		Environment env = getEnvironment();
		return new IterativeRuntimeUdfContext(taskName, env.getCurrentNumberOfSubtasks(), env.getIndexInSubtaskGroup());
	}

	// --------------------------------------------------------------------------------------------
	// Utility Methods for Iteration Handling
	// --------------------------------------------------------------------------------------------

	protected boolean inFirstIteration() {
		return this.superstepNum == 1;
	}

	protected int currentIteration() {
		return this.superstepNum;
	}

	protected void incrementIterationCounter() {
		this.superstepNum++;
	}

	public String brokerKey() {
		if (brokerKey == null) {
			int iterationId = config.getIterationId();
			brokerKey = getEnvironment().getJobID().toString() + '#' + iterationId + '#' +
					getEnvironment().getIndexInSubtaskGroup();
		}
		return brokerKey;
	}

	private void reinstantiateDriver() throws Exception {
		if (this.driver instanceof ResettablePactDriver) {
			final ResettablePactDriver<?, ?> resDriver = (ResettablePactDriver<?, ?>) this.driver;
			resDriver.reset();
		} else {
			Class<? extends PactDriver<S, OT>> driverClass = this.config.getDriver();
			this.driver = InstantiationUtil.instantiate(driverClass, PactDriver.class);

			try {
				this.driver.setup(this);
			}
			catch (Throwable t) {
				throw new Exception("The pact driver setup for '" + this.getEnvironment().getTaskName() +
						"' , caused an error: " + t.getMessage(), t);
			}
		}
	}

	public RuntimeAggregatorRegistry getIterationAggregators() {
		if (this.iterationAggregators == null) {
			this.iterationAggregators = IterationAggregatorBroker.instance().get(brokerKey());
		}
		return this.iterationAggregators;
	}

	protected void verifyEndOfSuperstepState() throws IOException {
		// sanity check that there is at least one iterative input reader
		if (this.iterativeInputs.length == 0 && this.iterativeBroadcastInputs.length == 0) {
			throw new IllegalStateException("Error: Iterative task without a single iterative input.");
		}

		for (int inputNum : this.iterativeInputs) {
			MutableReader<?> reader = this.inputReaders[inputNum];

			if (!reader.isInputClosed()) {
				if (reader.hasReachedEndOfSuperstep()) {
					reader.startNextSuperstep();
				}
				else {
					// need to read and drop all non-consumed data until we reach the end-of-superstep
					@SuppressWarnings("unchecked")
					MutableObjectIterator<Object> inIter = (MutableObjectIterator<Object>) this.inputIterators[inputNum];
					Object o = this.inputSerializers[inputNum].getSerializer().createInstance();
					while ((o = inIter.next(o)) != null);
					
					if (!reader.isInputClosed()) {
						// also reset the end-of-superstep state
						reader.startNextSuperstep();
					}
				}
			}
		}
		
		for (int inputNum : this.iterativeBroadcastInputs) {
			MutableReader<?> reader = this.broadcastInputReaders[inputNum];

			if (!reader.isInputClosed()) {
				
				// sanity check that the BC input is at the end of the superstep
				if (!reader.hasReachedEndOfSuperstep()) {
					throw new IllegalStateException("An iterative broadcast input has not been fully consumed.");
				}
				
				reader.startNextSuperstep();
			}
		}
	}

	@Override
	public boolean terminationRequested() {
		return this.terminationRequested;
	}

	@Override
	public void requestTermination() {
		this.terminationRequested = true;
	}

	@Override
	public void cancel() throws Exception {
		requestTermination();
		super.cancel();
	}

	// -----------------------------------------------------------------------------------------------------------------
	// Iteration State Update Handling
	// -----------------------------------------------------------------------------------------------------------------

	/**
	 * Creates a new {@link WorksetUpdateOutputCollector}.
	 * <p/>
	 * This collector is used by {@link IterationIntermediatePactTask} or {@link IterationTailPactTask} to update the
	 * workset.
	 * <p/>
	 * If a non-null delegate is given, the new {@link Collector} will write to the solution set and also call
	 * collect(T) of the delegate.
	 *
	 * @param delegate null -OR- the delegate on which to call collect() by the newly created collector
	 * @return a new {@link WorksetUpdateOutputCollector}
	 */
	protected Collector<OT> createWorksetUpdateOutputCollector(Collector<OT> delegate) {
		DataOutputView outputView = worksetBackChannel.getWriteEnd();
		TypeSerializer<OT> serializer = getOutputSerializer();
		return new WorksetUpdateOutputCollector<OT>(outputView, serializer, delegate);
	}

	protected Collector<OT> createWorksetUpdateOutputCollector() {
		return createWorksetUpdateOutputCollector(null);
	}

	/**
	 * Creates a new solution set update output collector.
	 * <p/>
	 * This collector is used by {@link IterationIntermediatePactTask} or {@link IterationTailPactTask} to update the
	 * solution set of workset iterations. Depending on the task configuration, either a fast (non-probing)
	 * {@link org.apache.flink.runtime.iterative.io.SolutionSetFastUpdateOutputCollector} or normal (re-probing)
	 * {@link SolutionSetUpdateOutputCollector} is created.
	 * <p/>
	 * If a non-null delegate is given, the new {@link Collector} will write back to the solution set and also call
	 * collect(T) of the delegate.
	 *
	 * @param delegate null -OR- a delegate collector to be called by the newly created collector
	 * @return a new {@link org.apache.flink.runtime.iterative.io.SolutionSetFastUpdateOutputCollector} or
	 * {@link SolutionSetUpdateOutputCollector}
	 */
	protected Collector<OT> createSolutionSetUpdateOutputCollector(Collector<OT> delegate) {
		Broker<CompactingHashTable<?>> solutionSetBroker = SolutionSetBroker.instance();

		/*if (config.getIsSolutionSetUpdateWithoutReprobe()) {
			@SuppressWarnings("unchecked")
			MutableHashTable<OT, ?> solutionSet = (MutableHashTable<OT, ?>) solutionSetBroker.get(brokerKey());

			return new SolutionSetFastUpdateOutputCollector<OT>(solutionSet, delegate);
		} else {*/
			@SuppressWarnings("unchecked")
			CompactingHashTable<OT> solutionSet = (CompactingHashTable<OT>) solutionSetBroker.get(brokerKey());
			TypeSerializer<OT> serializer = getOutputSerializer();
			return new SolutionSetUpdateOutputCollector<OT>(solutionSet, serializer, delegate);
		//}
	}

	/**
	 * @return output serializer of this task
	 */
	private TypeSerializer<OT> getOutputSerializer() {
		TypeSerializerFactory<OT> serializerFactory;

		if ((serializerFactory = getLastTasksConfig().getOutputSerializer(userCodeClassLoader)) == null) {
			throw new RuntimeException("Missing output serializer for workset update.");
		}

		return serializerFactory.getSerializer();
	}

	// -----------------------------------------------------------------------------------------------------------------

	private class IterativeRuntimeUdfContext extends RuntimeUDFContext implements IterationRuntimeContext {

		public IterativeRuntimeUdfContext(String name, int numParallelSubtasks, int subtaskIndex) {
			super(name, numParallelSubtasks, subtaskIndex);
		}

		@Override
		public int getSuperstepNumber() {
			return AbstractIterativePactTask.this.superstepNum;
		}

		@Override
		public <T extends Aggregator<?>> T getIterationAggregator(String name) {
			return getIterationAggregators().<T>getAggregator(name);
		}

		@Override
		@SuppressWarnings("unchecked")
		public <T extends Value> T getPreviousIterationAggregate(String name) {
			return (T) getIterationAggregators().getPreviousGlobalAggregate(name);
		}
	}

}
