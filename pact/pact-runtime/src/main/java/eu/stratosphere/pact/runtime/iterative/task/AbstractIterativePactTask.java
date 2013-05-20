/***********************************************************************************************************************
 *
 * Copyright (C) 2012 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.pact.runtime.iterative.task;

import eu.stratosphere.nephele.execution.Environment;
import eu.stratosphere.nephele.io.MutableReader;
import eu.stratosphere.pact.common.stubs.IterationRuntimeContext;
import eu.stratosphere.pact.common.stubs.RuntimeContext;
import eu.stratosphere.pact.common.stubs.Stub;
import eu.stratosphere.pact.common.stubs.aggregators.Aggregator;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.common.util.InstantiationUtil;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.generic.types.TypeSerializer;
import eu.stratosphere.pact.runtime.hash.MutableHashTable;
import eu.stratosphere.pact.runtime.iterative.concurrent.IterationAggregatorBroker;
import eu.stratosphere.pact.runtime.iterative.concurrent.SolutionsetBroker;
import eu.stratosphere.pact.runtime.iterative.event.EndOfSuperstepEvent;
import eu.stratosphere.pact.runtime.iterative.event.TerminationEvent;
import eu.stratosphere.pact.runtime.iterative.io.InterruptingMutableObjectIterator;
import eu.stratosphere.pact.runtime.iterative.io.UpdateSolutionsetOutputCollector;
import eu.stratosphere.pact.runtime.iterative.monitoring.IterationMonitoring;
import eu.stratosphere.pact.runtime.task.PactDriver;
import eu.stratosphere.pact.runtime.task.RegularPactTask;
import eu.stratosphere.pact.runtime.task.ResettablePactDriver;
import eu.stratosphere.pact.runtime.task.util.ReaderInterruptionBehavior;
import eu.stratosphere.pact.runtime.task.util.ReaderInterruptionBehaviors;
import eu.stratosphere.pact.runtime.udf.RuntimeUDFContext;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The base class for all tasks able to participate in an iteration.
 */
public abstract class AbstractIterativePactTask<S extends Stub, OT> extends RegularPactTask<S, OT>
	implements Terminable
{
	private static final Log log = LogFactory.getLog(AbstractIterativePactTask.class);

	private final AtomicBoolean terminationRequested = new AtomicBoolean(false);

	private int superstepNum = 1;
	
	private RuntimeAggregatorRegistry iterationAggregators;
	
	private String brokerKey;

	// --------------------------------------------------------------------------------------------
	// Wrapping methods to supplement behavior of the regular Pact Task
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
			// initialize the repeatable driver
			resDriver.initialize();
		}
		
		// instantiate the solution set update, if this task is responsible
		if (config.getUpdateSolutionSet()) {
			if (config.getUpdateSolutionSetWithoutReprobe()) {
				@SuppressWarnings("unchecked")
				MutableHashTable<OT, ?> hashTable = (MutableHashTable<OT, ?>) SolutionsetBroker.instance().get(brokerKey);
				this.output = new UpdateSolutionsetOutputCollector<OT>(this.output, hashTable);
			} else {
				throw new UnsupportedOperationException("Runtime currently supports only fast updates withpout reprobing.");
			}
		}
	}
	
	@Override
	public void run() throws Exception {
		if (!inFirstIteration()) {
			reinstantiateDriver();
			resetAllInputs();
		}
		super.run();
	}
	
	@Override
	protected void closeLocalStrategiesAndCaches() {
		super.closeLocalStrategiesAndCaches();
		
		if (this.driver instanceof ResettablePactDriver) {
			final ResettablePactDriver<?, ?> resDriver = (ResettablePactDriver<?, ?>) this.driver;
			try {
				resDriver.teardown();
			} catch (Throwable t) {
				log.error("Error shutting down a resettable driver.", t);
			}
		}
	}

	@Override
	protected ReaderInterruptionBehavior readerInterruptionBehavior(int inputGateIndex) {
		return getTaskConfig().isIterativeInputGate(inputGateIndex) ?
			ReaderInterruptionBehaviors.RELEASE_ON_INTERRUPT : ReaderInterruptionBehaviors.EXCEPTION_ON_INTERRUPT;
	}
	
	@Override
	protected MutableObjectIterator<?> createInputIterator(int i, 
		MutableReader<?> inputReader, TypeSerializer<?> serializer)
	{
		final MutableObjectIterator<?> inIter = super.createInputIterator(i, inputReader, serializer);
		final int numberOfEventsUntilInterrupt = getTaskConfig().getNumberOfEventsUntilInterruptInIterativeGate(i);
		
		if (numberOfEventsUntilInterrupt == 0) {
			// non iterative gate
			return inIter;
		}
	
		@SuppressWarnings({ "unchecked", "rawtypes" })
		InterruptingMutableObjectIterator<?> interruptingIterator = new InterruptingMutableObjectIterator(
			inIter, numberOfEventsUntilInterrupt, identifier(), this, i);
	
		inputReader.subscribeToEvent(interruptingIterator, EndOfSuperstepEvent.class);
		inputReader.subscribeToEvent(interruptingIterator, TerminationEvent.class);
	
		if (log.isInfoEnabled()) {
			log.info(formatLogString("wrapping input [" + i + "] with an interrupting iterator that waits " +
				"for [" + numberOfEventsUntilInterrupt + "] event(s)"));
		}
		return interruptingIterator;
	}
	
	public RuntimeContext getRuntimeContext(String taskName) {
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

	protected void notifyMonitor(IterationMonitoring.Event event) {
		if (log.isInfoEnabled()) {
			log.info(IterationMonitoring.logLine(getEnvironment().getJobID(), event, currentIteration(),
				getEnvironment().getIndexInSubtaskGroup()));
		}
	}

	public String brokerKey() {
		if (brokerKey == null) {
			int iterationId = config.getIterationId();
			brokerKey = getEnvironment().getJobID().toString() + '#' + iterationId + '#' + 
					getEnvironment().getIndexInSubtaskGroup();
		}
		return brokerKey;
	}

	protected String identifier() {
		return getEnvironment().getTaskName() + " (" + (getEnvironment().getIndexInSubtaskGroup() + 1) + '/' +
			getEnvironment().getCurrentNumberOfSubtasks() + ')';
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

	@Override
	public boolean terminationRequested() {
		return this.terminationRequested.get();
	}

	@Override
	public void requestTermination() {
		if (log.isInfoEnabled()) {
			log.info(formatLogString("requesting termination."));
		}
		this.terminationRequested.set(true);
	}
	
	@Override
	public void cancel() throws Exception {
		requestTermination();
		super.cancel();
	}
	
	private class IterativeRuntimeUdfContext extends RuntimeUDFContext implements IterationRuntimeContext {

		public IterativeRuntimeUdfContext(String name, int numParallelSubtasks, int subtaskIndex) {
			super(name, numParallelSubtasks, subtaskIndex);
		}

		@Override
		public int getSuperstepNumber() {
			return AbstractIterativePactTask.this.superstepNum;
		}

		@Override
		public <T extends Value> Aggregator<T> getIterationAggregator(String name) {
			return getIterationAggregators().getAggregator(name);
		}

		@Override
		@SuppressWarnings("unchecked")
		public <T extends Value> T getPreviousIterationAggregate(String name) {
			return (T) getIterationAggregators().getPreviousGlobalAggregate(name);
		}
	}
}
