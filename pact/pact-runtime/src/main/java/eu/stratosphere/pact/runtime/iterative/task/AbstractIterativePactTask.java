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

import eu.stratosphere.nephele.io.MutableReader;
import eu.stratosphere.pact.common.stubs.Stub;
import eu.stratosphere.pact.common.util.InstantiationUtil;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.generic.types.TypeSerializer;
import eu.stratosphere.pact.runtime.iterative.event.EndOfSuperstepEvent;
import eu.stratosphere.pact.runtime.iterative.event.TerminationEvent;
import eu.stratosphere.pact.runtime.iterative.io.InterruptingMutableObjectIterator;
import eu.stratosphere.pact.runtime.iterative.monitoring.IterationMonitoring;
import eu.stratosphere.pact.runtime.task.PactDriver;
import eu.stratosphere.pact.runtime.task.RegularPactTask;
import eu.stratosphere.pact.runtime.task.util.ReaderInterruptionBehavior;
import eu.stratosphere.pact.runtime.task.util.ReaderInterruptionBehaviors;
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

	private int numIterations = 1;

	// --------------------------------------------------------------------------------------------
	// Wrapping methods to supplement behavior of the regular Pact Task
	// --------------------------------------------------------------------------------------------
	
	@Override
	public void run() throws Exception {
		getTaskConfig().setStubParameter("pact.iterations.currentIteration", String.valueOf(currentIteration()));
		super.run();
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
	
	// --------------------------------------------------------------------------------------------
	// Utility Methods for Iteration Handling
	// --------------------------------------------------------------------------------------------
	
	protected boolean inFirstIteration() {
		return this.numIterations == 1;
	}

	protected int currentIteration() {
		return this.numIterations;
	}

	protected void incrementIterationCounter() {
		this.numIterations++;
	}

	protected void notifyMonitor(IterationMonitoring.Event event) {
		if (log.isInfoEnabled()) {
			log.info(IterationMonitoring.logLine(getEnvironment().getJobID(), event, currentIteration(),
				getEnvironment().getIndexInSubtaskGroup()));
		}
	}

	protected String brokerKey() {
		return getEnvironment().getJobID().toString() + '#' + getEnvironment().getIndexInSubtaskGroup();
	}

	protected String identifier() {
		return getEnvironment().getTaskName() + " (" + (getEnvironment().getIndexInSubtaskGroup() + 1) + '/' +
			getEnvironment().getCurrentNumberOfSubtasks() + ')';
	}

	protected void reinstantiateDriver() {
		Class<? extends PactDriver<S, OT>> driverClass = this.config.getDriver();
		this.driver = InstantiationUtil.instantiate(driverClass, PactDriver.class);
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
}
