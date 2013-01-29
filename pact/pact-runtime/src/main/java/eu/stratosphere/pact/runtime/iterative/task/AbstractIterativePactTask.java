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
	
	private MutableObjectIterator<?>[] wrappedInputs;

	private final AtomicBoolean terminationRequested = new AtomicBoolean(false);

	private int numIterations = 1;

	// --------------------------------------------------------------------------------------------
	// Wrapping methods to supplement behavior of the regular Pact Task
	// --------------------------------------------------------------------------------------------
	
	@Override
	public void invoke() throws Exception {
		getTaskConfig().setStubParameter("pact.iterations.currentIteration", String.valueOf(currentIteration()));
		super.invoke();
	}
	
	@Override
	protected void initInputStrategies() throws Exception {
		super.initInputStrategies();
		this.wrappedInputs = new MutableObjectIterator<?>[this.inputs.length];
	}

	@Override
	protected ReaderInterruptionBehavior readerInterruptionBehavior(int inputGateIndex) {
		return getTaskConfig().isIterativeInputGate(inputGateIndex) ?
			ReaderInterruptionBehaviors.RELEASE_ON_INTERRUPT : ReaderInterruptionBehaviors.EXCEPTION_ON_INTERRUPT;
	}

	@Override
	@SuppressWarnings("unchecked")
	public <X> MutableObjectIterator<X> getInput(int inputIndex) {
		if (this.wrappedInputs[inputIndex] != null) {
			return (MutableObjectIterator<X>) this.wrappedInputs[inputIndex];
		}

		if (this.config.isIterativeInputGate(inputIndex)) {
			return wrapWithInterruptingIterator(inputIndex);
		} else {
			// cache the input to avoid repeated config lookups
			MutableObjectIterator<X> input = super.getInput(inputIndex);
			this.wrappedInputs[inputIndex] = input;
			return input;
		}
	}

	private <X> MutableObjectIterator<X> wrapWithInterruptingIterator(int inputIndex) {
		int numberOfEventsUntilInterrupt = getTaskConfig().getNumberOfEventsUntilInterruptInIterativeGate(
			inputIndex);

		InterruptingMutableObjectIterator<X> interruptingIterator = new InterruptingMutableObjectIterator<X>(
			super.<X>getInput(inputIndex), numberOfEventsUntilInterrupt, identifier(),
			this, inputIndex);

		MutableReader<?> inputReader = getReader(inputIndex);
		inputReader.subscribeToEvent(interruptingIterator, EndOfSuperstepEvent.class);
		inputReader.subscribeToEvent(interruptingIterator, TerminationEvent.class);

		if (log.isInfoEnabled()) {
			log.info(formatLogString("wrapping input [" + inputIndex + 
				"] with an interrupting iterator that waits " +
				"for [" + numberOfEventsUntilInterrupt + "] event(s)"));
		}

		this.wrappedInputs[inputIndex] = interruptingIterator;

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
}
