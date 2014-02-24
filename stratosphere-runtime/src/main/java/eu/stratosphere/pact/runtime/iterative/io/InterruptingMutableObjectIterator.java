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

package eu.stratosphere.pact.runtime.iterative.io;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;

import com.google.common.base.Preconditions;

import eu.stratosphere.nephele.event.task.AbstractTaskEvent;
import eu.stratosphere.nephele.event.task.EventListener;
import eu.stratosphere.nephele.io.channels.bytebuffered.EndOfSuperstepEvent;
import eu.stratosphere.pact.runtime.iterative.event.TerminationEvent;
import eu.stratosphere.pact.runtime.iterative.task.Terminable;
import eu.stratosphere.util.MutableObjectIterator;

/**
 * a delegating {@link MutableObjectIterator} that interrupts the current thread when a given number of events occured.
 * This is necessary to repetitively read channels when executing iterative data flows. The wrapped iterator must return
 * false
 * on interruption, see {@link eu.stratosphere.pact.runtime.task.util.ReaderInterruptionBehaviors}
 */
public class InterruptingMutableObjectIterator<E> implements MutableObjectIterator<E>, EventListener {
	
//	private static final Log log = LogFactory.getLog(InterruptingMutableObjectIterator.class);
	

	private final MutableObjectIterator<E> delegate;

//	private final String name;

	private final int numberOfEventsUntilInterrupt;

	private final AtomicInteger endOfSuperstepEventCounter;

	private final AtomicInteger terminationEventCounter;

	private final Terminable owningIterativeTask;

//	private final int gateIndex;


	public InterruptingMutableObjectIterator(MutableObjectIterator<E> delegate, int numberOfEventsUntilInterrupt,
			String name, Terminable owningIterativeTask, int gateIndex)
	{
		Preconditions.checkArgument(numberOfEventsUntilInterrupt > 0);
		this.delegate = delegate;
		this.numberOfEventsUntilInterrupt = numberOfEventsUntilInterrupt;
//		this.name = name;
		this.owningIterativeTask = owningIterativeTask;
//		this.gateIndex = gateIndex;

		endOfSuperstepEventCounter = new AtomicInteger(0);
		terminationEventCounter = new AtomicInteger(0);
	}

	@Override
	public void eventOccurred(AbstractTaskEvent event) {

		if (EndOfSuperstepEvent.class.equals(event.getClass())) {
			onEndOfSuperstep();
			return;
		}

		if (TerminationEvent.class.equals(event.getClass())) {
			onTermination();
			return;
		}

		throw new IllegalStateException("Unable to handle event " + event.getClass().getName());
	}

	private void onTermination() {
		int numberOfEventsSeen = terminationEventCounter.incrementAndGet();
//		if (log.isInfoEnabled()) {
//			log.info("InterruptibleIterator of " + name + " on gate [" + gateIndex + "] received Termination event (" +
//				numberOfEventsSeen + ")");
//		}

		Preconditions.checkState(numberOfEventsSeen <= numberOfEventsUntilInterrupt);

		if (numberOfEventsSeen == numberOfEventsUntilInterrupt) {
			owningIterativeTask.requestTermination();
		}
	}

	private void onEndOfSuperstep() {
		int numberOfEventsSeen = endOfSuperstepEventCounter.incrementAndGet();
//		if (log.isInfoEnabled()) {
//			log.info("InterruptibleIterator of " + name + " on gate [" + gateIndex
//				+ "] received EndOfSuperstep event (" +
//				numberOfEventsSeen + ")");
//		}

		if (numberOfEventsSeen % numberOfEventsUntilInterrupt == 0) {
			Thread.currentThread().interrupt();
		}
	}

	@Override
	public E next(E reuse) throws IOException {
		E found = delegate.next(reuse);

//		if (!recordFound && log.isInfoEnabled()) {
//			log.info("InterruptibleIterator of " + name + " on gate [" + gateIndex + "] releases input");
//		}
		
		return found;
	}
}
