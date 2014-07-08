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

package eu.stratosphere.runtime.io.api;

import eu.stratosphere.core.io.IOReadableWritable;
import eu.stratosphere.nephele.event.task.AbstractTaskEvent;
import eu.stratosphere.runtime.io.gates.InputGate;
import eu.stratosphere.nephele.template.AbstractInvokable;

import java.io.IOException;

/**
 * This is an abstract base class for a record reader, either dealing with mutable or immutable records.
 * 
 * @param <T> The type of the record that can be read from this record reader.
 */
public abstract class AbstractSingleGateRecordReader<T extends IOReadableWritable> extends AbstractRecordReader {
	
	/**
	 * The input gate associated with the record reader.
	 */
	protected final InputGate<T> inputGate;
	
	// --------------------------------------------------------------------------------------------

	protected AbstractSingleGateRecordReader(AbstractInvokable invokable) {
		this.inputGate = invokable.getEnvironment().createAndRegisterInputGate();
	}

	/**
	 * Returns the number of input channels wired to this reader's input gate.
	 * 
	 * @return the number of input channels wired to this reader's input gate
	 */
	public int getNumberOfInputChannels() {
		return this.inputGate.getNumberOfInputChannels();
	}

	/**
	 * Publishes an event.
	 * 
	 * @param event
	 *        the event to be published
	 * @throws IOException
	 *         thrown if an error occurs while transmitting the event
	 * @throws InterruptedException
	 *         thrown if the thread is interrupted while waiting for the event to be published
	 */
	@Override
	public void publishEvent(AbstractTaskEvent event) throws IOException, InterruptedException {
		// Delegate call to input gate to send events
		this.inputGate.publishEvent(event);
	}

	InputGate<T> getInputGate() {
		return this.inputGate;
	}
}
