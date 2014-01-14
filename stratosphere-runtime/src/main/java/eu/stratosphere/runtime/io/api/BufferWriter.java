/***********************************************************************************************************************
 * Copyright (C) 2010-2014 by the Stratosphere project (http://stratosphere.eu)
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

import eu.stratosphere.nephele.event.task.AbstractEvent;
import eu.stratosphere.nephele.event.task.AbstractTaskEvent;
import eu.stratosphere.nephele.event.task.EventListener;
import eu.stratosphere.runtime.io.channels.EndOfSuperstepEvent;
import eu.stratosphere.nephele.template.AbstractInvokable;
import eu.stratosphere.runtime.io.Buffer;
import eu.stratosphere.runtime.io.gates.OutputGate;

import java.io.IOException;

public class BufferWriter {

	protected final OutputGate outputGate;

	public BufferWriter(AbstractInvokable invokable) {
		this.outputGate = invokable.getEnvironment().createAndRegisterOutputGate();
	}

	public void sendBuffer(Buffer buffer, int targetChannel) throws IOException, InterruptedException {
		this.outputGate.sendBuffer(buffer, targetChannel);
	}

	public void sendEvent(AbstractEvent event, int targetChannel) throws IOException, InterruptedException {
		this.outputGate.sendEvent(event, targetChannel);
	}

	public void sendBufferAndEvent(Buffer buffer, AbstractEvent event, int targetChannel) throws IOException, InterruptedException {
		this.outputGate.sendBufferAndEvent(buffer, event, targetChannel);
	}

	public void broadcastBuffer(Buffer buffer) throws IOException, InterruptedException {
		this.outputGate.broadcastBuffer(buffer);
	}

	public void broadcastEvent(AbstractEvent event) throws IOException, InterruptedException {
		this.outputGate.broadcastEvent(event);
	}

	// -----------------------------------------------------------------------------------------------------------------

	public void subscribeToEvent(EventListener eventListener, Class<? extends AbstractTaskEvent> eventType) {
		this.outputGate.subscribeToEvent(eventListener, eventType);
	}

	public void unsubscribeFromEvent(EventListener eventListener, Class<? extends AbstractTaskEvent> eventType) {
		this.outputGate.unsubscribeFromEvent(eventListener, eventType);
	}

	public void sendEndOfSuperstep() throws IOException, InterruptedException {
		this.outputGate.broadcastEvent(EndOfSuperstepEvent.INSTANCE);
	}
}
