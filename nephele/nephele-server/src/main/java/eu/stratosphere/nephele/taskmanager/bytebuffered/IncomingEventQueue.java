package eu.stratosphere.nephele.taskmanager.bytebuffered;

import java.util.Queue;
import java.util.concurrent.LinkedBlockingDeque;

import eu.stratosphere.nephele.event.task.AbstractEvent;

public final class IncomingEventQueue {

	private final Queue<AbstractEvent> incomingEventQueue = new LinkedBlockingDeque<AbstractEvent>();

	private final OutputChannelForwardingChain forwardingChain;

	IncomingEventQueue(final OutputChannelForwardingChain forwardingChain) {
		this.forwardingChain = forwardingChain;
	}

	public void processQueuedEvents() {

		AbstractEvent event = this.incomingEventQueue.poll();
		while (event != null) {

			this.forwardingChain.processEvent(event);
			event = this.incomingEventQueue.poll();
		}
	}

	void offer(final AbstractEvent event) {
		this.incomingEventQueue.offer(event);
	}
}
