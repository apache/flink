package eu.stratosphere.nephele.taskmanager.bytebuffered;

import java.io.IOException;

import eu.stratosphere.nephele.event.task.AbstractEvent;
import eu.stratosphere.nephele.taskmanager.transferenvelope.TransferEnvelope;

public interface OutputChannelForwarder {

	boolean forward(final TransferEnvelope transferEnvelope) throws IOException, InterruptedException;
	
	boolean hasDataLeft();
	
	void processEvent(AbstractEvent event);
	
	void destroy();
}
