package eu.stratosphere.nephele.taskmanager.bytebuffered;

import java.io.IOException;

import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.taskmanager.transferenvelope.TransferEnvelope;

interface ChannelContext {

	boolean isInputChannel();
	
	public JobID getJobID();

	public ChannelID getChannelID();

	public ChannelID getConnectedChannelID();

	public void reportIOException(IOException ioe);
	
	public void queueTransferEnvelope(TransferEnvelope transferEnvelope);
}
