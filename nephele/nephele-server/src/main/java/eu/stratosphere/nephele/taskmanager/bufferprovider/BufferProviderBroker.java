package eu.stratosphere.nephele.taskmanager.bufferprovider;

import java.io.IOException;

import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.jobgraph.JobID;

public interface BufferProviderBroker {

	BufferProvider getBufferProvider(JobID jobID, ChannelID sourceChannelID) throws IOException, InterruptedException;
}
