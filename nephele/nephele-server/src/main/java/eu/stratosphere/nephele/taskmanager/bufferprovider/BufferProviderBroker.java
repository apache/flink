package eu.stratosphere.nephele.taskmanager.bufferprovider;

import eu.stratosphere.nephele.io.channels.ChannelID;

public interface BufferProviderBroker {

	BufferProvider getBufferProvider(ChannelID channelID);
}
