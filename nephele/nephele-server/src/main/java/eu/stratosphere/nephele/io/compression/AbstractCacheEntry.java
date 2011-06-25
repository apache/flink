package eu.stratosphere.nephele.io.compression;

import java.util.HashSet;
import java.util.Set;

import eu.stratosphere.nephele.io.channels.ChannelID;

abstract class AbstractCacheEntry {

	private final Set<ChannelID> assignedChannels = new HashSet<ChannelID>();

	protected void addAssignedChannel(final ChannelID channelID) {

		this.assignedChannels.add(channelID);
	}

	public void removeAssignedChannel(final ChannelID channelID) {

		if (!this.assignedChannels.remove(channelID)) {
			throw new IllegalStateException(channelID + " has not been in the set of assigned channels");
		}
	}

	public boolean hasAssignedChannels() {

		return (!this.assignedChannels.isEmpty());
	}
}
