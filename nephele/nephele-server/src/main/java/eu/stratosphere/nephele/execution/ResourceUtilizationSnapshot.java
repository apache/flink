/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.nephele.execution;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import eu.stratosphere.nephele.annotations.ForceCheckpoint;
import eu.stratosphere.nephele.io.IOReadableWritable;
import eu.stratosphere.nephele.io.channels.ChannelID;

/**
 * A resource utilization snapshot provides a momentary snapshot of a task's resource utilization.
 * 
 * @author warneke
 */
public final class ResourceUtilizationSnapshot implements IOReadableWritable {

	/**
	 * The time stamp which indicates when the snapshot has been taken.
	 */
	private long timestamp = 0L;

	/**
	 * Stores the utilization of task's output channels at the time when the snapshot was created.
	 */
	private final Map<ChannelID, Long> channelUtilization;
	
	/**
	 * userCPu Time in percent
	 */
	private long userCPU;
	
	/**
	 * The forced decision if annotated
	 */
	private Boolean forced;


	public ResourceUtilizationSnapshot(final long timestamp, final Map<ChannelID, Long> channelUtilization,long userCPU) {

		if (timestamp <= 0L) {
			throw new IllegalArgumentException("Argument timestamp must be larger than zero");
		}

		if (channelUtilization == null) {
			throw new IllegalArgumentException("Argument channelUtilization is null");
		}

		this.timestamp = timestamp;
		this.channelUtilization = channelUtilization;
		this.userCPU = userCPU;
		
	}
	public ResourceUtilizationSnapshot(final long timestamp, final Map<ChannelID, Long> channelUtilization,long userCPU, Boolean forced) {

		if (timestamp <= 0L) {
			throw new IllegalArgumentException("Argument timestamp must be larger than zero");
		}

		if (channelUtilization == null) {
			throw new IllegalArgumentException("Argument channelUtilization is null");
		}

		this.timestamp = timestamp;
		this.channelUtilization = channelUtilization;
		this.userCPU = userCPU;
		this.forced = forced;
		
	}

	public ResourceUtilizationSnapshot() {
		this.channelUtilization = new HashMap<ChannelID, Long>();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final DataOutput out) throws IOException {

		// Write the timestamp
		out.writeLong(this.timestamp);

		// Serialize the output channel utilization
		out.writeInt(this.channelUtilization.size());
		final Iterator<Map.Entry<ChannelID, Long>> it = this.channelUtilization.entrySet().iterator();
		while (it.hasNext()) {

			final Map.Entry<ChannelID, Long> entry = it.next();
			entry.getKey().write(out);
			out.writeLong(entry.getValue().longValue());
		}
		// Write the userCPU
		out.writeLong(this.userCPU);
		// Write forced decision 
		if(this.forced == null){
			out.writeByte(0);
		}else{
			out.writeByte(1);
			out.writeBoolean(this.forced);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void read(final DataInput in) throws IOException {

		// Read the timestamp
		this.timestamp = in.readLong();

		// Deserialize the output channel utilization
		final int size = in.readInt();
		for (int i = 0; i < size; ++i) {

			final ChannelID channelID = new ChannelID();
			channelID.read(in);
			final Long l = Long.valueOf(in.readLong());
			this.channelUtilization.put(channelID, l);
		}
		this.userCPU = in.readLong();
		
		if(in.readByte() == 1){
			this.forced = in.readBoolean();
		}
	}

	/**
	 * Returns the time stamp which indicates when the snapshot has been taken at the worker node.
	 * 
	 * @return the time stamp which indicates when the snapshot has been taken
	 */
	public long getTimestamp() {

		return this.timestamp;
	}

	/**
	 * Returns the number of bytes which have been transmitted through the channel with the given channel ID between the
	 * channel's instantiation and the point in time when the snapshot was taken.
	 * 
	 * @param channelID
	 *        the ID of the channel to get the amount of transmitted data for
	 * @return the number of bytes transmitted through the channel or <code>-1</code> if no information is available for
	 *         the channel with the given ID
	 */
	public long getAmountOfDataTransmitted(final ChannelID channelID) {

		final Long l = this.channelUtilization.get(channelID);
		if (l == null) {
			return -1L;
		}

		return l.longValue();
	}


	/**
	 * Returns the userCPU.
	 * 
	 * @return the userCPU
	 */
	public long getUserCPU() {
		return this.userCPU;
	}
	/**
	 * Returns whether a decision was forced by the user
	 * @return 
	 */
	public Boolean getForced() {
		return forced;
	}
	
}
