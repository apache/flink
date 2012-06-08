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

package eu.stratosphere.nephele.deployment;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import eu.stratosphere.nephele.io.GateID;
import eu.stratosphere.nephele.io.IOReadableWritable;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.io.compression.CompressionLevel;
import eu.stratosphere.nephele.util.EnumUtils;

/**
 * A gate deployment descriptor contains all the information necessary to deploy either an input or an output gate as
 * part of a task on a task manager.
 * <p>
 * This class is not thread-safe in general.
 * 
 * @author warneke
 */
public final class GateDeploymentDescriptor implements IOReadableWritable {

	/**
	 * The ID of the gate.
	 */
	private final GateID gateID;

	/**
	 * The channel type of the gate.
	 */
	private ChannelType channelType;

	/**
	 * The compression level of the gate.
	 */
	private CompressionLevel compressionLevel;

	/**
	 * The list of channel deployment descriptors attached to this gate.
	 */
	private final List<ChannelDeploymentDescriptor> channels;

	/**
	 * Constructs a new gate deployment descriptor
	 * 
	 * @param gateID
	 *        the ID of the gate
	 * @param channelType
	 *        the channel type of the gate
	 * @param compressionLevel
	 *        the compression level of the gate
	 * @param channels
	 *        the list of channel deployment descriptors attached to this gate
	 */
	public GateDeploymentDescriptor(final GateID gateID, final ChannelType channelType,
			final CompressionLevel compressionLevel, List<ChannelDeploymentDescriptor> channels) {

		if (gateID == null) {
			throw new IllegalArgumentException("Argument gateID must no be null");
		}

		if (channelType == null) {
			throw new IllegalArgumentException("Argument channelType must no be null");
		}

		if (compressionLevel == null) {
			throw new IllegalArgumentException("Argument compressionLevel must no be null");
		}

		if (channels == null) {
			throw new IllegalArgumentException("Argument channels must no be null");
		}

		this.gateID = gateID;
		this.channelType = channelType;
		this.compressionLevel = compressionLevel;
		this.channels = channels;
	}

	/**
	 * Default constructor for serialization/deserialization.
	 */
	public GateDeploymentDescriptor() {

		this.gateID = new GateID();
		this.channelType = null;
		this.compressionLevel = null;
		this.channels = new ArrayList<ChannelDeploymentDescriptor>();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void write(final DataOutput out) throws IOException {

		this.gateID.write(out);
		EnumUtils.writeEnum(out, channelType);
		EnumUtils.writeEnum(out, this.compressionLevel);
		out.writeInt(this.channels.size());
		final Iterator<ChannelDeploymentDescriptor> it = this.channels.iterator();
		while (it.hasNext()) {
			it.next().write(out);
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void read(final DataInput in) throws IOException {

		this.gateID.read(in);
		this.channelType = EnumUtils.readEnum(in, ChannelType.class);
		this.compressionLevel = EnumUtils.readEnum(in, CompressionLevel.class);
		final int nocdd = in.readInt();
		for (int i = 0; i < nocdd; ++i) {
			final ChannelDeploymentDescriptor cdd = new ChannelDeploymentDescriptor();
			cdd.read(in);
			this.channels.add(cdd);
		}
	}
}
