/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
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

import java.util.List;

import eu.stratosphere.nephele.io.GateID;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.io.compression.CompressionLevel;

/**
 * A gate deployment descriptor contains all the information necessary to deploy either an input or an output gate as
 * part of a task on a task manager.
 * <p>
 * This class is not thread-safe in general.
 * 
 * @author warneke
 */
public final class GateDeploymentDescriptor {

	/**
	 * The ID of the gate.
	 */
	private final GateID gateID;

	/**
	 * The channel type of the gate.
	 */
	private final ChannelType channelType;

	/**
	 * The compression level of the gate.
	 */
	private final CompressionLevel compressionLevel;

	/**
	 * Stores whether the channels connected to this gate shall allow spanning records at runtime.
	 */
	private boolean allowSpanningRecords;

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
	 * @param allowSpanningRecords
	 *        <code>true</code> to indicate that the channels connected to this gate shall allow spanning records at
	 *        runtime, <code>false</code> otherwise
	 * @param channels
	 *        the list of channel deployment descriptors attached to this gate
	 */
	public GateDeploymentDescriptor(final GateID gateID, final ChannelType channelType,
			final CompressionLevel compressionLevel, final boolean allowSpanningRecords,
			List<ChannelDeploymentDescriptor> channels) {

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
		this.allowSpanningRecords = allowSpanningRecords;
		this.channels = channels;
	}

	/**
	 * Default constructor required by kryo.
	 */
	@SuppressWarnings("unused")
	private GateDeploymentDescriptor() {

		this.gateID = null;
		this.channelType = null;
		this.compressionLevel = null;
		this.channels = null;
	}

	/**
	 * Returns the ID of the gate.
	 * 
	 * @return the ID of the gate
	 */
	public GateID getGateID() {

		return this.gateID;
	}

	/**
	 * Returns the channel type of the gate.
	 * 
	 * @return the channel type of the gate
	 */
	public ChannelType getChannelType() {

		return this.channelType;
	}

	/**
	 * Returns the compression level of the gate.
	 * 
	 * @return the compression level of the gate
	 */
	public CompressionLevel getCompressionLevel() {

		return this.compressionLevel;
	}

	/**
	 * Returns <code>true</code> if the gate shall allow spanning records, <code>false</code> otherwise.
	 * 
	 * @return <code>true</code> if the gate shall allow spanning records, <code>false</code> otherwise
	 */
	public boolean spanningRecordsAllowed() {

		return this.allowSpanningRecords;
	}

	/**
	 * Returns the number of channel deployment descriptors attached to this gate descriptor.
	 * 
	 * @return the number of channel deployment descriptors
	 */
	public int getNumberOfChannelDescriptors() {

		return this.channels.size();
	}

	public ChannelDeploymentDescriptor getChannelDescriptor(final int index) {

		return this.channels.get(index);
	}
}
