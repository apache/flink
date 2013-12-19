/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.nephele.deployment;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import eu.stratosphere.core.io.IOReadableWritable;
import eu.stratosphere.nephele.io.channels.ChannelID;

/**
 * A channel deployment descriptor contains all the information necessary to deploy either an input or an output channel
 * as part of a task on a task manager.
 * <p>
 * This class is not thread-safe in general.
 * 
 */
public final class ChannelDeploymentDescriptor implements IOReadableWritable {

	/**
	 * The ID of the output channel.
	 */
	private final ChannelID outputChannelID;

	/**
	 * The ID of the input channel.
	 */
	private final ChannelID inputChannelID;

	/**
	 * Constructs a new channel deployment descriptor.
	 * 
	 * @param outputChannelID
	 *        the ID of the output channel
	 * @param inputChannelID
	 *        the ID of the input channel
	 */
	public ChannelDeploymentDescriptor(final ChannelID outputChannelID, final ChannelID inputChannelID) {

		if (outputChannelID == null) {
			throw new IllegalArgumentException("Argument outputChannelID must not be null");
		}

		if (inputChannelID == null) {
			throw new IllegalArgumentException("Argument inputChannelID must not be null");
		}

		this.outputChannelID = outputChannelID;
		this.inputChannelID = inputChannelID;
	}

	/**
	 * Default constructor for serialization/deserialization.
	 */
	public ChannelDeploymentDescriptor() {

		this.outputChannelID = new ChannelID();
		this.inputChannelID = new ChannelID();
	}


	@Override
	public void write(final DataOutput out) throws IOException {

		this.outputChannelID.write(out);
		this.inputChannelID.write(out);
	}


	@Override
	public void read(final DataInput in) throws IOException {

		this.outputChannelID.read(in);
		this.inputChannelID.read(in);
	}

	/**
	 * Returns the output channel ID attached to this deployment descriptor.
	 * 
	 * @return the output channel ID attached to this deployment descriptor
	 */
	public ChannelID getOutputChannelID() {

		return this.outputChannelID;
	}

	/**
	 * Returns the input channel ID attached to this deployment descriptor.
	 * 
	 * @return the input channel ID attached to this deployment descriptor
	 */
	public ChannelID getInputChannelID() {

		return this.inputChannelID;
	}
}
