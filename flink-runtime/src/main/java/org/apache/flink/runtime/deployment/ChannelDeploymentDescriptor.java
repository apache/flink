/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.deployment;

import java.io.Serializable;

import org.apache.flink.runtime.executiongraph.ExecutionEdge;
import org.apache.flink.runtime.io.network.channels.ChannelID;

/**
 * A channel deployment descriptor contains all the information necessary to deploy either an input or an output channel
 * as part of a task on a task manager.
 */
public final class ChannelDeploymentDescriptor implements Serializable {

	private static final long serialVersionUID = -4079084629425460213L;

	/** The ID of the output channel. */
	private final ChannelID outputChannelID;

	/** The ID of the input channel. */
	private final ChannelID inputChannelID;

	/**
	 * Constructs a new channel deployment descriptor.
	 * 
	 * @param outputChannelID The ID of the output channel
	 * @param inputChannelID The ID of the input channel
	 */
	public ChannelDeploymentDescriptor(ChannelID outputChannelID, ChannelID inputChannelID) {
		if (outputChannelID == null || inputChannelID == null) {
			throw new IllegalArgumentException("Channel IDs must not be null");
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
	
	// --------------------------------------------------------------------------------------------
	
	public static ChannelDeploymentDescriptor fromExecutionEdge(ExecutionEdge edge) {
		return new ChannelDeploymentDescriptor(edge.getOutputChannelId(), edge.getInputChannelId());
	}
}
