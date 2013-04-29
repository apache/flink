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

package eu.stratosphere.nephele.executiongraph;

import eu.stratosphere.nephele.io.channels.AbstractInputChannel;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.io.channels.ChannelType;
import eu.stratosphere.nephele.io.compression.CompressionLevel;

/**
 * Objects of this class represent a pair of {@link AbstractInputChannel} and {@link AbstractOutputChannel} objects
 * within an {@link ExecutionGraph}, Nephele's internal scheduling representation for jobs.
 * 
 * @author warneke
 */
public final class ExecutionEdge {

	private final ExecutionGroupEdge groupEdge;

	private final ExecutionGate outputGate;

	private final ExecutionGate inputGate;

	private final ChannelID outputChannelID;

	private final ChannelID inputChannelID;

	private final int outputGateIndex;

	private final int inputGateIndex;

	ExecutionEdge(final ExecutionGate outputGate, final ExecutionGate inputGate, final ExecutionGroupEdge groupEdge,
			final ChannelID outputChannelID, final ChannelID inputChannelID, final int outputGateIndex,
			final int inputGateIndex) {

		this.outputGate = outputGate;
		this.inputGate = inputGate;
		this.groupEdge = groupEdge;
		this.outputChannelID = outputChannelID;
		this.inputChannelID = inputChannelID;
		this.outputGateIndex = outputGateIndex;
		this.inputGateIndex = inputGateIndex;
	}

	public ExecutionGate getInputGate() {

		return this.inputGate;
	}

	public ExecutionGate getOutputGate() {

		return this.outputGate;
	}

	public ChannelID getOutputChannelID() {

		return this.outputChannelID;
	}

	public ChannelID getInputChannelID() {

		return this.inputChannelID;
	}

	public int getOutputGateIndex() {

		return this.outputGateIndex;
	}

	public int getInputGateIndex() {

		return this.inputGateIndex;
	}
	
	public ChannelType getChannelType() {
		
		return this.groupEdge.getChannelType();
	}
	
	public CompressionLevel getCompressionLevel() {
		
		return this.groupEdge.getCompressionLevel();
	}
	
	public boolean isBroadcast() {
		
		return this.groupEdge.isBroadcast();
	}
	
	public int getConnectionID() {
		
		return this.groupEdge.getConnectionID();
	}
}
