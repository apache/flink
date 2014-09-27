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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.executiongraph.ExecutionEdge;

/**
 * A gate deployment descriptor contains the deployment descriptors for the channels associated with that gate.
 */
public final class GateDeploymentDescriptor implements IOReadableWritable {

	/** The list of channel deployment descriptors attached to this gate. */
	private final List<ChannelDeploymentDescriptor> channels;

	/**
	 * Constructs a new gate deployment descriptor
	 * 
	 * @param channels
	 *        the list of channel deployment descriptors attached to this gate
	 */
	public GateDeploymentDescriptor(List<ChannelDeploymentDescriptor> channels) {
		if (channels == null) {
			throw new NullPointerException();
		}

		this.channels = channels;
	}

	/**
	 * Default constructor for serialization/deserialization.
	 */
	public GateDeploymentDescriptor() {
		this.channels = new ArrayList<ChannelDeploymentDescriptor>();
	}

	
	public List<ChannelDeploymentDescriptor> getChannels() {
		return channels;
	}
	
	// --------------------------------------------------------------------------------------------
	
	@Override
	public void write(final DataOutputView out) throws IOException {
		out.writeInt(this.channels.size());
		for (ChannelDeploymentDescriptor cdd : this.channels) {
			cdd.write(out);
		}
	}

	@Override
	public void read(final DataInputView in) throws IOException {
		final int nocdd = in.readInt();
		for (int i = 0; i < nocdd; ++i) {
			ChannelDeploymentDescriptor cdd = new ChannelDeploymentDescriptor();
			cdd.read(in);
			this.channels.add(cdd);
		}
	}
	
	// --------------------------------------------------------------------------------------------
	
	public static GateDeploymentDescriptor fromEdges(List<ExecutionEdge> edges) {
		List<ChannelDeploymentDescriptor> channels = new ArrayList<ChannelDeploymentDescriptor>(edges.size());
		for (ExecutionEdge edge : edges) {
			channels.add(ChannelDeploymentDescriptor.fromExecutionEdge(edge));
		}
		return new GateDeploymentDescriptor(channels);
	}
	
	public static GateDeploymentDescriptor fromEdges(ExecutionEdge[] edges) {
		List<ChannelDeploymentDescriptor> channels = new ArrayList<ChannelDeploymentDescriptor>(edges.length);
		for (ExecutionEdge edge : edges) {
			channels.add(ChannelDeploymentDescriptor.fromExecutionEdge(edge));
		}
		return new GateDeploymentDescriptor(channels);
	}
}
