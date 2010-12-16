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

package eu.stratosphere.nephele.instance.local;

import java.util.HashMap;
import java.util.Map;

import eu.stratosphere.nephele.instance.AbstractInstance;
import eu.stratosphere.nephele.instance.InstanceConnectionInfo;
import eu.stratosphere.nephele.instance.InstanceType;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.topology.NetworkNode;
import eu.stratosphere.nephele.topology.NetworkTopology;

public class LocalInstance extends AbstractInstance {

	private final Map<ChannelID, String> filenames = new HashMap<ChannelID, String>();

	public LocalInstance(InstanceType instanceType, InstanceConnectionInfo instanceConnectionInfo,
			NetworkNode parentNode, NetworkTopology networkTopology) {
		super(instanceType, instanceConnectionInfo, parentNode, networkTopology);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String getUniqueFilename(ChannelID id) {

		if (this.filenames.containsKey(id))
			return this.filenames.get(id);

		// Simple implementation to generate a random filename
		char[] alphabet = { 'a', 'b', 'c', 'd', 'e', 'f', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9' };

		String filename = "ne";

		for (int i = 0; i < 16; i++) {
			filename += alphabet[(int) (Math.random() * alphabet.length)];
		}

		filename += ".dat";
		// Store filename with id
		this.filenames.put(id, filename);

		return filename;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String toString() {

		return this.getInstanceConnectionInfo().toString();
	}

}
