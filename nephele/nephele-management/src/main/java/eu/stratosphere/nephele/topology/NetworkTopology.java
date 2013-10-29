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

package eu.stratosphere.nephele.topology;

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import eu.stratosphere.nephele.io.IOReadableWritable;

public class NetworkTopology implements IOReadableWritable {

	private static final String DEFAULT_ROOT_NODE_NAME = "root";

	private static final String SEPARATOR = "/";

	private final NetworkNode rootNode;

	private final Map<String, NetworkNode> nodeMap = new HashMap<String, NetworkNode>();

	private Object attachment = null;

	public NetworkTopology() {
		this.rootNode = new NetworkNode(DEFAULT_ROOT_NODE_NAME, null, this);
	}

	public static NetworkTopology fromFile(final File topologyFile) throws IOException {

		// First create root node and topology object
		final NetworkTopology topology = new NetworkTopology();

		// Now read the topology file
		final FileInputStream fstream = new FileInputStream(topologyFile);
		final DataInputStream in = new DataInputStream(fstream);
		final BufferedReader br = new BufferedReader(new InputStreamReader(in));
		String strLine;
		final Pattern pattern = Pattern.compile("^(\\S+)\\s*(\\S*)\\s*$");

		try {

			while ((strLine = br.readLine()) != null && !strLine.isEmpty()) {

				final Matcher m = pattern.matcher(strLine);
				if (!m.matches()) {
					throw new IOException("Cannot extract topology information from line \"" + strLine + "\"");
				}

				strLine = m.group(1);

				// Remove leading SEPARATOR
				if (strLine.charAt(0) == SEPARATOR.charAt(0)) {
					strLine = strLine.substring(1);
				}

				final String[] splits = strLine.split(SEPARATOR);

				NetworkNode previousNode = topology.getRootNode();
				for (int i = 0; i < splits.length; i++) {

					NetworkNode networkNode = topology.getNodeByName(splits[i]);
					if (networkNode == null) {
						networkNode = new NetworkNode(splits[i], previousNode, topology);
					}

					previousNode = networkNode;
				}
			}
		} finally {
			// Close reader
			br.close();
		}

		return topology;
	}

	public static NetworkTopology fromFile(final String topologyFileName) throws IOException {

		return fromFile(new File(topologyFileName));
	}

	public static NetworkTopology createEmptyTopology() {

		return new NetworkTopology();
	}

	void addNode(final NetworkNode networkNode) {

		this.nodeMap.put(networkNode.getName(), networkNode);
	}

	public NetworkNode getNodeByName(final String name) {

		return this.nodeMap.get(name);
	}

	public NetworkNode getRootNode() {

		return this.rootNode;
	}

	@Override
	public void read(final DataInput in) throws IOException {

		this.rootNode.read(in);

	}

	@Override
	public void write(final DataOutput out) throws IOException {

		this.rootNode.write(out);

	}

	public int getDepth() {

		return this.rootNode.getHeight();

	}

	@Override
	public String toString() {

		return this.rootNode.toString();

	}

	void removeNode(NetworkNode networkNode) {
		this.nodeMap.remove(networkNode.getName());
	}

	public Iterator<NetworkNode> iterator() {

		return new NetworkTopologyIterator(this);
	}

	public void setAttachment(Object attachment) {
		this.attachment = attachment;
	}

	public Object getAttachment() {
		return this.attachment;
	}
}
