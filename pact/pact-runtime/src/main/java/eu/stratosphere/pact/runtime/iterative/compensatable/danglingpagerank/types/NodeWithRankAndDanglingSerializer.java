/***********************************************************************************************************************
 *
 * Copyright (C) 2012 by the Stratosphere project (http://stratosphere.eu)
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
package eu.stratosphere.pact.runtime.iterative.compensatable.danglingpagerank.types;

import java.io.IOException;

import eu.stratosphere.nephele.services.memorymanager.DataInputView;
import eu.stratosphere.nephele.services.memorymanager.DataOutputView;
import eu.stratosphere.pact.generic.types.TypeSerializer;


/**
 *
 */
public final class NodeWithRankAndDanglingSerializer implements TypeSerializer<NodeWithRankAndDangling> {

	@Override
	public NodeWithRankAndDangling createInstance() {
		return new NodeWithRankAndDangling();
	}

	@Override
	public NodeWithRankAndDangling createCopy(NodeWithRankAndDangling from) {
		NodeWithRankAndDangling n = new NodeWithRankAndDangling();
		copyTo(from, n);
		return n;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.generic.types.TypeSerializer#copyTo(java.lang.Object, java.lang.Object)
	 */
	@Override
	public void copyTo(NodeWithRankAndDangling from, NodeWithRankAndDangling to) {
		to.setNodeId(from.getNodeId());
		to.setRank(from.getRank());
		to.setDangling(from.isDangling());
	}

	@Override
	public int getLength() {
		return 17;
	}

	@Override
	public void serialize(NodeWithRankAndDangling record, DataOutputView target) throws IOException {
		target.writeLong(record.getNodeId());
		target.writeDouble(record.getRank());
		target.writeBoolean(record.isDangling());
	}

	@Override
	public void deserialize(NodeWithRankAndDangling target, DataInputView source) throws IOException {
		target.setNodeId(source.readLong());
		target.setRank(source.readDouble());
		target.setDangling(source.readBoolean());
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		target.write(source, 17);
	}
}
