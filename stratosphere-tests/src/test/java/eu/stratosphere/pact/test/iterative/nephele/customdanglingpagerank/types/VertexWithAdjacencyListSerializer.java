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
package eu.stratosphere.pact.test.iterative.nephele.customdanglingpagerank.types;

import java.io.IOException;

import eu.stratosphere.api.typeutils.TypeSerializer;
import eu.stratosphere.core.memory.DataInputView;
import eu.stratosphere.core.memory.DataOutputView;


/**
 *
 */
public final class VertexWithAdjacencyListSerializer extends TypeSerializer<VertexWithAdjacencyList> {

	@Override
	public VertexWithAdjacencyList createInstance() {
		return new VertexWithAdjacencyList();
	}

	@Override
	public VertexWithAdjacencyList createCopy(VertexWithAdjacencyList from) {
		long[] targets = new long[from.getTargets().length];
		System.arraycopy(from.getTargets(), 0, targets, 0, targets.length);
		
		VertexWithAdjacencyList copy = new VertexWithAdjacencyList();
		copy.setVertexID(from.getVertexID());
		copy.setNumTargets(from.getNumTargets());
		copy.setTargets(targets);
		return copy;
	}

	@Override
	public void copyTo(VertexWithAdjacencyList from, VertexWithAdjacencyList to) {
		if (to.getTargets().length < from.getTargets().length) {
			to.setTargets(new long[from.getTargets().length]);
		}
		
		to.setVertexID(from.getVertexID());
		to.setNumTargets(from.getNumTargets());
		System.arraycopy(from.getTargets(), 0, to.getTargets(), 0, from.getNumTargets());
	}

	@Override
	public int getLength() {
		return -1;
	}

	@Override
	public void serialize(VertexWithAdjacencyList record, DataOutputView target) throws IOException {
		target.writeLong(record.getVertexID());
		
		final long[] targets = record.getTargets();
		final int numTargets = record.getNumTargets();
		target.writeInt(numTargets);
		
		for (int i = 0; i < numTargets; i++) {
			target.writeLong(targets[i]);
		}
	}

	@Override
	public void deserialize(VertexWithAdjacencyList target, DataInputView source) throws IOException {
		target.setVertexID(source.readLong());
		
		final int numTargets = source.readInt();
		long[] targets = target.getTargets();
		if (targets.length < numTargets) {
			targets = new long[numTargets];
			target.setTargets(targets);
		}
		
		target.setNumTargets(numTargets);
		
		for (int i = 0; i < numTargets; i++) {
			targets[i] = source.readLong();
		}
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.generic.types.TypeSerializer#copy(eu.stratosphere.nephele.services.memorymanager.DataInputView, eu.stratosphere.nephele.services.memorymanager.DataOutputView)
	 */
	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		target.write(source, 8);
		
		final int numTargets = source.readInt();
		target.writeInt(numTargets);
		target.write(source, numTargets * 8);
	}
}
