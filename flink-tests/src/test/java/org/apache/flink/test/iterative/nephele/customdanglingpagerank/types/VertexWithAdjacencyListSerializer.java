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

package org.apache.flink.test.iterative.nephele.customdanglingpagerank.types;

import java.io.IOException;

import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

public final class VertexWithAdjacencyListSerializer extends TypeSerializerSingleton<VertexWithAdjacencyList> {

	private static final long serialVersionUID = 1L;
	
	
	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public boolean isStateful() {
		return false;
	}
	
	@Override
	public VertexWithAdjacencyList createInstance() {
		return new VertexWithAdjacencyList();
	}

	@Override
	public VertexWithAdjacencyList copy(VertexWithAdjacencyList from) {
		VertexWithAdjacencyList copy = new VertexWithAdjacencyList(from.getVertexID(), new long[from.getNumTargets()]);
		copy.setNumTargets(from.getNumTargets());
		System.arraycopy(from.getTargets(), 0, copy.getTargets(), 0, from.getNumTargets());
		return copy;
	}
	
	@Override
	public VertexWithAdjacencyList copy(VertexWithAdjacencyList from, VertexWithAdjacencyList reuse) {
		if (reuse.getTargets().length < from.getTargets().length) {
			reuse.setTargets(new long[from.getTargets().length]);
		}
		
		reuse.setVertexID(from.getVertexID());
		reuse.setNumTargets(from.getNumTargets());
		System.arraycopy(from.getTargets(), 0, reuse.getTargets(), 0, from.getNumTargets());
		return reuse;
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
	public VertexWithAdjacencyList deserialize(DataInputView source) throws IOException {
		return deserialize(new VertexWithAdjacencyList(), source);
	}
	
	@Override
	public VertexWithAdjacencyList deserialize(VertexWithAdjacencyList target, DataInputView source) throws IOException {
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
		return target;
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		target.write(source, 8);
		
		final int numTargets = source.readInt();
		target.writeInt(numTargets);
		target.write(source, numTargets * 8);
	}
}
