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

package org.apache.flink.cep.nfa.sharedbuffer;

import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.cep.nfa.sharedbuffer.SharedBufferEdge.SharedBufferEdgeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * An entry in {@link SharedBuffer} that allows to store relations between different entries.
 */
public class SharedBufferNode {

	private final List<SharedBufferEdge> edges;
	private final Lock lock;

	public SharedBufferNode(int refCount) {
		this(new ArrayList<>(), refCount);
	}

	private SharedBufferNode(List<SharedBufferEdge> edges, int refCount) {
		this.edges = edges;
		this.lock = new Lock(refCount);
	}

	public List<SharedBufferEdge> getEdges() {
		return edges;
	}

	Lock getLock() {
		return lock;
	}

	public void addEdge(SharedBufferEdge edge) {
		edges.add(edge);
	}

	@Override
	public String toString() {
		return "SharedBufferNode{" +
			"edges=" + edges +
			", lock=" + lock +
			'}';
	}

	/** Serializer for {@link SharedBufferNode}. */
	public static class SharedBufferNodeSerializer extends TypeSerializerSingleton<SharedBufferNode> {

		private static final long serialVersionUID = -6687780732295439832L;

		private final ListSerializer<SharedBufferEdge> edgesSerializer =
			new ListSerializer<>(SharedBufferEdgeSerializer.INSTANCE);

		@Override
		public boolean isImmutableType() {
			return false;
		}

		@Override
		public SharedBufferNode createInstance() {
			return new SharedBufferNode(0);
		}

		@Override
		public SharedBufferNode copy(SharedBufferNode from) {
			return new SharedBufferNode(edgesSerializer.copy(from.edges), from.lock.getRefCounter());
		}

		@Override
		public SharedBufferNode copy(SharedBufferNode from, SharedBufferNode reuse) {
			return copy(from);
		}

		@Override
		public int getLength() {
			return -1;
		}

		@Override
		public void serialize(SharedBufferNode record, DataOutputView target) throws IOException {
			edgesSerializer.serialize(record.edges, target);
			IntSerializer.INSTANCE.serialize(record.lock.getRefCounter(), target);
		}

		@Override
		public SharedBufferNode deserialize(DataInputView source) throws IOException {
			List<SharedBufferEdge> edges = edgesSerializer.deserialize(source);
			Integer refCount = IntSerializer.INSTANCE.deserialize(source);
			return new SharedBufferNode(edges, refCount);
		}

		@Override
		public SharedBufferNode deserialize(SharedBufferNode reuse, DataInputView source) throws IOException {
			return deserialize(source);
		}

		@Override
		public void copy(DataInputView source, DataOutputView target) throws IOException {
			edgesSerializer.copy(source, target);
			IntSerializer.INSTANCE.copy(source, target);
		}

		@Override
		public boolean canEqual(Object obj) {
			return obj.getClass().equals(SharedBufferNodeSerializer.class);
		}
	}
}
