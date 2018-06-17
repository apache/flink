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

import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.util.Objects;

/**
 * Unique identifier for {@link SharedBufferNode}.
 */
public class NodeId {

	private final String pageName;
	private final EventId eventId;

	public NodeId(EventId eventId, String pageName) {
		this.eventId = eventId;
		this.pageName = pageName;
	}

	public EventId getEventId() {
		return eventId;
	}

	public String getPageName() {
		return pageName;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		NodeId nodeId = (NodeId) o;
		return Objects.equals(eventId, nodeId.eventId) &&
			Objects.equals(pageName, nodeId.pageName);
	}

	@Override
	public int hashCode() {
		return Objects.hash(eventId, pageName);
	}

	@Override
	public String toString() {
		return "NodeId{" +
			"eventId=" + eventId +
			", pageName='" + pageName + '\'' +
			'}';
	}

	/** Serializer for {@link NodeId}. */
	public static class NodeIdSerializer extends TypeSerializerSingleton<NodeId> {

		private static final long serialVersionUID = 9209498028181378582L;

		public static final NodeIdSerializer INSTANCE = new NodeIdSerializer();

		private NodeIdSerializer() {
		}

		@Override
		public boolean isImmutableType() {
			return true;
		}

		@Override
		public NodeId createInstance() {
			return null;
		}

		@Override
		public NodeId copy(NodeId from) {
			return new NodeId(from.eventId, from.pageName);
		}

		@Override
		public NodeId copy(NodeId from, NodeId reuse) {
			return copy(from);
		}

		@Override
		public int getLength() {
			return -1;
		}

		@Override
		public void serialize(NodeId record, DataOutputView target) throws IOException {
			if (record != null) {
				target.writeByte(1);
				EventId.EventIdSerializer.INSTANCE.serialize(record.eventId, target);
				StringSerializer.INSTANCE.serialize(record.pageName, target);
			} else {
				target.writeByte(0);
			}
		}

		@Override
		public NodeId deserialize(DataInputView source) throws IOException {
			byte b = source.readByte();
			if (b == 0) {
				return null;
			}

			EventId eventId = EventId.EventIdSerializer.INSTANCE.deserialize(source);
			String pageName = StringSerializer.INSTANCE.deserialize(source);
			return new NodeId(eventId, pageName);
		}

		@Override
		public NodeId deserialize(NodeId reuse, DataInputView source) throws IOException {
			return deserialize(source);
		}

		@Override
		public void copy(DataInputView source, DataOutputView target) throws IOException {
			target.writeByte(source.readByte());

			LongSerializer.INSTANCE.copy(source, target); // eventId
			LongSerializer.INSTANCE.copy(source, target); // timestamp
			StringSerializer.INSTANCE.copy(source, target); // pageName
		}

		@Override
		public boolean canEqual(Object obj) {
			return obj.getClass().equals(NodeIdSerializer.class);
		}

	}
}
