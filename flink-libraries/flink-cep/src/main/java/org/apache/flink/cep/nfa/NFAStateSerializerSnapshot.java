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

package org.apache.flink.cep.nfa;

import org.apache.flink.api.common.typeutils.CompositeTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.cep.nfa.sharedbuffer.EventId;
import org.apache.flink.cep.nfa.sharedbuffer.NodeId;

/**
 * Snapshot class for {@link NFAStateSerializer}.
 */
public class NFAStateSerializerSnapshot extends CompositeTypeSerializerSnapshot<NFAState, NFAStateSerializer> {

	private static final int CURRENT_VERSION = 1;

	/**
	 * Constructor for read instantiation.
	 */
	public NFAStateSerializerSnapshot() {
		super(NFAStateSerializer.class);
	}

	/**
	 * Constructor to create the snapshot for writing.
	 */
	public NFAStateSerializerSnapshot(NFAStateSerializer serializerInstance) {
		super(serializerInstance);
	}

	@Override
	protected int getCurrentOuterSnapshotVersion() {
		return CURRENT_VERSION;
	}

	@Override
	protected TypeSerializer<?>[] getNestedSerializers(NFAStateSerializer outerSerializer) {
		TypeSerializer<DeweyNumber> versionSerializer = outerSerializer.getVersionSerializer();
		TypeSerializer<NodeId> nodeIdSerializer = outerSerializer.getNodeIdSerializer();
		TypeSerializer<EventId> eventIdSerializer = outerSerializer.getEventIdSerializer();

		return new TypeSerializer[]{versionSerializer, nodeIdSerializer, eventIdSerializer};
	}

	@Override
	protected NFAStateSerializer createOuterSerializerWithNestedSerializers(TypeSerializer<?>[] nestedSerializers) {

		@SuppressWarnings("unchecked")
		TypeSerializer<DeweyNumber> versionSerializer = (TypeSerializer<DeweyNumber>) nestedSerializers[0];

		@SuppressWarnings("unchecked")
		TypeSerializer<NodeId> nodeIdSerializer = (TypeSerializer<NodeId>) nestedSerializers[1];

		@SuppressWarnings("unchecked")
		TypeSerializer<EventId> eventIdSerializer = (TypeSerializer<EventId>) nestedSerializers[2];

		return new NFAStateSerializer(versionSerializer, nodeIdSerializer, eventIdSerializer);
	}
}
