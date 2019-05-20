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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.cep.nfa.sharedbuffer.EventId;
import org.apache.flink.cep.nfa.sharedbuffer.NodeId;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.types.StringValue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.PriorityQueue;
import java.util.Queue;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link TypeSerializer} for {@link NFAState}.
 */
public class NFAStateSerializer extends TypeSerializerSingleton<NFAState> {

	private static final long serialVersionUID = 2098282423980597010L;

	/**
	 * NOTE: this field should actually be final.
	 * The reason that it isn't final is due to backward compatible deserialization
	 * paths. See {@link #readObject(ObjectInputStream)}.
	 */
	private TypeSerializer<DeweyNumber> versionSerializer;
	private TypeSerializer<NodeId> nodeIdSerializer;
	private TypeSerializer<EventId> eventIdSerializer;

	public NFAStateSerializer() {
		this.versionSerializer = DeweyNumber.DeweyNumberSerializer.INSTANCE;
		this.eventIdSerializer = EventId.EventIdSerializer.INSTANCE;
		this.nodeIdSerializer = new NodeId.NodeIdSerializer();
	}

	NFAStateSerializer(
			final TypeSerializer<DeweyNumber> versionSerializer,
			final TypeSerializer<NodeId> nodeIdSerializer,
			final TypeSerializer<EventId> eventIdSerializer) {
		this.versionSerializer = checkNotNull(versionSerializer);
		this.nodeIdSerializer = checkNotNull(nodeIdSerializer);
		this.eventIdSerializer = checkNotNull(eventIdSerializer);
	}

	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public NFAState createInstance() {
		return null;
	}

	@Override
	public NFAState copy(NFAState from) {
		try {
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			serialize(from, new DataOutputViewStreamWrapper(baos));
			baos.close();

			byte[] data = baos.toByteArray();

			ByteArrayInputStream bais = new ByteArrayInputStream(data);
			NFAState copy = deserialize(new DataInputViewStreamWrapper(bais));
			bais.close();
			return copy;
		} catch (IOException e) {
			throw new RuntimeException("Could not copy NFA.", e);
		}
	}

	@Override
	public NFAState copy(NFAState from, NFAState reuse) {
		return copy(from);
	}

	@Override
	public int getLength() {
		return -1;
	}

	@Override
	public void serialize(NFAState record, DataOutputView target) throws IOException {
		serializeComputationStates(record.getPartialMatches(), target);
		serializeComputationStates(record.getCompletedMatches(), target);
	}

	@Override
	public NFAState deserialize(DataInputView source) throws IOException {
		PriorityQueue<ComputationState> partialMatches = deserializeComputationStates(source);
		PriorityQueue<ComputationState> completedMatches = deserializeComputationStates(source);
		return new NFAState(partialMatches, completedMatches);
	}

	@Override
	public NFAState deserialize(NFAState reuse, DataInputView source) throws IOException {
		return deserialize(source);
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		copyStates(source, target); // copy partial matches
		copyStates(source, target); // copy completed matches
	}

	private void copyStates(DataInputView source, DataOutputView target) throws IOException {
		int computationStateNo = source.readInt();
		target.writeInt(computationStateNo);

		for (int i = 0; i < computationStateNo; i++) {
			copySingleComputationState(source, target);
		}
	}

	@Override
	public TypeSerializerSnapshot<NFAState> snapshotConfiguration() {
		return new NFAStateSerializerSnapshot(this);
	}

	/*
		Getters for internal serializers to use in NFAStateSerializerSnapshot.
	 */

	TypeSerializer<DeweyNumber> getVersionSerializer() {
		return versionSerializer;
	}

	TypeSerializer<NodeId> getNodeIdSerializer() {
		return nodeIdSerializer;
	}

	TypeSerializer<EventId> getEventIdSerializer() {
		return eventIdSerializer;
	}

	/*
		De/serialization methods
	 */

	private void serializeComputationStates(Queue<ComputationState> states, DataOutputView target) throws IOException {
		target.writeInt(states.size());
		for (ComputationState computationState : states) {
			serializeSingleComputationState(computationState, target);
		}
	}

	private PriorityQueue<ComputationState> deserializeComputationStates(DataInputView source) throws IOException {
		PriorityQueue<ComputationState> computationStates = new PriorityQueue<>(NFAState.COMPUTATION_STATE_COMPARATOR);

		int computationStateNo = source.readInt();
		for (int i = 0; i < computationStateNo; i++) {
			final ComputationState computationState = deserializeSingleComputationState(source);
			computationStates.add(computationState);
		}
		return computationStates;
	}

	private void serializeSingleComputationState(
			ComputationState computationState,
			DataOutputView target) throws IOException {

		StringValue.writeString(computationState.getCurrentStateName(), target);
		nodeIdSerializer.serialize(computationState.getPreviousBufferEntry(), target);
		versionSerializer.serialize(computationState.getVersion(), target);
		target.writeLong(computationState.getStartTimestamp());
		serializeStartEvent(computationState.getStartEventID(), target);
	}

	private ComputationState deserializeSingleComputationState(DataInputView source) throws IOException {
		String stateName = StringValue.readString(source);
		NodeId prevState = nodeIdSerializer.deserialize(source);
		DeweyNumber version = versionSerializer.deserialize(source);
		long startTimestamp = source.readLong();

		EventId startEventId = deserializeStartEvent(source);

		return ComputationState.createState(stateName,
			prevState,
			version,
			startTimestamp,
			startEventId);
	}

	private void copySingleComputationState(DataInputView source, DataOutputView target) throws IOException {
		StringValue.copyString(source, target);
		NodeId prevState = nodeIdSerializer.deserialize(source);
		nodeIdSerializer.serialize(prevState, target);
		DeweyNumber version = versionSerializer.deserialize(source);
		versionSerializer.serialize(version, target);
		long startTimestamp = source.readLong();
		target.writeLong(startTimestamp);

		copyStartEvent(source, target);
	}

	private void serializeStartEvent(EventId startEventID, DataOutputView target) throws IOException {
		if (startEventID != null) {
			target.writeByte(1);
			eventIdSerializer.serialize(startEventID, target);
		} else {
			target.writeByte(0);
		}
	}

	private EventId deserializeStartEvent(DataInputView source) throws IOException {
		byte isNull = source.readByte();
		EventId startEventId = null;
		if (isNull == 1) {
			startEventId = eventIdSerializer.deserialize(source);
		}
		return startEventId;
	}

	private void copyStartEvent(DataInputView source, DataOutputView target) throws IOException {
		byte isNull = source.readByte();
		target.writeByte(isNull);

		if (isNull == 1) {
			EventId startEventId = eventIdSerializer.deserialize(source);
			eventIdSerializer.serialize(startEventId, target);
		}
	}

	/*
	* Backwards compatible deserializing of NFAStateSerializer.
	*/
	private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
		in.defaultReadObject();

		// the nested serializer will be null if this was read from a savepoint taken with versions
		// lower than Flink 1.7; in this case, we explicitly create instance for the nested serializer.
		if (versionSerializer == null || nodeIdSerializer == null || eventIdSerializer == null) {
			this.versionSerializer = DeweyNumber.DeweyNumberSerializer.INSTANCE;
			this.eventIdSerializer = EventId.EventIdSerializer.INSTANCE;
			this.nodeIdSerializer = new NodeId.NodeIdSerializer();
		}
	}
}
