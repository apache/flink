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

import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
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
import java.util.PriorityQueue;
import java.util.Queue;

/**
 * A {@link TypeSerializer} for {@link NFAState} that uses Java Serialization.
 */
public class NFAStateSerializer extends TypeSerializerSingleton<NFAState> {

	private static final long serialVersionUID = 2098282423980597010L;

	public static final NFAStateSerializer INSTANCE = new NFAStateSerializer();

	private static final DeweyNumber.DeweyNumberSerializer VERSION_SERIALIZER = DeweyNumber.DeweyNumberSerializer.INSTANCE;
	private static final NodeId.NodeIdSerializer NODE_ID_SERIALIZER = new NodeId.NodeIdSerializer();
	private static final EventId.EventIdSerializer EVENT_ID_SERIALIZER = EventId.EventIdSerializer.INSTANCE;

	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public NFAStateSerializer duplicate() {
		return new NFAStateSerializer();
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
	public boolean canEqual(Object obj) {
		return true;
	}

	// -----------------------------------------------------------------------------------

	@Override
	public TypeSerializerSnapshot<NFAState> snapshotConfiguration() {
		return new NFAStateSerializerSnapshot();
	}

	private NFAStateSerializer() {
	}

	/**
	 * Serializer configuration snapshot for compatibility and format evolution.
	 */
	@SuppressWarnings("WeakerAccess")
	public static final class NFAStateSerializerSnapshot extends SimpleTypeSerializerSnapshot<NFAState> {
		public NFAStateSerializerSnapshot() {
			super(() -> INSTANCE);
		}
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
		NODE_ID_SERIALIZER.serialize(computationState.getPreviousBufferEntry(), target);
		VERSION_SERIALIZER.serialize(computationState.getVersion(), target);
		target.writeLong(computationState.getStartTimestamp());
		serializeStartEvent(computationState.getStartEventID(), target);
	}

	private ComputationState deserializeSingleComputationState(DataInputView source) throws IOException {
		String stateName = StringValue.readString(source);
		NodeId prevState = NODE_ID_SERIALIZER.deserialize(source);
		DeweyNumber version = VERSION_SERIALIZER.deserialize(source);
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
		NodeId prevState = NODE_ID_SERIALIZER.deserialize(source);
		NODE_ID_SERIALIZER.serialize(prevState, target);
		DeweyNumber version = VERSION_SERIALIZER.deserialize(source);
		VERSION_SERIALIZER.serialize(version, target);
		long startTimestamp = source.readLong();
		target.writeLong(startTimestamp);

		copyStartEvent(source, target);
	}

	private void serializeStartEvent(EventId startEventID, DataOutputView target) throws IOException {
		if (startEventID != null) {
			target.writeByte(1);
			EVENT_ID_SERIALIZER.serialize(startEventID, target);
		} else {
			target.writeByte(0);
		}
	}

	private EventId deserializeStartEvent(DataInputView source) throws IOException {
		byte isNull = source.readByte();
		EventId startEventId = null;
		if (isNull == 1) {
			startEventId = EVENT_ID_SERIALIZER.deserialize(source);
		}
		return startEventId;
	}

	private void copyStartEvent(DataInputView source, DataOutputView target) throws IOException {
		byte isNull = source.readByte();
		target.writeByte(isNull);

		if (isNull == 1) {
			EventId startEventId = EVENT_ID_SERIALIZER.deserialize(source);
			EVENT_ID_SERIALIZER.serialize(startEventId, target);
		}
	}
}
