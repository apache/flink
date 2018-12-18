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
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.cep.nfa.sharedbuffer.EventId;
import org.apache.flink.cep.nfa.sharedbuffer.NodeId;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;

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

	private NFAStateSerializer() {
	}

	public static final NFAStateSerializer INSTANCE = new NFAStateSerializer();

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

	private static final StringSerializer STATE_NAME_SERIALIZER = StringSerializer.INSTANCE;
	private static final LongSerializer TIMESTAMP_SERIALIZER = LongSerializer.INSTANCE;
	private static final DeweyNumber.DeweyNumberSerializer VERSION_SERIALIZER = DeweyNumber.DeweyNumberSerializer.INSTANCE;
	private static final NodeId.NodeIdSerializer NODE_ID_SERIALIZER = NodeId.NodeIdSerializer.INSTANCE;
	private static final EventId.EventIdSerializer EVENT_ID_SERIALIZER = EventId.EventIdSerializer.INSTANCE;

	@Override
	public void serialize(NFAState record, DataOutputView target) throws IOException {
		serializeComputationStates(record.getPartialMatches(), target);
		serializeComputationStates(record.getCompletedMatches(), target);
	}

	private void serializeComputationStates(Queue<ComputationState> states, DataOutputView target) throws IOException {
		target.writeInt(states.size());
		for (ComputationState computationState : states) {
			STATE_NAME_SERIALIZER.serialize(computationState.getCurrentStateName(), target);
			NODE_ID_SERIALIZER.serialize(computationState.getPreviousBufferEntry(), target);

			VERSION_SERIALIZER.serialize(computationState.getVersion(), target);
			TIMESTAMP_SERIALIZER.serialize(computationState.getStartTimestamp(), target);
			if (computationState.getStartEventID() != null) {
				target.writeByte(1);
				EVENT_ID_SERIALIZER.serialize(computationState.getStartEventID(), target);
			} else {
				target.writeByte(0);
			}
		}
	}

	@Override
	public NFAState deserialize(DataInputView source) throws IOException {
		PriorityQueue<ComputationState> partialMatches = deserializeComputationStates(source);
		PriorityQueue<ComputationState> completedMatches = deserializeComputationStates(source);
		return new NFAState(partialMatches, completedMatches);
	}

	private PriorityQueue<ComputationState> deserializeComputationStates(DataInputView source) throws IOException {
		PriorityQueue<ComputationState> computationStates = new PriorityQueue<>(NFAState.COMPUTATION_STATE_COMPARATOR);

		int computationStateNo = source.readInt();
		for (int i = 0; i < computationStateNo; i++) {
			String state = STATE_NAME_SERIALIZER.deserialize(source);
			NodeId prevState = NODE_ID_SERIALIZER.deserialize(source);
			DeweyNumber version = VERSION_SERIALIZER.deserialize(source);
			long startTimestamp = TIMESTAMP_SERIALIZER.deserialize(source);

			byte isNull = source.readByte();
			EventId startEventId = null;
			if (isNull == 1) {
				startEventId = EVENT_ID_SERIALIZER.deserialize(source);
			}

			computationStates.add(ComputationState.createState(state, prevState, version, startTimestamp, startEventId));
		}
		return computationStates;
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
			String state = STATE_NAME_SERIALIZER.deserialize(source);
			STATE_NAME_SERIALIZER.serialize(state, target);
			NodeId prevState = NODE_ID_SERIALIZER.deserialize(source);
			NODE_ID_SERIALIZER.serialize(prevState, target);
			DeweyNumber version = VERSION_SERIALIZER.deserialize(source);
			VERSION_SERIALIZER.serialize(version, target);
			long startTimestamp = TIMESTAMP_SERIALIZER.deserialize(source);
			TIMESTAMP_SERIALIZER.serialize(startTimestamp, target);

			byte isNull = source.readByte();
			target.writeByte(isNull);

			if (isNull == 1) {
				EventId startEventId = EVENT_ID_SERIALIZER.deserialize(source);
				EVENT_ID_SERIALIZER.serialize(startEventId, target);
			}
		}
	}

	@Override
	public boolean canEqual(Object obj) {
		return true;
	}

}
