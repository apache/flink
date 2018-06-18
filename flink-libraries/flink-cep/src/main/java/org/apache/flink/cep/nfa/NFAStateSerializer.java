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
import org.apache.flink.cep.nfa.sharedbuffer.NodeId;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.LinkedList;
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

	@Override
	public void serialize(NFAState record, DataOutputView target) throws IOException {

		target.writeInt(record.getComputationStates().size());

		StringSerializer stateNameSerializer = StringSerializer.INSTANCE;
		LongSerializer timestampSerializer = LongSerializer.INSTANCE;
		DeweyNumber.DeweyNumberSerializer versionSerializer = DeweyNumber.DeweyNumberSerializer.INSTANCE;
		NodeId.NodeIdSerializer nodeIdSerializer = NodeId.NodeIdSerializer.INSTANCE;

		for (ComputationState computationState : record.getComputationStates()) {
			stateNameSerializer.serialize(computationState.getCurrentStateName(), target);
			nodeIdSerializer.serialize(computationState.getPreviousBufferEntry(), target);

			versionSerializer.serialize(computationState.getVersion(), target);
			timestampSerializer.serialize(computationState.getStartTimestamp(), target);
		}
	}

	@Override
	public NFAState deserialize(DataInputView source) throws IOException {
		Queue<ComputationState> computationStates = new LinkedList<>();
		StringSerializer stateNameSerializer = StringSerializer.INSTANCE;
		LongSerializer timestampSerializer = LongSerializer.INSTANCE;
		DeweyNumber.DeweyNumberSerializer versionSerializer = DeweyNumber.DeweyNumberSerializer.INSTANCE;
		NodeId.NodeIdSerializer nodeIdSerializer = NodeId.NodeIdSerializer.INSTANCE;

		int computationStateNo = source.readInt();
		for (int i = 0; i < computationStateNo; i++) {
			String state = stateNameSerializer.deserialize(source);
			NodeId prevState = nodeIdSerializer.deserialize(source);
			DeweyNumber version = versionSerializer.deserialize(source);
			long startTimestamp = timestampSerializer.deserialize(source);

			computationStates.add(ComputationState.createState(state, prevState, version, startTimestamp));
		}
		return new NFAState(computationStates);
	}

	@Override
	public NFAState deserialize(NFAState reuse, DataInputView source) throws IOException {
		return deserialize(source);
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		serialize(deserialize(source), target);
	}

	@Override
	public boolean canEqual(Object obj) {
		return true;
	}

}
