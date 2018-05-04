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
import org.apache.flink.core.memory.DataInputView;

import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;

class NFASerializationUtils {

	/**
	 * Deserializes {@link Queue} of {@link ComputationState}s. The queue is represented as count of states then follows
	 * n instances of the computational state.
	 *
	 * @param eventSerializer event serializer for deserializing accepted events
	 * @param source          view on the serialized data
	 * @param <T>             type of processed events
	 * @return queue of computation states
	 */
	static <T> Queue<ComputationState<T>> deserializeComputationStates(TypeSerializer<T> eventSerializer,
			DataInputView source) throws IOException {
		Queue<ComputationState<T>> computationStates = new LinkedList<>();
		StringSerializer stateNameSerializer = StringSerializer.INSTANCE;
		LongSerializer timestampSerializer = LongSerializer.INSTANCE;
		DeweyNumber.DeweyNumberSerializer versionSerializer = new DeweyNumber.DeweyNumberSerializer();

		int computationStateNo = source.readInt();
		for (int i = 0; i < computationStateNo; i++) {
			String state = stateNameSerializer.deserialize(source);
			String prevState = stateNameSerializer.deserialize(source);
			long timestamp = timestampSerializer.deserialize(source);
			DeweyNumber version = versionSerializer.deserialize(source);
			long startTimestamp = timestampSerializer.deserialize(source);
			int counter = source.readInt();

			T event = null;
			if (source.readBoolean()) {
				event = eventSerializer.deserialize(source);
			}

			computationStates.add(ComputationState.createState(
				state, prevState, event, counter, timestamp, version, startTimestamp));
		}
		return computationStates;
	}

	private NFASerializationUtils() {
	}
}
