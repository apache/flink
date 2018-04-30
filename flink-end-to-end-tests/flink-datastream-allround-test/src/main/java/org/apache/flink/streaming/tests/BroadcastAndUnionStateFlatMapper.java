/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.tests;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * A self-verifiable {@link RichCoFlatMapFunction} that uses broadcast and union state.
 *
 * <p>For verifying broadcast state, the number of broadcast elements as well as the elements
 * themselves are kept as broadcast state, and verified for correctness on restore.
 *
 * <p>For verifying union state, each subtask of this operator only stores a subset of all
 * elements from the broadcast stream (subsets determined using simple round-robin partitioning).
 * On restore, the restored union state should contain all elements from the broadcast stream.
 *
 * <p>Elements from the non-broadcasted stream are simply forwarded.
 */
public class BroadcastAndUnionStateFlatMapper extends RichCoFlatMapFunction<Event, Event, Event> implements CheckpointedFunction {

	private static final long serialVersionUID = 4190630048584561485L;

	private static final String NUM_BROADCAST_ELEMENTS_KEY = "numBroadcastElements";

	private transient BroadcastState<Long, Long> broadcastElementsState;
	private transient BroadcastState<String, Integer> numBroadcastElementsState;

	private transient ListState<Event> unionElementsState;

	/**
	 * Broadcasted events stream.
	 */
	@Override
	public void flatMap1(Event value, Collector<Event> out) throws Exception {
		Integer numBroadcastElements = numBroadcastElementsState.get(NUM_BROADCAST_ELEMENTS_KEY);
		if (numBroadcastElements == null) {
			numBroadcastElements = 0;
		}

		broadcastElementsState.put(value.getSequenceNumber(), value.getSequenceNumber() * 2);
		numBroadcastElementsState.put(NUM_BROADCAST_ELEMENTS_KEY, numBroadcastElements + 1);

		// each subtask only adds a portion of broadcasted elements to the union state (round-robin partitioned)
		if (value.getSequenceNumber() %  getRuntimeContext().getNumberOfParallelSubtasks() == getRuntimeContext().getIndexOfThisSubtask()) {
			unionElementsState.add(value);
		}
	}

	@Override
	public void flatMap2(Event value, Collector<Event> out) throws Exception {
		out.collect(value);
	}

	@Override
	public void initializeState(FunctionInitializationContext context) throws Exception {
		this.broadcastElementsState = context.getOperatorStateStore()
			.getBroadcastState(new MapStateDescriptor<>("broadcastElementsState", LongSerializer.INSTANCE, LongSerializer.INSTANCE));

		this.numBroadcastElementsState = context.getOperatorStateStore()
			.getBroadcastState(new MapStateDescriptor<>("numBroadcastElementsState", StringSerializer.INSTANCE, IntSerializer.INSTANCE));

		this.unionElementsState = context.getOperatorStateStore()
			.getUnionListState(new ListStateDescriptor<Event>("unionListState", Event.class));

		if (context.isRestored()) {
			Integer numBroadcastElements = numBroadcastElementsState.get(NUM_BROADCAST_ELEMENTS_KEY);

			Preconditions.checkState(numBroadcastElements != null);

			// -- verification for broadcast state --
			Set<Long> expected = new HashSet<>();
			for (long i = 0; i < numBroadcastElements; i++) {
				expected.add(i + 1);
			}

			for (Map.Entry<Long, Long> broadcastElementEntry : broadcastElementsState.entries()) {
				long key = broadcastElementEntry.getKey();
				Preconditions.checkState(expected.remove(key), "Unexpected keys in restored broadcast state.");
				Preconditions.checkState(broadcastElementEntry.getValue() == key * 2, "Incorrect value in restored broadcast state.");
			}

			Preconditions.checkState(expected.size() == 0, "Missing keys in restored broadcast state.");

			// -- verification for union state --
			for (long i = 0; i < numBroadcastElements; i++) {
				expected.add(i + 1);
			}

			for (Event unionedEvent : unionElementsState.get()) {
				Preconditions.checkState(expected.remove(unionedEvent.getSequenceNumber()), "Unexpected events in restored union state.");
			}
			Preconditions.checkState(expected.size() == 0, "Missing events in restored union state.");
		} else {
			// verify that the broadcast / union state is actually empty if this is not a restored run, as told by the state context;
			// this catches incorrect logic with the context.isRestored() when using broadcast state / union state.

			Preconditions.checkState(!numBroadcastElementsState.iterator().hasNext());
			Preconditions.checkState(!broadcastElementsState.iterator().hasNext());
			Preconditions.checkState(!unionElementsState.get().iterator().hasNext());
		}
	}

	@Override
	public void snapshotState(FunctionSnapshotContext context) throws Exception {}
}
