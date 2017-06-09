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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Current default implementation of {@link OperatorStateRepartitioner} that redistributes state in round robin fashion.
 */
public class RoundRobinOperatorStateRepartitioner implements OperatorStateRepartitioner {

	public static final OperatorStateRepartitioner INSTANCE = new RoundRobinOperatorStateRepartitioner();
	private static final boolean OPTIMIZE_MEMORY_USE = false;

	@Override
	public List<Collection<OperatorStateHandle>> repartitionState(
			List<OperatorStateHandle> previousParallelSubtaskStates,
			int parallelism) {

		Preconditions.checkNotNull(previousParallelSubtaskStates);
		Preconditions.checkArgument(parallelism > 0);

		// Reorganize: group by (State Name -> StreamStateHandle + Offsets)
		GroupByStateNameResults nameToStateByMode = groupByStateName(previousParallelSubtaskStates);

		if (OPTIMIZE_MEMORY_USE) {
			previousParallelSubtaskStates.clear(); // free for GC at to cost that old handles are no longer available
		}

		// Assemble result from all merge maps
		List<Collection<OperatorStateHandle>> result = new ArrayList<>(parallelism);

		// Do the actual repartitioning for all named states
		List<Map<StreamStateHandle, OperatorStateHandle>> mergeMapList =
				repartition(nameToStateByMode, parallelism);

		for (int i = 0; i < mergeMapList.size(); ++i) {
			result.add(i, new ArrayList<>(mergeMapList.get(i).values()));
		}

		return result;
	}

	/**
	 * Group by the different named states.
	 */
	@SuppressWarnings("unchecked, rawtype")
	private GroupByStateNameResults groupByStateName(
			List<OperatorStateHandle> previousParallelSubtaskStates) {

		//Reorganize: group by (State Name -> StreamStateHandle + StateMetaInfo)
		EnumMap<OperatorStateHandle.Mode,
				Map<String, List<Tuple2<StreamStateHandle, OperatorStateHandle.StateMetaInfo>>>> nameToStateByMode =
				new EnumMap<>(OperatorStateHandle.Mode.class);

		for (OperatorStateHandle.Mode mode : OperatorStateHandle.Mode.values()) {
			Map<String, List<Tuple2<StreamStateHandle, OperatorStateHandle.StateMetaInfo>>> map = new HashMap<>();
			nameToStateByMode.put(
					mode,
					new HashMap<String, List<Tuple2<StreamStateHandle, OperatorStateHandle.StateMetaInfo>>>());
		}

		for (OperatorStateHandle psh : previousParallelSubtaskStates) {

			for (Map.Entry<String, OperatorStateHandle.StateMetaInfo> e :
					psh.getStateNameToPartitionOffsets().entrySet()) {
				OperatorStateHandle.StateMetaInfo metaInfo = e.getValue();

				Map<String, List<Tuple2<StreamStateHandle, OperatorStateHandle.StateMetaInfo>>> nameToState =
						nameToStateByMode.get(metaInfo.getDistributionMode());

				List<Tuple2<StreamStateHandle, OperatorStateHandle.StateMetaInfo>> stateLocations =
						nameToState.get(e.getKey());

				if (stateLocations == null) {
					stateLocations = new ArrayList<>();
					nameToState.put(e.getKey(), stateLocations);
				}

				stateLocations.add(new Tuple2<>(psh.getDelegateStateHandle(), e.getValue()));
			}
		}

		return new GroupByStateNameResults(nameToStateByMode);
	}

	/**
	 * Repartition all named states.
	 */
	private List<Map<StreamStateHandle, OperatorStateHandle>> repartition(
			GroupByStateNameResults nameToStateByMode,
			int parallelism) {

		// We will use this to merge w.r.t. StreamStateHandles for each parallel subtask inside the maps
		List<Map<StreamStateHandle, OperatorStateHandle>> mergeMapList = new ArrayList<>(parallelism);

		// Initialize
		for (int i = 0; i < parallelism; ++i) {
			mergeMapList.add(new HashMap<StreamStateHandle, OperatorStateHandle>());
		}

		// Start with the state handles we distribute round robin by splitting by offsets
		Map<String, List<Tuple2<StreamStateHandle, OperatorStateHandle.StateMetaInfo>>> distributeNameToState =
				nameToStateByMode.getByMode(OperatorStateHandle.Mode.SPLIT_DISTRIBUTE);

		int startParallelOp = 0;
		// Iterate all named states and repartition one named state at a time per iteration
		for (Map.Entry<String, List<Tuple2<StreamStateHandle, OperatorStateHandle.StateMetaInfo>>> e :
				distributeNameToState.entrySet()) {

			List<Tuple2<StreamStateHandle, OperatorStateHandle.StateMetaInfo>> current = e.getValue();

			// Determine actual number of partitions for this named state
			int totalPartitions = 0;
			for (Tuple2<StreamStateHandle, OperatorStateHandle.StateMetaInfo> offsets : current) {
				totalPartitions += offsets.f1.getOffsets().length;
			}

			// Repartition the state across the parallel operator instances
			int lstIdx = 0;
			int offsetIdx = 0;
			int baseFraction = totalPartitions / parallelism;
			int remainder = totalPartitions % parallelism;

			int newStartParallelOp = startParallelOp;

			for (int i = 0; i < parallelism; ++i) {

				// Preparation: calculate the actual index considering wrap around
				int parallelOpIdx = (i + startParallelOp) % parallelism;

				// Now calculate the number of partitions we will assign to the parallel instance in this round ...
				int numberOfPartitionsToAssign = baseFraction;

				// ... and distribute odd partitions while we still have some, one at a time
				if (remainder > 0) {
					++numberOfPartitionsToAssign;
					--remainder;
				} else if (remainder == 0) {
					// We are out of odd partitions now and begin our next redistribution round with the current
					// parallel operator to ensure fair load balance
					newStartParallelOp = parallelOpIdx;
					--remainder;
				}

				// Now start collection the partitions for the parallel instance into this list
				List<Tuple2<StreamStateHandle, OperatorStateHandle.StateMetaInfo>> parallelOperatorState =
						new ArrayList<>();

				while (numberOfPartitionsToAssign > 0) {
					Tuple2<StreamStateHandle, OperatorStateHandle.StateMetaInfo> handleWithOffsets =
							current.get(lstIdx);

					long[] offsets = handleWithOffsets.f1.getOffsets();
					int remaining = offsets.length - offsetIdx;
					// Repartition offsets
					long[] offs;
					if (remaining > numberOfPartitionsToAssign) {
						offs = Arrays.copyOfRange(offsets, offsetIdx, offsetIdx + numberOfPartitionsToAssign);
						offsetIdx += numberOfPartitionsToAssign;
					} else {
						if (OPTIMIZE_MEMORY_USE) {
							handleWithOffsets.f1 = null; // GC
						}
						offs = Arrays.copyOfRange(offsets, offsetIdx, offsets.length);
						offsetIdx = 0;
						++lstIdx;
					}

					parallelOperatorState.add(new Tuple2<>(
							handleWithOffsets.f0,
							new OperatorStateHandle.StateMetaInfo(offs, OperatorStateHandle.Mode.SPLIT_DISTRIBUTE)));

					numberOfPartitionsToAssign -= remaining;

					// As a last step we merge partitions that use the same StreamStateHandle in a single
					// OperatorStateHandle
					Map<StreamStateHandle, OperatorStateHandle> mergeMap = mergeMapList.get(parallelOpIdx);
					OperatorStateHandle operatorStateHandle = mergeMap.get(handleWithOffsets.f0);
					if (operatorStateHandle == null) {
						operatorStateHandle = new OperatorStateHandle(
								new HashMap<String, OperatorStateHandle.StateMetaInfo>(),
								handleWithOffsets.f0);

						mergeMap.put(handleWithOffsets.f0, operatorStateHandle);
					}
					operatorStateHandle.getStateNameToPartitionOffsets().put(
							e.getKey(),
							new OperatorStateHandle.StateMetaInfo(offs, OperatorStateHandle.Mode.SPLIT_DISTRIBUTE));
				}
			}
			startParallelOp = newStartParallelOp;
			e.setValue(null);
		}

		// Now we also add the state handles marked for broadcast to all parallel instances
		Map<String, List<Tuple2<StreamStateHandle, OperatorStateHandle.StateMetaInfo>>> broadcastNameToState =
				nameToStateByMode.getByMode(OperatorStateHandle.Mode.BROADCAST);

		for (int i = 0; i < parallelism; ++i) {

			Map<StreamStateHandle, OperatorStateHandle> mergeMap = mergeMapList.get(i);

			for (Map.Entry<String, List<Tuple2<StreamStateHandle, OperatorStateHandle.StateMetaInfo>>> e :
					broadcastNameToState.entrySet()) {

				List<Tuple2<StreamStateHandle, OperatorStateHandle.StateMetaInfo>> current = e.getValue();

				for (Tuple2<StreamStateHandle, OperatorStateHandle.StateMetaInfo> handleWithMetaInfo : current) {
					OperatorStateHandle operatorStateHandle = mergeMap.get(handleWithMetaInfo.f0);
					if (operatorStateHandle == null) {
						operatorStateHandle = new OperatorStateHandle(
								new HashMap<String, OperatorStateHandle.StateMetaInfo>(),
								handleWithMetaInfo.f0);

						mergeMap.put(handleWithMetaInfo.f0, operatorStateHandle);
					}
					operatorStateHandle.getStateNameToPartitionOffsets().put(e.getKey(), handleWithMetaInfo.f1);
				}
			}
		}
		return mergeMapList;
	}

	private static final class GroupByStateNameResults {
		private final EnumMap<OperatorStateHandle.Mode,
				Map<String, List<Tuple2<StreamStateHandle, OperatorStateHandle.StateMetaInfo>>>> byMode;

		public GroupByStateNameResults(
				EnumMap<OperatorStateHandle.Mode,
						Map<String, List<Tuple2<StreamStateHandle, OperatorStateHandle.StateMetaInfo>>>> byMode) {
			this.byMode = Preconditions.checkNotNull(byMode);
		}

		public Map<String, List<Tuple2<StreamStateHandle, OperatorStateHandle.StateMetaInfo>>> getByMode(
				OperatorStateHandle.Mode mode) {
			return byMode.get(mode);
		}
	}
}
