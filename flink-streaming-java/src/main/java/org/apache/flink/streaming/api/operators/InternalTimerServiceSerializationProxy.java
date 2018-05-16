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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.core.io.PostVersionedIOReadableWritable;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.KeyGroupsList;
import org.apache.flink.runtime.state.KeyedStateCheckpointOutputStream;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Serialization proxy for the timer services for a given key-group.
 */
@Internal
public class InternalTimerServiceSerializationProxy<K, N> extends PostVersionedIOReadableWritable {

	public static final int VERSION = 1;

	/** The key-group timer services to write / read. */
	private Map<String, HeapInternalTimerService<K, N>> timerServices;

	/** The user classloader; only relevant if the proxy is used to restore timer services. */
	private ClassLoader userCodeClassLoader;

	/** Properties of restored timer services. */
	private KeyGroupsList allKeyGroups;
	private int keyGroupIdx;
	private int totalKeyGroups;
	private KeyGroupsList localKeyGroupRange;
	private KeyContext keyContext;
	private ProcessingTimeService processingTimeService;
	private Map<String, Tuple5<KeyGroupsList, Integer, Integer, InternalTimer[], InternalTimer[]>> epQueueMap;
	private KeyedStateCheckpointOutputStream out;
	/**
	 * Constructor to use when restoring timer services.
	 */
	public InternalTimerServiceSerializationProxy(
			Map<String, HeapInternalTimerService<K, N>> timerServicesMapToPopulate,
			ClassLoader userCodeClassLoader,
			int totalKeyGroups,
			KeyGroupsList localKeyGroupRange,
			KeyContext keyContext,
			ProcessingTimeService processingTimeService,
			int keyGroupIdx) {

		this.timerServices = checkNotNull(timerServicesMapToPopulate);
		this.userCodeClassLoader = checkNotNull(userCodeClassLoader);
		this.totalKeyGroups = totalKeyGroups;
		this.localKeyGroupRange = checkNotNull(localKeyGroupRange);
		this.keyContext = checkNotNull(keyContext);
		this.processingTimeService = checkNotNull(processingTimeService);
		this.keyGroupIdx = keyGroupIdx;
	}

	/**
	 * Constructor to use when writing timer services.
	 */
	public InternalTimerServiceSerializationProxy(
			Map<String, HeapInternalTimerService<K, N>> timerServices,
			KeyGroupsList allKeyGroups,
			Map<String, Tuple5<KeyGroupsList, Integer, Integer, InternalTimer[], InternalTimer[]>> epQueueMap,
			KeyedStateCheckpointOutputStream out) {
		this.timerServices = checkNotNull(timerServices);
		this.allKeyGroups = allKeyGroups;
		this.epQueueMap = epQueueMap;
		this.out = out;

	}

	@Override
	public int getVersion() {
		return VERSION;
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		Map<String, Tuple2<Set<InternalTimer<K, N>>[], Set<InternalTimer<K, N>>[]>> epTimersKeyGroups = new HashMap<>(epQueueMap.size() * 2);
		for (Map.Entry<String, Tuple5<KeyGroupsList, Integer, Integer, InternalTimer[], InternalTimer[]>> entry : epQueueMap.entrySet()) {
			String serviceName = entry.getKey();
			Tuple5<KeyGroupsList, Integer, Integer, InternalTimer[], InternalTimer[]> epQueue = entry.getValue();
			Set<InternalTimer<K, N>>[] eventTimeTimersByKeyGroup = buildTimersKeyGroup(epQueue.f0, epQueue.f1, epQueue.f2, epQueue.f3);
			Set<InternalTimer<K, N>>[] processingTimeTimersByKeyGroup = buildTimersKeyGroup(epQueue.f0, epQueue.f1, epQueue.f2, epQueue.f4);
			Tuple2<Set<InternalTimer<K, N>>[], Set<InternalTimer<K, N>>[]> epTimersKeyGroup = new Tuple2<>(eventTimeTimersByKeyGroup, processingTimeTimersByKeyGroup);
			epTimersKeyGroups.put(serviceName, epTimersKeyGroup);
		}
		for (int keyGroupIdx : allKeyGroups) {
			this.out.startNewKeyGroup(keyGroupIdx);
			super.write(out);

			out.writeInt(timerServices.size());
			for (Map.Entry<String, HeapInternalTimerService<K, N>> entry : timerServices.entrySet()) {
				String serviceName = entry.getKey();
				HeapInternalTimerService<K, N> timerService = entry.getValue();
				Tuple2<Set<InternalTimer<K, N>>[], Set<InternalTimer<K, N>>[]> epTimersKeyGroup = epTimersKeyGroups.get(serviceName);
				Tuple5<KeyGroupsList, Integer, Integer, InternalTimer[], InternalTimer[]> epQueue = epQueueMap.get(serviceName);
				Integer localKeyGroupRangeStartIdx = epQueue.f2;
				int localIdx = keyGroupIdx - localKeyGroupRangeStartIdx;
				Set<InternalTimer<K, N>> eventTimeTimers = epTimersKeyGroup.f0[localIdx];
				Set<InternalTimer<K, N>> processingTimeTimers = epTimersKeyGroup.f1[localIdx];
				out.writeUTF(serviceName);
				InternalTimersSnapshotReaderWriters
					.getWriterForVersion(VERSION, timerService.snapshotTimersForKeyGroup(eventTimeTimers, processingTimeTimers))
					.writeTimersSnapshot(out);
			}
		}
	}

	private Set<InternalTimer<K, N>>[] buildTimersKeyGroup(KeyGroupsList localKeyGroupRange, Integer totalKeyGroups, Integer localKeyGroupRangeStartIdx, InternalTimer[] timers) {
		int localKeyGroups = localKeyGroupRange.getNumberOfKeyGroups();
		Set<InternalTimer<K, N>>[] ret = new HashSet[localKeyGroups];
		for (int i = 0; i < ret.length; i++) {
			ret[i] = new HashSet<>();
		}
		for (InternalTimer timer : timers) {
			int keyGroupIdx = KeyGroupRangeAssignment.assignToKeyGroup(timer.getKey(), totalKeyGroups);
			int localIdx = keyGroupIdx - localKeyGroupRangeStartIdx;
			Set<InternalTimer<K, N>> timerSet = ret[localIdx];
			timerSet.add(timer);
		}
		return ret;
	}

	@Override
	protected void read(DataInputView in, boolean wasVersioned) throws IOException {
		int noOfTimerServices = in.readInt();

		for (int i = 0; i < noOfTimerServices; i++) {
			String serviceName = in.readUTF();

			HeapInternalTimerService<K, N> timerService = timerServices.get(serviceName);
			if (timerService == null) {
				timerService = new HeapInternalTimerService<>(
					totalKeyGroups,
					localKeyGroupRange,
					keyContext,
					processingTimeService);
				timerServices.put(serviceName, timerService);
			}

			int readerVersion = wasVersioned ? getReadVersion() : InternalTimersSnapshotReaderWriters.NO_VERSION;
			InternalTimersSnapshot<?, ?> restoredTimersSnapshot = InternalTimersSnapshotReaderWriters
				.getReaderForVersion(readerVersion, userCodeClassLoader)
				.readTimersSnapshot(in);

			timerService.restoreTimersForKeyGroup(restoredTimersSnapshot, keyGroupIdx);
		}
	}
}
