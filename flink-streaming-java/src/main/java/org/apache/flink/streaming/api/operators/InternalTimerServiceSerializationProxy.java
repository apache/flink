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
import org.apache.flink.core.io.PostVersionedIOReadableWritable;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.state.KeyGroupsList;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;

import java.io.IOException;
import java.util.Map;

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
	private int keyGroupIdx;
	private int totalKeyGroups;
	private KeyGroupsList localKeyGroupRange;
	private KeyContext keyContext;
	private ProcessingTimeService processingTimeService;

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
			int keyGroupIdx) {

		this.timerServices = checkNotNull(timerServices);
		this.keyGroupIdx = keyGroupIdx;
	}

	@Override
	public int getVersion() {
		return VERSION;
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		super.write(out);

		out.writeInt(timerServices.size());
		for (Map.Entry<String, HeapInternalTimerService<K, N>> entry : timerServices.entrySet()) {
			String serviceName = entry.getKey();
			HeapInternalTimerService<K, N> timerService = entry.getValue();

			out.writeUTF(serviceName);
			InternalTimersSnapshotReaderWriters
				.getWriterForVersion(VERSION, timerService.snapshotTimersForKeyGroup(keyGroupIdx))
				.writeTimersSnapshot(out);
		}
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
