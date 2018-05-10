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

import java.io.IOException;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Serialization proxy for the timer services for a given key-group.
 */
@Internal
public class InternalTimerServiceSerializationProxy<K, N> extends PostVersionedIOReadableWritable {

	public static final int VERSION = 1;

	/** The timer service manager to write / read. */
	private InternalTimeServiceManager<K, N> timeServiceManager;

	/** The user classloader; only relevant if the proxy is used to restore timer services. */
	private ClassLoader userCodeClassLoader;

	/** The index of the key group to write / read. */
	private int keyGroupIdx;

	/**
	 * Constructor to use when restoring timer services.
	 */
	public InternalTimerServiceSerializationProxy(
			InternalTimeServiceManager<K, N> timeServiceManager,
			int keyGroupIdx,
			ClassLoader userCodeClassLoader) {

		this.timeServiceManager = checkNotNull(timeServiceManager);
		this.keyGroupIdx = keyGroupIdx;
		this.userCodeClassLoader = checkNotNull(userCodeClassLoader);
	}

	/**
	 * Constructor to use when writing timer services.
	 */
	public InternalTimerServiceSerializationProxy(
			InternalTimeServiceManager<K, N> timeServiceManager,
			int keyGroupIdx) {

		this.timeServiceManager = checkNotNull(timeServiceManager);
		this.keyGroupIdx = keyGroupIdx;
	}

	@Override
	public int getVersion() {
		return VERSION;
	}

	@Override
	public void write(DataOutputView out) throws IOException {

		Map<String, InternalTimerService<K, N>> timerServices = timeServiceManager.getTimerServices();

		super.write(out);

		out.writeInt(timerServices.size());
		for (Map.Entry<String, InternalTimerService<K, N>> entry : timerServices.entrySet()) {
			String serviceName = entry.getKey();
			InternalTimerService<K, N> timerService = entry.getValue();

			out.writeUTF(serviceName);
			InternalTimersSnapshotReaderWriters
				.getWriterForVersion(VERSION, timerService.snapshotTimersForKeyGroup(keyGroupIdx))
				.writeTimersSnapshot(out);
		}
	}

	@Override
	protected void read(DataInputView in, boolean wasVersioned) throws IOException {

		Map<String, InternalTimerService<K, N>> timerServices = timeServiceManager.getTimerServices();

		int noOfTimerServices = in.readInt();

		for (int i = 0; i < noOfTimerServices; i++) {
			String serviceName = in.readUTF();

			int readerVersion = wasVersioned ? getReadVersion() : InternalTimersSnapshotReaderWriters.NO_VERSION;
			InternalTimersSnapshot<K, N> restoredTimersSnapshot = InternalTimersSnapshotReaderWriters
				.<K, N>getReaderForVersion(readerVersion, userCodeClassLoader)
				.readTimersSnapshot(in);

			InternalTimerService<K, N> timerService =
				timerServices.computeIfAbsent(
					serviceName,
					k -> timeServiceManager.createInternalTimerService(
						serviceName, restoredTimersSnapshot.getKeySerializer(), restoredTimersSnapshot.getNamespaceSerializer()));

			timerService.restoreTimersForKeyGroup(restoredTimersSnapshot, keyGroupIdx);
		}
	}
}
