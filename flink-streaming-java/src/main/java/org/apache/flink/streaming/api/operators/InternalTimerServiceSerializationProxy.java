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
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.io.PostVersionedIOReadableWritable;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Serialization proxy for the timer services for a given key-group. */
@Internal
public class InternalTimerServiceSerializationProxy<K> extends PostVersionedIOReadableWritable {

    public static final int VERSION = 2;

    /** The key-group timer services to write / read. */
    private final InternalTimeServiceManagerImpl<K> timerServicesManager;

    /** The user classloader; only relevant if the proxy is used to restore timer services. */
    private ClassLoader userCodeClassLoader;

    /** Properties of restored timer services. */
    private final int keyGroupIdx;

    /** Constructor to use when restoring timer services. */
    public InternalTimerServiceSerializationProxy(
            InternalTimeServiceManagerImpl<K> timerServicesManager,
            ClassLoader userCodeClassLoader,
            int keyGroupIdx) {
        this.timerServicesManager = checkNotNull(timerServicesManager);
        this.userCodeClassLoader = checkNotNull(userCodeClassLoader);
        this.keyGroupIdx = keyGroupIdx;
    }

    /** Constructor to use when writing timer services. */
    public InternalTimerServiceSerializationProxy(
            InternalTimeServiceManagerImpl<K> timerServicesManager, int keyGroupIdx) {
        this.timerServicesManager = checkNotNull(timerServicesManager);
        this.keyGroupIdx = keyGroupIdx;
    }

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public int[] getCompatibleVersions() {
        return new int[] {VERSION, 1};
    }

    @Override
    @SuppressWarnings("unchecked")
    public void write(DataOutputView out) throws IOException {
        super.write(out);
        final Map<String, InternalTimerServiceImpl<K, ?>> registeredTimerServices =
                timerServicesManager.getRegisteredTimerServices();

        out.writeInt(registeredTimerServices.size());
        for (Map.Entry<String, InternalTimerServiceImpl<K, ?>> entry :
                registeredTimerServices.entrySet()) {
            String serviceName = entry.getKey();
            InternalTimerServiceImpl<K, ?> timerService = entry.getValue();

            out.writeUTF(serviceName);
            InternalTimersSnapshotReaderWriters.getWriterForVersion(
                            VERSION,
                            timerService.snapshotTimersForKeyGroup(keyGroupIdx),
                            timerService.getKeySerializer(),
                            (TypeSerializer) timerService.getNamespaceSerializer())
                    .writeTimersSnapshot(out);
        }
    }

    @Override
    protected void read(DataInputView in, boolean wasVersioned) throws IOException {
        int noOfTimerServices = in.readInt();

        for (int i = 0; i < noOfTimerServices; i++) {
            String serviceName = in.readUTF();

            int readerVersion =
                    wasVersioned
                            ? getReadVersion()
                            : InternalTimersSnapshotReaderWriters.NO_VERSION;
            InternalTimersSnapshot<?, ?> restoredTimersSnapshot =
                    InternalTimersSnapshotReaderWriters.getReaderForVersion(
                                    readerVersion, userCodeClassLoader)
                            .readTimersSnapshot(in);

            InternalTimerServiceImpl<K, ?> timerService =
                    registerOrGetTimerService(serviceName, restoredTimersSnapshot);

            timerService.restoreTimersForKeyGroup(restoredTimersSnapshot, keyGroupIdx);
        }
    }

    @SuppressWarnings("unchecked")
    private <N> InternalTimerServiceImpl<K, N> registerOrGetTimerService(
            String serviceName, InternalTimersSnapshot<?, ?> restoredTimersSnapshot) {
        final TypeSerializer<K> keySerializer =
                (TypeSerializer<K>)
                        restoredTimersSnapshot.getKeySerializerSnapshot().restoreSerializer();
        final TypeSerializer<N> namespaceSerializer =
                (TypeSerializer<N>)
                        restoredTimersSnapshot.getNamespaceSerializerSnapshot().restoreSerializer();
        TimerSerializer<K, N> timerSerializer =
                new TimerSerializer<>(keySerializer, namespaceSerializer);
        return timerServicesManager.registerOrGetTimerService(serviceName, timerSerializer);
    }
}
