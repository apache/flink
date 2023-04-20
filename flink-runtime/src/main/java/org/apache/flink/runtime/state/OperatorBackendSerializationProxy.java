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

package org.apache.flink.runtime.state;

import org.apache.flink.core.io.VersionedIOReadableWritable;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoReader;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshotReadersWriters;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshotReadersWriters.CURRENT_STATE_META_INFO_SNAPSHOT_VERSION;

/**
 * Serialization proxy for all meta data in operator state backends. In the future we might also
 * requiresMigration the actual state serialization logic here.
 */
public class OperatorBackendSerializationProxy extends VersionedIOReadableWritable {

    public static final int VERSION = 6;

    private static final Map<Integer, Integer> META_INFO_SNAPSHOT_FORMAT_VERSION_MAPPER =
            new HashMap<>();

    static {
        META_INFO_SNAPSHOT_FORMAT_VERSION_MAPPER.put(1, 1);
        META_INFO_SNAPSHOT_FORMAT_VERSION_MAPPER.put(2, 2);
        META_INFO_SNAPSHOT_FORMAT_VERSION_MAPPER.put(3, 3);
        META_INFO_SNAPSHOT_FORMAT_VERSION_MAPPER.put(4, 5);
        META_INFO_SNAPSHOT_FORMAT_VERSION_MAPPER.put(5, 6);
        META_INFO_SNAPSHOT_FORMAT_VERSION_MAPPER.put(6, CURRENT_STATE_META_INFO_SNAPSHOT_VERSION);
    }

    private List<StateMetaInfoSnapshot> operatorStateMetaInfoSnapshots;
    private List<StateMetaInfoSnapshot> broadcastStateMetaInfoSnapshots;
    private ClassLoader userCodeClassLoader;
    /** This specifies if we use a compressed format write the operator states */
    private boolean usingStateCompression;

    public OperatorBackendSerializationProxy(ClassLoader userCodeClassLoader) {
        this.userCodeClassLoader = Preconditions.checkNotNull(userCodeClassLoader);
    }

    public OperatorBackendSerializationProxy(
            List<StateMetaInfoSnapshot> operatorStateMetaInfoSnapshots,
            List<StateMetaInfoSnapshot> broadcastStateMetaInfoSnapshots,
            boolean compression) {

        this.operatorStateMetaInfoSnapshots =
                Preconditions.checkNotNull(operatorStateMetaInfoSnapshots);
        this.broadcastStateMetaInfoSnapshots =
                Preconditions.checkNotNull(broadcastStateMetaInfoSnapshots);
        Preconditions.checkArgument(
                operatorStateMetaInfoSnapshots.size() <= Short.MAX_VALUE
                        && broadcastStateMetaInfoSnapshots.size() <= Short.MAX_VALUE);
        this.usingStateCompression = compression;
    }

    boolean isUsingStateCompression() {
        return usingStateCompression;
    }

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public int[] getCompatibleVersions() {
        return new int[] {VERSION, 5, 4, 3, 2, 1};
    }

    @Override
    public void write(DataOutputView out) throws IOException {
        super.write(out);
        // write the compression format used to write each operator state
        out.writeBoolean(usingStateCompression);
        writeStateMetaInfoSnapshots(operatorStateMetaInfoSnapshots, out);
        writeStateMetaInfoSnapshots(broadcastStateMetaInfoSnapshots, out);
    }

    private void writeStateMetaInfoSnapshots(
            List<StateMetaInfoSnapshot> snapshots, DataOutputView out) throws IOException {
        out.writeShort(snapshots.size());
        for (StateMetaInfoSnapshot state : snapshots) {
            StateMetaInfoSnapshotReadersWriters.getWriter().writeStateMetaInfoSnapshot(state, out);
        }
    }

    @Override
    public void read(DataInputView in) throws IOException {
        super.read(in);

        final int proxyReadVersion = getReadVersion();
        final Integer metaInfoSnapshotVersion =
                META_INFO_SNAPSHOT_FORMAT_VERSION_MAPPER.get(proxyReadVersion);
        if (metaInfoSnapshotVersion == null) {
            // this should not happen; guard for the future
            throw new IOException(
                    "Cannot determine corresponding meta info snapshot version for operator backend serialization readVersion="
                            + proxyReadVersion);
        }
        if (metaInfoSnapshotVersion >= 7) {
            usingStateCompression = in.readBoolean();
        } else {
            usingStateCompression = false;
        }

        final StateMetaInfoReader stateMetaInfoReader =
                StateMetaInfoSnapshotReadersWriters.getReader(metaInfoSnapshotVersion);

        int numOperatorStates = in.readShort();
        operatorStateMetaInfoSnapshots = new ArrayList<>(numOperatorStates);
        for (int i = 0; i < numOperatorStates; i++) {
            operatorStateMetaInfoSnapshots.add(
                    stateMetaInfoReader.readStateMetaInfoSnapshot(in, userCodeClassLoader));
        }

        if (proxyReadVersion >= 3) {
            // broadcast states did not exist prior to version 3
            int numBroadcastStates = in.readShort();
            broadcastStateMetaInfoSnapshots = new ArrayList<>(numBroadcastStates);
            for (int i = 0; i < numBroadcastStates; i++) {
                broadcastStateMetaInfoSnapshots.add(
                        stateMetaInfoReader.readStateMetaInfoSnapshot(in, userCodeClassLoader));
            }
        } else {
            broadcastStateMetaInfoSnapshots = new ArrayList<>();
        }
    }

    public List<StateMetaInfoSnapshot> getOperatorStateMetaInfoSnapshots() {
        return operatorStateMetaInfoSnapshots;
    }

    public List<StateMetaInfoSnapshot> getBroadcastStateMetaInfoSnapshots() {
        return broadcastStateMetaInfoSnapshots;
    }
}
