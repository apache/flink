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

import org.apache.flink.api.common.typeutils.BackwardsCompatibleSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSerializationUtil;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshotSerializationUtil;
import org.apache.flink.api.java.tuple.Tuple2;
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
 * Serialization proxy for all meta data in keyed state backends. In the future we might also
 * requiresMigration the actual state serialization logic here.
 */
public class KeyedBackendSerializationProxy<K> extends VersionedIOReadableWritable {

    public static final int VERSION = 6;

    private static final Map<Integer, Integer> META_INFO_SNAPSHOT_FORMAT_VERSION_MAPPER =
            new HashMap<>();

    static {
        META_INFO_SNAPSHOT_FORMAT_VERSION_MAPPER.put(1, 1);
        META_INFO_SNAPSHOT_FORMAT_VERSION_MAPPER.put(2, 2);
        META_INFO_SNAPSHOT_FORMAT_VERSION_MAPPER.put(3, 3);
        META_INFO_SNAPSHOT_FORMAT_VERSION_MAPPER.put(4, 4);
        META_INFO_SNAPSHOT_FORMAT_VERSION_MAPPER.put(5, 5);
        META_INFO_SNAPSHOT_FORMAT_VERSION_MAPPER.put(6, CURRENT_STATE_META_INFO_SNAPSHOT_VERSION);
    }

    // TODO allow for more (user defined) compression formats + backwards compatibility story.
    /** This specifies if we use a compressed format write the key-groups */
    private boolean usingKeyGroupCompression;

    // TODO the keySerializer field should be removed, once all serializers have the
    // restoreSerializer() method implemented
    private TypeSerializer<K> keySerializer;
    private TypeSerializerSnapshot<K> keySerializerSnapshot;

    private List<StateMetaInfoSnapshot> stateMetaInfoSnapshots;

    private ClassLoader userCodeClassLoader;

    public KeyedBackendSerializationProxy(ClassLoader userCodeClassLoader) {
        this.userCodeClassLoader = Preconditions.checkNotNull(userCodeClassLoader);
    }

    public KeyedBackendSerializationProxy(
            TypeSerializer<K> keySerializer,
            List<StateMetaInfoSnapshot> stateMetaInfoSnapshots,
            boolean compression) {

        this.usingKeyGroupCompression = compression;

        this.keySerializer = Preconditions.checkNotNull(keySerializer);
        this.keySerializerSnapshot =
                Preconditions.checkNotNull(keySerializer.snapshotConfiguration());

        Preconditions.checkNotNull(stateMetaInfoSnapshots);
        Preconditions.checkArgument(stateMetaInfoSnapshots.size() <= Short.MAX_VALUE);
        this.stateMetaInfoSnapshots = stateMetaInfoSnapshots;
    }

    public List<StateMetaInfoSnapshot> getStateMetaInfoSnapshots() {
        return stateMetaInfoSnapshots;
    }

    public TypeSerializerSnapshot<K> getKeySerializerSnapshot() {
        return keySerializerSnapshot;
    }

    public boolean isUsingKeyGroupCompression() {
        return usingKeyGroupCompression;
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

        // write the compression format used to write each key-group
        out.writeBoolean(usingKeyGroupCompression);

        TypeSerializerSnapshotSerializationUtil.writeSerializerSnapshot(
                out, keySerializerSnapshot, keySerializer);

        // write individual registered keyed state metainfos
        out.writeShort(stateMetaInfoSnapshots.size());
        for (StateMetaInfoSnapshot metaInfoSnapshot : stateMetaInfoSnapshots) {
            StateMetaInfoSnapshotReadersWriters.getWriter()
                    .writeStateMetaInfoSnapshot(metaInfoSnapshot, out);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void read(DataInputView in) throws IOException {
        super.read(in);

        final int readVersion = getReadVersion();

        if (readVersion >= 4) {
            usingKeyGroupCompression = in.readBoolean();
        } else {
            usingKeyGroupCompression = false;
        }

        // only starting from version 3, we have the key serializer and its config snapshot written
        if (readVersion >= 6) {
            this.keySerializerSnapshot =
                    TypeSerializerSnapshotSerializationUtil.readSerializerSnapshot(
                            in, userCodeClassLoader, null);
        } else if (readVersion >= 3) {
            Tuple2<TypeSerializer<?>, TypeSerializerSnapshot<?>> keySerializerAndConfig =
                    TypeSerializerSerializationUtil.readSerializersAndConfigsWithResilience(
                                    in, userCodeClassLoader)
                            .get(0);
            this.keySerializerSnapshot = (TypeSerializerSnapshot<K>) keySerializerAndConfig.f1;
        } else {
            this.keySerializerSnapshot =
                    new BackwardsCompatibleSerializerSnapshot<>(
                            TypeSerializerSerializationUtil.tryReadSerializer(
                                    in, userCodeClassLoader, true));
        }
        this.keySerializer = null;

        Integer metaInfoSnapshotVersion = META_INFO_SNAPSHOT_FORMAT_VERSION_MAPPER.get(readVersion);
        if (metaInfoSnapshotVersion == null) {
            // this should not happen; guard for the future
            throw new IOException(
                    "Cannot determine corresponding meta info snapshot version for keyed backend serialization readVersion="
                            + readVersion);
        }
        final StateMetaInfoReader stateMetaInfoReader =
                StateMetaInfoSnapshotReadersWriters.getReader(
                        metaInfoSnapshotVersion,
                        StateMetaInfoSnapshotReadersWriters.StateTypeHint.KEYED_STATE);

        int numKvStates = in.readShort();
        stateMetaInfoSnapshots = new ArrayList<>(numKvStates);
        for (int i = 0; i < numKvStates; i++) {
            StateMetaInfoSnapshot snapshot =
                    stateMetaInfoReader.readStateMetaInfoSnapshot(in, userCodeClassLoader);

            stateMetaInfoSnapshots.add(snapshot);
        }
    }
}
