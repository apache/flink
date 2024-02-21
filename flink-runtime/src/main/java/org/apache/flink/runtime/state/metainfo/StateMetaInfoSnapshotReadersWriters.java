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

package org.apache.flink.runtime.state.metainfo;

import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshotSerializationUtil;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.CollectionUtil;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Static factory that gives out the write and readers for different versions of {@link
 * StateMetaInfoSnapshot}.
 */
public class StateMetaInfoSnapshotReadersWriters {

    private StateMetaInfoSnapshotReadersWriters() {}

    /**
     * Current version for the serialization format of {@link StateMetaInfoSnapshotReadersWriters}.
     * - v6: since Flink 1.7.x
     */
    public static final int CURRENT_STATE_META_INFO_SNAPSHOT_VERSION = 7;

    /** Returns the writer for {@link StateMetaInfoSnapshot}. */
    @Nonnull
    public static StateMetaInfoWriter getWriter() {
        return CurrentWriterImpl.INSTANCE;
    }

    /**
     * Returns a reader for {@link StateMetaInfoSnapshot} with the requested state type and version
     * number.
     *
     * @param readVersion the format version to read.
     * @return the requested reader.
     */
    @Nonnull
    public static StateMetaInfoReader getReader(int readVersion) {

        checkArgument(
                readVersion <= CURRENT_STATE_META_INFO_SNAPSHOT_VERSION,
                "Unsupported read version for state meta info [%s]",
                readVersion);
        if (readVersion < 6) {
            // versions before 5 still had different state meta info formats between keyed /
            // operator state
            throw new UnsupportedOperationException(
                    String.format(
                            "No longer supported version [%d]. Please upgrade first to Flink 1.16. ",
                            readVersion));
        }
        return CurrentReaderImpl.INSTANCE;
    }

    // ---------------------------------------------------------------------------------
    //  Current version reader / writer implementation
    // ---------------------------------------------------------------------------------

    /**
     * Implementation of {@link StateMetaInfoWriter} for current implementation. The serialization
     * format is as follows:
     *
     * <ul>
     *   <li>1. State name (UDF)
     *   <li>2. State backend type enum ordinal (int)
     *   <li>3. Meta info options map, consisting of the map size (int) followed by the key value
     *       pairs (String, String)
     *   <li>4. Serializer configuration map, consisting of the map size (int) followed by the key
     *       value pairs (String, TypeSerializerSnapshot)
     * </ul>
     */
    static class CurrentWriterImpl implements StateMetaInfoWriter {

        private static final CurrentWriterImpl INSTANCE = new CurrentWriterImpl();

        @Override
        public void writeStateMetaInfoSnapshot(
                @Nonnull StateMetaInfoSnapshot snapshot, @Nonnull DataOutputView outputView)
                throws IOException {
            final Map<String, String> optionsMap = snapshot.getOptionsImmutable();
            final Map<String, TypeSerializerSnapshot<?>> serializerConfigSnapshotsMap =
                    snapshot.getSerializerSnapshotsImmutable();

            outputView.writeUTF(snapshot.getName());
            outputView.writeInt(snapshot.getBackendStateType().ordinal());
            outputView.writeInt(optionsMap.size());
            for (Map.Entry<String, String> entry : optionsMap.entrySet()) {
                outputView.writeUTF(entry.getKey());
                outputView.writeUTF(entry.getValue());
            }

            outputView.writeInt(serializerConfigSnapshotsMap.size());
            for (Map.Entry<String, TypeSerializerSnapshot<?>> entry :
                    serializerConfigSnapshotsMap.entrySet()) {
                final String key = entry.getKey();
                outputView.writeUTF(entry.getKey());

                TypeSerializerSnapshotSerializationUtil.writeSerializerSnapshot(
                        outputView, (TypeSerializerSnapshot) entry.getValue());
            }
        }
    }

    /**
     * Implementation of {@link StateMetaInfoReader} for the current version and generic for all
     * state types.
     */
    static class CurrentReaderImpl implements StateMetaInfoReader {

        private static final CurrentReaderImpl INSTANCE = new CurrentReaderImpl();

        @Nonnull
        @Override
        public StateMetaInfoSnapshot readStateMetaInfoSnapshot(
                @Nonnull DataInputView inputView, @Nonnull ClassLoader userCodeClassLoader)
                throws IOException {

            final String stateName = inputView.readUTF();
            final StateMetaInfoSnapshot.BackendStateType stateType =
                    StateMetaInfoSnapshot.BackendStateType.values()[inputView.readInt()];
            final int numOptions = inputView.readInt();
            HashMap<String, String> optionsMap =
                    CollectionUtil.newHashMapWithExpectedSize(numOptions);
            for (int i = 0; i < numOptions; ++i) {
                String key = inputView.readUTF();
                String value = inputView.readUTF();
                optionsMap.put(key, value);
            }

            final int numSerializerConfigSnapshots = inputView.readInt();
            final HashMap<String, TypeSerializerSnapshot<?>> serializerConfigsMap =
                    CollectionUtil.newHashMapWithExpectedSize(numSerializerConfigSnapshots);

            for (int i = 0; i < numSerializerConfigSnapshots; ++i) {
                serializerConfigsMap.put(
                        inputView.readUTF(),
                        TypeSerializerSnapshotSerializationUtil.readSerializerSnapshot(
                                inputView, userCodeClassLoader));
            }

            return new StateMetaInfoSnapshot(
                    stateName, stateType, optionsMap, serializerConfigsMap);
        }
    }
}
