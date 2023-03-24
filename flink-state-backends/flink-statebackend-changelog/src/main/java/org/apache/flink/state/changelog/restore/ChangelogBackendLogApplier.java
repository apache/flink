/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.state.changelog.restore;

import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.api.java.typeutils.runtime.DataInputViewStream;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.RegisteredPriorityQueueStateBackendMetaInfo;
import org.apache.flink.runtime.state.RegisteredStateMetaInfoBase;
import org.apache.flink.runtime.state.changelog.StateChange;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoReader;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot.BackendStateType;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshotReadersWriters;
import org.apache.flink.state.changelog.ChangelogKeyedStateBackend;
import org.apache.flink.state.changelog.ChangelogState;
import org.apache.flink.state.changelog.StateChangeOperation;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Map;

import static org.apache.flink.state.changelog.StateChangeOperation.METADATA;
import static org.apache.flink.state.changelog.restore.FunctionDelegationHelper.delegateAggregateFunction;
import static org.apache.flink.state.changelog.restore.FunctionDelegationHelper.delegateReduceFunction;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Applies {@link StateChange}s to a {@link ChangelogKeyedStateBackend}. */
@SuppressWarnings({"rawtypes", "unchecked"})
class ChangelogBackendLogApplier {
    private static final Logger LOG = LoggerFactory.getLogger(ChangelogBackendLogApplier.class);

    public static void apply(
            StateChange stateChange,
            ChangelogRestoreTarget<?> changelogRestoreTarget,
            ClassLoader classLoader,
            Map<Short, StateID> stateIds)
            throws Exception {
        DataInputViewStreamWrapper in =
                new DataInputViewStreamWrapper(new ByteArrayInputStream(stateChange.getChange()));
        applyOperation(
                StateChangeOperation.byCode(in.readByte()),
                stateChange.getKeyGroup(),
                changelogRestoreTarget,
                in,
                classLoader,
                ChangelogApplierFactoryImpl.INSTANCE,
                stateIds);
    }

    private static void applyOperation(
            StateChangeOperation operation,
            int keyGroup,
            ChangelogRestoreTarget<?> changelogRestoreTarget,
            DataInputView in,
            ClassLoader classLoader,
            ChangelogApplierFactory factory,
            Map<Short, StateID> stateIds)
            throws Exception {
        LOG.debug("apply {} in key group {}", operation, keyGroup);
        if (operation == METADATA) {
            applyMetaDataChange(in, changelogRestoreTarget, classLoader, stateIds);
        } else if (changelogRestoreTarget.getKeyGroupRange().contains(keyGroup)) {
            applyDataChange(in, factory, changelogRestoreTarget, operation, stateIds);
        }
    }

    private static void applyMetaDataChange(
            DataInputView in,
            ChangelogRestoreTarget<?> changelogRestoreTarget,
            ClassLoader classLoader,
            Map<Short, StateID> stateIds)
            throws Exception {

        StateMetaInfoSnapshot snapshot = readStateMetaInfoSnapshot(in, classLoader);
        RegisteredStateMetaInfoBase meta;
        switch (snapshot.getBackendStateType()) {
            case KEY_VALUE:
                meta = restoreKvMetaData(changelogRestoreTarget, snapshot, in);
                break;
            case PRIORITY_QUEUE:
                meta = restorePqMetaData(changelogRestoreTarget, snapshot);
                break;
            default:
                throw new RuntimeException(
                        "Unsupported state type: "
                                + snapshot.getBackendStateType()
                                + ", sate: "
                                + snapshot.getName());
        }
        stateIds.put(
                in.readShort(),
                new StateID(meta.getName(), BackendStateType.byCode(in.readByte())));
    }

    private static StateTtlConfig readTtlConfig(DataInputView in) throws IOException {
        if (in.readBoolean()) {
            try {
                try (ObjectInputStream objectInputStream =
                        new ObjectInputStream(new DataInputViewStream(in))) {
                    return (StateTtlConfig) objectInputStream.readObject();
                }
            } catch (ClassNotFoundException e) {
                throw new IOException(e);
            }
        } else {
            return StateTtlConfig.DISABLED;
        }
    }

    @Nullable
    private static Object readDefaultValue(
            DataInputView in, RegisteredKeyValueStateBackendMetaInfo meta) throws IOException {
        return in.readBoolean() ? meta.getStateSerializer().deserialize(in) : null;
    }

    private static RegisteredKeyValueStateBackendMetaInfo restoreKvMetaData(
            ChangelogRestoreTarget<?> changelogRestoreTarget,
            StateMetaInfoSnapshot snapshot,
            DataInputView in)
            throws Exception {
        RegisteredKeyValueStateBackendMetaInfo meta =
                new RegisteredKeyValueStateBackendMetaInfo(snapshot);
        StateTtlConfig ttlConfig = readTtlConfig(in);
        Object defaultValue = readDefaultValue(in, meta);
        // Use regular API to create states in both changelog and the base backends the metadata is
        // persisted in log before data changes.
        // An alternative solution to load metadata "natively" by the base backends would require
        // base state to be always present, i.e. the 1st checkpoint would have to be "full" always.
        StateDescriptor stateDescriptor = toStateDescriptor(meta, defaultValue);
        if (ttlConfig.isEnabled()) {
            stateDescriptor.enableTimeToLive(ttlConfig);
        }
        changelogRestoreTarget.createKeyedState(meta.getNamespaceSerializer(), stateDescriptor);
        return meta;
    }

    private static StateDescriptor toStateDescriptor(
            RegisteredKeyValueStateBackendMetaInfo meta, @Nullable Object defaultValue) {
        switch (meta.getStateType()) {
            case VALUE:
                return new ValueStateDescriptor(
                        meta.getName(), meta.getStateSerializer(), defaultValue);
            case MAP:
                MapSerializer mapSerializer = (MapSerializer) meta.getStateSerializer();
                return new MapStateDescriptor(
                        meta.getName(),
                        mapSerializer.getKeySerializer(),
                        mapSerializer.getValueSerializer());
            case LIST:
                return new ListStateDescriptor(
                        meta.getName(),
                        ((ListSerializer) meta.getStateSerializer()).getElementSerializer());
            case AGGREGATING:
                return new AggregatingStateDescriptor(
                        meta.getName(), delegateAggregateFunction(), meta.getStateSerializer());
            case REDUCING:
                return new ReducingStateDescriptor(
                        meta.getName(), delegateReduceFunction(), meta.getStateSerializer());
            default:
                throw new IllegalArgumentException(meta.getStateType().toString());
        }
    }

    private static RegisteredPriorityQueueStateBackendMetaInfo restorePqMetaData(
            ChangelogRestoreTarget<?> changelogRestoreTarget, StateMetaInfoSnapshot snapshot) {
        RegisteredPriorityQueueStateBackendMetaInfo meta =
                new RegisteredPriorityQueueStateBackendMetaInfo(snapshot);
        changelogRestoreTarget.createPqState(meta.getName(), meta.getElementSerializer());
        return meta;
    }

    private static StateMetaInfoSnapshot readStateMetaInfoSnapshot(
            DataInputView in, ClassLoader classLoader) throws IOException {
        int version = in.readInt();
        StateMetaInfoReader reader = StateMetaInfoSnapshotReadersWriters.getReader(version);
        return reader.readStateMetaInfoSnapshot(in, classLoader);
    }

    private static void applyDataChange(
            DataInputView in,
            ChangelogApplierFactory factory,
            ChangelogRestoreTarget<?> changelogRestoreTarget,
            StateChangeOperation operation,
            Map<Short, StateID> stateIds)
            throws Exception {
        StateID id = checkNotNull(stateIds.get(in.readShort()));
        ChangelogState state = changelogRestoreTarget.getExistingState(id.stateName, id.stateType);
        Preconditions.checkState(
                state != null, String.format("%s state %s not found", id.stateType, id.stateName));
        StateChangeApplier changeApplier = state.getChangeApplier(factory);
        changeApplier.apply(operation, in);
    }

    private ChangelogBackendLogApplier() {}
}
