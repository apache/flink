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

package org.apache.flink.state.changelog;

import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.state.RegisteredStateMetaInfoBase;
import org.apache.flink.runtime.state.changelog.StateChangelogWriter;
import org.apache.flink.runtime.state.heap.InternalKeyContext;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshotReadersWriters;
import org.apache.flink.util.function.ThrowingConsumer;

import javax.annotation.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshotReadersWriters.CURRENT_STATE_META_INFO_SNAPSHOT_VERSION;
import static org.apache.flink.state.changelog.AbstractStateChangeLogger.StateChangeOperation.ADD;
import static org.apache.flink.state.changelog.AbstractStateChangeLogger.StateChangeOperation.ADD_ELEMENT;
import static org.apache.flink.state.changelog.AbstractStateChangeLogger.StateChangeOperation.ADD_OR_UPDATE_ELEMENT;
import static org.apache.flink.state.changelog.AbstractStateChangeLogger.StateChangeOperation.CLEAR;
import static org.apache.flink.state.changelog.AbstractStateChangeLogger.StateChangeOperation.METADATA;
import static org.apache.flink.state.changelog.AbstractStateChangeLogger.StateChangeOperation.REMOVE_ELEMENT;
import static org.apache.flink.state.changelog.AbstractStateChangeLogger.StateChangeOperation.SET;
import static org.apache.flink.state.changelog.AbstractStateChangeLogger.StateChangeOperation.SET_INTERNAL;
import static org.apache.flink.util.Preconditions.checkNotNull;

abstract class AbstractStateChangeLogger<Key, Value, Ns> implements StateChangeLogger<Value, Ns> {
    static final int COMMON_KEY_GROUP = -1;
    protected final StateChangelogWriter<?> stateChangelogWriter;
    protected final InternalKeyContext<Key> keyContext;
    protected final RegisteredStateMetaInfoBase metaInfo;
    private boolean metaDataWritten = false;

    public AbstractStateChangeLogger(
            StateChangelogWriter<?> stateChangelogWriter,
            InternalKeyContext<Key> keyContext,
            RegisteredStateMetaInfoBase metaInfo) {
        this.stateChangelogWriter = checkNotNull(stateChangelogWriter);
        this.keyContext = checkNotNull(keyContext);
        this.metaInfo = checkNotNull(metaInfo);
    }

    @Override
    public void valueUpdated(Value newValue, Ns ns) throws IOException {
        if (newValue == null) {
            valueCleared(ns);
        } else {
            log(SET, out -> serializeValue(newValue, out), ns);
        }
    }

    @Override
    public void valueUpdatedInternal(Value newValue, Ns ns) throws IOException {
        if (newValue == null) {
            valueCleared(ns);
        } else {
            log(SET_INTERNAL, out -> serializeValue(newValue, out), ns);
        }
    }

    protected abstract void serializeValue(Value value, DataOutputViewStreamWrapper out)
            throws IOException;

    @Override
    public void valueAdded(Value addedValue, Ns ns) throws IOException {
        log(ADD, out -> serializeValue(addedValue, out), ns);
    }

    @Override
    public void valueCleared(Ns ns) throws IOException {
        log(CLEAR, ns);
    }

    @Override
    public void valueElementAdded(
            ThrowingConsumer<DataOutputViewStreamWrapper, IOException> dataSerializer, Ns ns)
            throws IOException {
        log(ADD_ELEMENT, dataSerializer, ns);
    }

    @Override
    public void valueElementAddedOrUpdated(
            ThrowingConsumer<DataOutputViewStreamWrapper, IOException> dataSerializer, Ns ns)
            throws IOException {
        log(ADD_OR_UPDATE_ELEMENT, dataSerializer, ns);
    }

    @Override
    public void valueElementRemoved(
            ThrowingConsumer<DataOutputViewStreamWrapper, IOException> dataSerializer, Ns ns)
            throws IOException {
        log(REMOVE_ELEMENT, dataSerializer, ns);
    }

    protected void log(StateChangeOperation op, Ns ns) throws IOException {
        logMetaIfNeeded();
        stateChangelogWriter.append(keyContext.getCurrentKeyGroupIndex(), serialize(op, ns, null));
    }

    protected void log(
            StateChangeOperation op,
            @Nullable ThrowingConsumer<DataOutputViewStreamWrapper, IOException> dataWriter,
            Ns ns)
            throws IOException {
        logMetaIfNeeded();
        stateChangelogWriter.append(
                keyContext.getCurrentKeyGroupIndex(), serialize(op, ns, dataWriter));
    }

    private void logMetaIfNeeded() throws IOException {
        if (!metaDataWritten) {
            // todo: add StateChangelogWriter.append() version without a keygroup
            //     when all callers and implementers are merged (FLINK-21356 or later)
            stateChangelogWriter.append(
                    COMMON_KEY_GROUP,
                    serializeRaw(
                            out -> {
                                out.writeByte(METADATA.code);
                                out.writeInt(CURRENT_STATE_META_INFO_SNAPSHOT_VERSION);
                                StateMetaInfoSnapshotReadersWriters.getWriter()
                                        .writeStateMetaInfoSnapshot(metaInfo.snapshot(), out);
                            }));
            metaDataWritten = true;
        }
    }

    private byte[] serialize(
            StateChangeOperation op,
            Ns ns,
            @Nullable ThrowingConsumer<DataOutputViewStreamWrapper, IOException> dataWriter)
            throws IOException {
        return serializeRaw(
                wrapper -> {
                    wrapper.writeByte(op.code);
                    // todo: optimize in FLINK-22944 by either writing short code or grouping and
                    // writing once (same for key, ns)
                    wrapper.writeUTF(metaInfo.getName());
                    serializeScope(ns, wrapper);
                    if (dataWriter != null) {
                        dataWriter.accept(wrapper);
                    }
                });
    }

    protected abstract void serializeScope(Ns ns, DataOutputViewStreamWrapper out)
            throws IOException;

    private byte[] serializeRaw(
            ThrowingConsumer<DataOutputViewStreamWrapper, IOException> dataWriter)
            throws IOException {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream();
                DataOutputViewStreamWrapper wrapper = new DataOutputViewStreamWrapper(out)) {
            dataWriter.accept(wrapper);
            return out.toByteArray();
        }
    }

    enum StateChangeOperation {
        /** Scope: key + namespace. */
        CLEAR((byte) 0),
        /** Scope: key + namespace. */
        SET((byte) 1),
        /** Scope: key + namespace. */
        SET_INTERNAL((byte) 2),
        /** Scope: key + namespace. */
        ADD((byte) 3),
        /** Scope: key + namespace, also affecting other (source) namespaces. */
        MERGE_NS((byte) 4),
        /** Scope: key + namespace + element (e.g. user list append). */
        ADD_ELEMENT((byte) 5),
        /** Scope: key + namespace + element (e.g. user map key put). */
        ADD_OR_UPDATE_ELEMENT((byte) 6),
        /** Scope: key + namespace + element (e.g. user map remove or iterator remove). */
        REMOVE_ELEMENT((byte) 7),
        /** Scope: key + namespace, first element (e.g. priority queue poll). */
        REMOVE_FIRST_ELEMENT((byte) 8),
        /** State metadata (name, serializers, etc.). */
        METADATA((byte) 9);
        private final byte code;

        StateChangeOperation(byte code) {
            this.code = code;
        }

        private static final Map<Byte, KvStateChangeLoggerImpl.StateChangeOperation> BY_CODES =
                Arrays.stream(AbstractStateChangeLogger.StateChangeOperation.values())
                        .collect(Collectors.toMap(o -> o.code, Function.identity()));

        public static StateChangeOperation byCode(byte opCode) {
            return checkNotNull(BY_CODES.get(opCode), Byte.toString(opCode));
        }

        public byte getCode() {
            return code;
        }
    }
}
