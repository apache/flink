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

import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.RegisteredStateMetaInfoBase;
import org.apache.flink.runtime.state.changelog.StateChangelogWriter;
import org.apache.flink.runtime.state.heap.InternalKeyContext;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Collection;

import static org.apache.flink.state.changelog.StateChangeOperation.MERGE_NS;
import static org.apache.flink.util.Preconditions.checkNotNull;

@NotThreadSafe
class KvStateChangeLoggerImpl<Key, Value, Ns> extends AbstractStateChangeLogger<Key, Value, Ns>
        implements KvStateChangeLogger<Value, Ns> {

    private TypeSerializer<Ns> namespaceSerializer;
    protected final TypeSerializer<Key> keySerializer;
    private TypeSerializer<Value> valueSerializer;
    private StateTtlConfig ttlConfig;
    @Nullable private Value defaultValue;

    KvStateChangeLoggerImpl(
            TypeSerializer<Key> keySerializer,
            TypeSerializer<Ns> namespaceSerializer,
            TypeSerializer<Value> valueSerializer,
            InternalKeyContext<Key> keyContext,
            StateChangelogWriter<?> stateChangelogWriter,
            RegisteredStateMetaInfoBase metaInfo,
            StateTtlConfig ttlConfig,
            @Nullable Value defaultValue,
            short stateId) {
        super(stateChangelogWriter, keyContext, metaInfo, stateId);
        this.keySerializer = checkNotNull(keySerializer);
        this.valueSerializer = checkNotNull(valueSerializer);
        this.namespaceSerializer = checkNotNull(namespaceSerializer);
        this.ttlConfig = checkNotNull(ttlConfig);
        this.defaultValue = defaultValue;
    }

    @Override
    public void namespacesMerged(Ns target, Collection<Ns> sources) throws IOException {
        log(
                MERGE_NS,
                out -> {
                    namespaceSerializer.serialize(target, out);
                    out.writeInt(sources.size());
                    for (Ns ns : sources) {
                        namespaceSerializer.serialize(ns, out);
                    }
                },
                target);
    }

    @Override
    protected void serializeValue(Value value, DataOutputView out) throws IOException {
        valueSerializer.serialize(value, out);
    }

    @Override
    protected void serializeScope(Ns ns, DataOutputView out) throws IOException {
        keySerializer.serialize(keyContext.getCurrentKey(), out);
        namespaceSerializer.serialize(ns, out);
    }

    protected void writeDefaultValueAndTtl(DataOutputView out) throws IOException {
        out.writeBoolean(ttlConfig.isEnabled());
        if (ttlConfig.isEnabled()) {
            try (ByteArrayOutputStreamWithPos outputStreamWithPos =
                            new ByteArrayOutputStreamWithPos();
                    ObjectOutputStream objectOutputStream =
                            new ObjectOutputStream(outputStreamWithPos)) {
                objectOutputStream.writeObject(ttlConfig);
                out.write(outputStreamWithPos.toByteArray());
            }
        }
        out.writeBoolean(defaultValue != null);
        if (defaultValue != null) {
            serializeValue(defaultValue, out);
        }
    }

    @Override
    protected KvStateChangeLoggerImpl<Key, Value, Ns> setMetaInfo(
            RegisteredStateMetaInfoBase metaInfo) {
        super.setMetaInfo(metaInfo);
        @SuppressWarnings("unchecked")
        RegisteredKeyValueStateBackendMetaInfo<Ns, Value> kvMetaInfo =
                (RegisteredKeyValueStateBackendMetaInfo<Ns, Value>) metaInfo;
        this.namespaceSerializer = kvMetaInfo.getNamespaceSerializer();
        this.valueSerializer = kvMetaInfo.getStateSerializer();
        return this;
    }

    KvStateChangeLoggerImpl<Key, Value, Ns> setStateTtlConfig(StateTtlConfig ttlConfig) {
        this.ttlConfig = ttlConfig;
        return this;
    }

    KvStateChangeLoggerImpl<Key, Value, Ns> setDefaultValue(Value defaultValue) {
        this.defaultValue = defaultValue;
        return this;
    }
}
