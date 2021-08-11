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
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
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

    private final TypeSerializer<Ns> namespaceSerializer;
    protected final TypeSerializer<Key> keySerializer;
    private final TypeSerializer<Value> valueSerializer;
    private final StateTtlConfig ttlConfig;
    @Nullable private final Value defaultValue;

    KvStateChangeLoggerImpl(
            TypeSerializer<Key> keySerializer,
            TypeSerializer<Ns> namespaceSerializer,
            TypeSerializer<Value> valueSerializer,
            InternalKeyContext<Key> keyContext,
            StateChangelogWriter<?> stateChangelogWriter,
            RegisteredStateMetaInfoBase metaInfo,
            StateTtlConfig ttlConfig,
            @Nullable Value defaultValue) {
        super(stateChangelogWriter, keyContext, metaInfo);
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
    protected void serializeValue(Value value, DataOutputViewStreamWrapper out) throws IOException {
        valueSerializer.serialize(value, out);
    }

    @Override
    protected void serializeScope(Ns ns, DataOutputViewStreamWrapper out) throws IOException {
        keySerializer.serialize(keyContext.getCurrentKey(), out);
        namespaceSerializer.serialize(ns, out);
    }

    protected void writeDefaultValueAndTtl(DataOutputViewStreamWrapper out) throws IOException {
        out.writeBoolean(ttlConfig.isEnabled());
        if (ttlConfig.isEnabled()) {
            try (ObjectOutputStream o = new ObjectOutputStream(out)) {
                o.writeObject(ttlConfig);
            }
        }
        out.writeBoolean(defaultValue != null);
        if (defaultValue != null) {
            serializeValue(defaultValue, out);
        }
    }
}
