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

package org.apache.flink.state.hashmap;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.KeyedBackendSerializationProxy;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoReader;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshotReadersWriters;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * Extends {@link KeyedBackendSerializationProxy} to write and read a new version so that
 * incremental snapshots can be read differently on recovery * by {@link
 * org.apache.flink.state.hashmap.IncrementalKeyGroupReader#FACTORY}.
 */
class IncrementalKeyedBackendSerializationProxy<K> extends KeyedBackendSerializationProxy<K> {
    private static final Logger LOG =
            LoggerFactory.getLogger(IncrementalKeyedBackendSerializationProxy.class);
    static final int VERSION = 100;

    public IncrementalKeyedBackendSerializationProxy(ClassLoader classLoader) {
        super(classLoader);
    }

    public IncrementalKeyedBackendSerializationProxy(
            TypeSerializer<K> keySerializer,
            List<StateMetaInfoSnapshot> stateMetaInfoSnapshots,
            boolean compression) {
        super(keySerializer, stateMetaInfoSnapshots, compression);
    }

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    protected StateMetaInfoReader getMetaInfoReader(int readVersion) throws IOException {
        if (readVersion == getVersion()) {
            LOG.debug("using modern snapshot reader, read version: {}", readVersion);
            return StateMetaInfoSnapshotReadersWriters.CurrentReaderImpl.INSTANCE;
        } else {
            LOG.debug("using older snapshot reader, read version: {}", readVersion);
            return super.getMetaInfoReader(readVersion);
        }
    }
}
