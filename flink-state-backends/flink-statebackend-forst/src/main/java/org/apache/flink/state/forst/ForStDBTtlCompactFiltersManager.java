/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.state.forst;

import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.RegisteredStateMetaInfoBase;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.runtime.state.ttl.TtlUtils;
import org.apache.flink.runtime.state.ttl.TtlValue;
import org.apache.flink.runtime.state.v2.ttl.TtlStateFactory;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;

import org.forstdb.ColumnFamilyOptions;
import org.forstdb.DBOptions;
import org.forstdb.FlinkCompactionFilter;
import org.forstdb.FlinkCompactionFilter.FlinkCompactionFilterFactory;
import org.forstdb.InfoLogLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.time.Duration;
import java.util.LinkedHashMap;

/** RocksDB compaction filter utils for state with TTL. */
public class ForStDBTtlCompactFiltersManager {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkCompactionFilter.class);

    private final TtlTimeProvider ttlTimeProvider;

    /** Registered compaction filter factories. */
    private final LinkedHashMap<String, FlinkCompactionFilterFactory> compactionFilterFactories;

    /** Created column family options. */
    private final LinkedHashMap<String, ColumnFamilyOptions> columnFamilyOptionsMap;

    /**
     * Number of state entries to process by compaction filter before updating current timestamp.
     */
    private final long queryTimeAfterNumEntries;

    /**
     * Periodic compaction could speed up expired state entries cleanup, especially for state
     * entries rarely accessed. Files older than this value will be picked up for compaction, and
     * re-written to the same level as they were before. It makes sure a file goes through
     * compaction filters periodically. 0 means turning off periodic compaction.
     */
    private final Duration periodicCompactionTime;

    public ForStDBTtlCompactFiltersManager(
            TtlTimeProvider ttlTimeProvider,
            long queryTimeAfterNumEntries,
            Duration periodicCompactionTime) {
        this.ttlTimeProvider = ttlTimeProvider;
        this.queryTimeAfterNumEntries = queryTimeAfterNumEntries;
        this.periodicCompactionTime = periodicCompactionTime;
        this.compactionFilterFactories = new LinkedHashMap<>();
        this.columnFamilyOptionsMap = new LinkedHashMap<>();
    }

    public void setAndRegisterCompactFilterIfStateTtl(
            @Nonnull RegisteredStateMetaInfoBase metaInfoBase,
            @Nonnull ColumnFamilyOptions options) {

        if (metaInfoBase instanceof RegisteredKeyValueStateBackendMetaInfo) {
            RegisteredKeyValueStateBackendMetaInfo kvMetaInfoBase =
                    (RegisteredKeyValueStateBackendMetaInfo) metaInfoBase;
            if (org.apache.flink.runtime.state.ttl.TtlStateFactory.TtlSerializer
                    .isTtlStateSerializer(kvMetaInfoBase.getStateSerializer())) {
                createAndSetCompactFilterFactory(metaInfoBase.getName(), options);
            }
        }
    }

    public void setAndRegisterCompactFilterIfStateTtlV2(
            @Nonnull RegisteredStateMetaInfoBase metaInfoBase,
            @Nonnull ColumnFamilyOptions options) {

        if (metaInfoBase
                instanceof
                org.apache.flink.runtime.state.v2.RegisteredKeyValueStateBackendMetaInfo) {
            org.apache.flink.runtime.state.v2.RegisteredKeyValueStateBackendMetaInfo
                    kvMetaInfoBase =
                            (org.apache.flink.runtime.state.v2
                                            .RegisteredKeyValueStateBackendMetaInfo)
                                    metaInfoBase;
            if (TtlStateFactory.isTtlStateSerializer(kvMetaInfoBase.getStateSerializer())) {
                createAndSetCompactFilterFactory(metaInfoBase.getName(), options);
            }
        }
    }

    private void createAndSetCompactFilterFactory(
            String stateName, @Nonnull ColumnFamilyOptions options) {

        FlinkCompactionFilterFactory compactionFilterFactory =
                new FlinkCompactionFilterFactory(
                        new TimeProviderWrapper(ttlTimeProvider), createRocksDbNativeLogger());
        //noinspection resource
        options.setCompactionFilterFactory(compactionFilterFactory);
        compactionFilterFactories.put(stateName, compactionFilterFactory);
        columnFamilyOptionsMap.put(stateName, options);
    }

    private static org.forstdb.Logger createRocksDbNativeLogger() {
        if (LOG.isDebugEnabled()) {
            // options are always needed for org.forstdb.Logger construction (no other constructor)
            // the logger level gets configured from the options in native code
            try (DBOptions opts = new DBOptions().setInfoLogLevel(InfoLogLevel.DEBUG_LEVEL)) {
                return new org.forstdb.Logger(opts) {
                    @Override
                    protected void log(InfoLogLevel infoLogLevel, String logMsg) {
                        LOG.debug("RocksDB filter native code log: " + logMsg);
                    }
                };
            }
        } else {
            return null;
        }
    }

    public void configCompactFilter(
            @Nonnull StateDescriptor<?, ?> stateDesc, TypeSerializer<?> stateSerializer) {
        StateTtlConfig ttlConfig = stateDesc.getTtlConfig();
        if (ttlConfig.isEnabled() && ttlConfig.getCleanupStrategies().inRocksdbCompactFilter()) {
            FlinkCompactionFilterFactory compactionFilterFactory =
                    compactionFilterFactories.get(stateDesc.getName());
            Preconditions.checkNotNull(compactionFilterFactory);
            long ttl = ttlConfig.getTimeToLive().toMillis();

            ColumnFamilyOptions columnFamilyOptions =
                    columnFamilyOptionsMap.get(stateDesc.getName());
            Preconditions.checkNotNull(columnFamilyOptions);

            StateTtlConfig.RocksdbCompactFilterCleanupStrategy rocksdbCompactFilterCleanupStrategy =
                    ttlConfig.getCleanupStrategies().getRocksdbCompactFilterCleanupStrategy();

            Duration periodicCompactionTime = this.periodicCompactionTime;
            long queryTimeAfterNumEntries = this.queryTimeAfterNumEntries;

            if (rocksdbCompactFilterCleanupStrategy != null) {
                periodicCompactionTime =
                        rocksdbCompactFilterCleanupStrategy.getPeriodicCompactionTime();
                queryTimeAfterNumEntries =
                        rocksdbCompactFilterCleanupStrategy.getQueryTimeAfterNumEntries();
            }
            if (periodicCompactionTime != null) {
                columnFamilyOptions.setPeriodicCompactionSeconds(
                        periodicCompactionTime.getSeconds());
            }

            FlinkCompactionFilter.Config config;
            if (stateDesc instanceof ListStateDescriptor) {
                TypeSerializer<?> elemSerializer =
                        ((ListSerializer<?>) stateSerializer).getElementSerializer();
                int len = elemSerializer.getLength();
                if (len > 0) {
                    config =
                            FlinkCompactionFilter.Config.createForFixedElementList(
                                    ttl,
                                    queryTimeAfterNumEntries,
                                    len + 1); // plus one byte for list element delimiter
                } else {
                    config =
                            FlinkCompactionFilter.Config.createForList(
                                    ttl,
                                    queryTimeAfterNumEntries,
                                    new ListElementFilterFactory<>(elemSerializer.duplicate()));
                }
            } else if (stateDesc instanceof MapStateDescriptor) {
                config = FlinkCompactionFilter.Config.createForMap(ttl, queryTimeAfterNumEntries);
            } else {
                config = FlinkCompactionFilter.Config.createForValue(ttl, queryTimeAfterNumEntries);
            }
            compactionFilterFactory.configure(config);
        }
    }

    private static class ListElementFilterFactory<T>
            implements FlinkCompactionFilter.ListElementFilterFactory {
        private final TypeSerializer<T> serializer;

        private ListElementFilterFactory(TypeSerializer<T> serializer) {
            this.serializer = serializer;
        }

        @Override
        public FlinkCompactionFilter.ListElementFilter createListElementFilter() {
            return new ListElementFilter<>(serializer);
        }
    }

    private static class TimeProviderWrapper implements FlinkCompactionFilter.TimeProvider {
        private final TtlTimeProvider ttlTimeProvider;

        private TimeProviderWrapper(TtlTimeProvider ttlTimeProvider) {
            this.ttlTimeProvider = ttlTimeProvider;
        }

        @Override
        public long currentTimestamp() {
            return ttlTimeProvider.currentTimestamp();
        }
    }

    private static class ListElementFilter<T> implements FlinkCompactionFilter.ListElementFilter {
        private final TypeSerializer<T> serializer;
        private DataInputDeserializer input;

        private ListElementFilter(TypeSerializer<T> serializer) {
            this.serializer = serializer;
            this.input = new DataInputDeserializer();
        }

        @Override
        public int nextUnexpiredOffset(byte[] bytes, long ttl, long currentTimestamp) {
            input.setBuffer(bytes);
            int lastElementOffset = 0;
            while (input.available() > 0) {
                try {
                    long timestamp = nextElementLastAccessTimestamp();
                    if (!TtlUtils.expired(timestamp, ttl, currentTimestamp)) {
                        break;
                    }
                    lastElementOffset = input.getPosition();
                } catch (IOException e) {
                    throw new FlinkRuntimeException(
                            "Failed to deserialize list element for TTL compaction filter", e);
                }
            }
            return lastElementOffset;
        }

        private long nextElementLastAccessTimestamp() throws IOException {
            TtlValue<?> ttlValue = (TtlValue<?>) serializer.deserialize(input);
            if (input.available() > 0) {
                input.skipBytesToRead(1);
            }
            return ttlValue.getLastAccessTimestamp();
        }
    }

    public void disposeAndClearRegisteredCompactionFactories() {
        for (FlinkCompactionFilterFactory factory : compactionFilterFactories.values()) {
            IOUtils.closeQuietly(factory);
        }
        compactionFilterFactories.clear();
        columnFamilyOptionsMap.clear();
    }
}
