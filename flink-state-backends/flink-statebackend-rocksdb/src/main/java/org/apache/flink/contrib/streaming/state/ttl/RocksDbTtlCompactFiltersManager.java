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

package org.apache.flink.contrib.streaming.state.ttl;

import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.RegisteredStateMetaInfoBase;
import org.apache.flink.runtime.state.ttl.TtlStateFactory;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.runtime.state.ttl.TtlUtils;
import org.apache.flink.runtime.state.ttl.TtlValue;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;

import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.FlinkCompactionFilter;
import org.rocksdb.FlinkCompactionFilter.FlinkCompactionFilterFactory;
import org.rocksdb.InfoLogLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.LinkedHashMap;

/** RocksDB compaction filter utils for state with TTL. */
public class RocksDbTtlCompactFiltersManager {
	private static final Logger LOG = LoggerFactory.getLogger(FlinkCompactionFilter.class);

	/** Enables RocksDb compaction filter for State with TTL. */
	private final boolean enableTtlCompactionFilter;

	private final TtlTimeProvider ttlTimeProvider;

	/** Registered compaction filter factories. */
	private final LinkedHashMap<String, FlinkCompactionFilterFactory> compactionFilterFactories;

	public RocksDbTtlCompactFiltersManager(boolean enableTtlCompactionFilter, TtlTimeProvider ttlTimeProvider) {
		this.enableTtlCompactionFilter = enableTtlCompactionFilter;
		this.ttlTimeProvider = ttlTimeProvider;
		this.compactionFilterFactories = new LinkedHashMap<>();
	}

	public void setAndRegisterCompactFilterIfStateTtl(
		@Nonnull RegisteredStateMetaInfoBase metaInfoBase,
		@Nonnull ColumnFamilyOptions options) {

		if (enableTtlCompactionFilter && metaInfoBase instanceof RegisteredKeyValueStateBackendMetaInfo) {
			RegisteredKeyValueStateBackendMetaInfo kvMetaInfoBase = (RegisteredKeyValueStateBackendMetaInfo) metaInfoBase;
			if (TtlStateFactory.TtlSerializer.isTtlStateSerializer(kvMetaInfoBase.getStateSerializer())) {
				createAndSetCompactFilterFactory(metaInfoBase.getName(), options);
			}
		}
	}

	private void createAndSetCompactFilterFactory(String stateName, @Nonnull ColumnFamilyOptions options) {

		FlinkCompactionFilterFactory compactionFilterFactory =
			new FlinkCompactionFilterFactory(new TimeProviderWrapper(ttlTimeProvider), createRocksDbNativeLogger());
		//noinspection resource
		options.setCompactionFilterFactory(compactionFilterFactory);
		compactionFilterFactories.put(stateName, compactionFilterFactory);
	}

	private static org.rocksdb.Logger createRocksDbNativeLogger() {
		if (LOG.isDebugEnabled()) {
			// options are always needed for org.rocksdb.Logger construction (no other constructor)
			// the logger level gets configured from the options in native code
			try (DBOptions opts = new DBOptions().setInfoLogLevel(InfoLogLevel.DEBUG_LEVEL)) {
				return new org.rocksdb.Logger(opts) {
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
			@Nonnull StateDescriptor<?, ?> stateDesc,
			TypeSerializer<?> stateSerializer) {
		StateTtlConfig ttlConfig = stateDesc.getTtlConfig();
		if (ttlConfig.isEnabled() && ttlConfig.getCleanupStrategies().inRocksdbCompactFilter()) {
			if (!enableTtlCompactionFilter) {
				LOG.warn("Cannot configure RocksDB TTL compaction filter for state <{}>: " +
					"feature is disabled for the state backend.", stateDesc.getName());
				return;
			}
			FlinkCompactionFilterFactory compactionFilterFactory = compactionFilterFactories.get(stateDesc.getName());
			Preconditions.checkNotNull(compactionFilterFactory);
			long ttl = ttlConfig.getTtl().toMilliseconds();

			StateTtlConfig.RocksdbCompactFilterCleanupStrategy rocksdbCompactFilterCleanupStrategy =
				ttlConfig.getCleanupStrategies().getRocksdbCompactFilterCleanupStrategy();
			Preconditions.checkNotNull(rocksdbCompactFilterCleanupStrategy);
			long queryTimeAfterNumEntries =
				rocksdbCompactFilterCleanupStrategy.getQueryTimeAfterNumEntries();

			FlinkCompactionFilter.Config config;
			if (stateDesc instanceof ListStateDescriptor) {
				TypeSerializer<?> elemSerializer = ((ListSerializer<?>) stateSerializer).getElementSerializer();
				int len = elemSerializer.getLength();
				if (len > 0) {
					config = FlinkCompactionFilter.Config.createForFixedElementList(
						ttl, queryTimeAfterNumEntries, len + 1); // plus one byte for list element delimiter
				} else {
					config = FlinkCompactionFilter.Config.createForList(
						ttl, queryTimeAfterNumEntries,
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

	private static class ListElementFilterFactory<T> implements FlinkCompactionFilter.ListElementFilterFactory {
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
					throw new FlinkRuntimeException("Failed to deserialize list element for TTL compaction filter", e);
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
	}
}
