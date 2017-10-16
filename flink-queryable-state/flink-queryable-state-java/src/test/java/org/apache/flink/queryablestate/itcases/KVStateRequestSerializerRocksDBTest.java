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

package org.apache.flink.queryablestate.itcases;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.contrib.streaming.state.PredefinedOptions;
import org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackend;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.query.netty.message.KvStateRequestSerializerTest;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.internal.InternalListState;
import org.apache.flink.runtime.state.internal.InternalMapState;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;

import java.io.File;

import static org.mockito.Mockito.mock;

/**
 * Additional tests for the serialization and deserialization using
 * the KvStateSerializer with a RocksDB state back-end.
 */
public final class KVStateRequestSerializerRocksDBTest {

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	/**
	 * Extension of {@link RocksDBKeyedStateBackend} to make {@link
	 * #createListState(TypeSerializer, ListStateDescriptor)} public for use in
	 * the tests.
	 *
	 * @param <K> key type
	 */
	static final class RocksDBKeyedStateBackend2<K> extends RocksDBKeyedStateBackend<K> {

		RocksDBKeyedStateBackend2(
				final String operatorIdentifier,
				final ClassLoader userCodeClassLoader,
				final File instanceBasePath,
				final DBOptions dbOptions,
				final ColumnFamilyOptions columnFamilyOptions,
				final TaskKvStateRegistry kvStateRegistry,
				final TypeSerializer<K> keySerializer,
				final int numberOfKeyGroups,
				final KeyGroupRange keyGroupRange,
				final ExecutionConfig executionConfig) throws Exception {

			super(operatorIdentifier, userCodeClassLoader,
				instanceBasePath,
				dbOptions, columnFamilyOptions, kvStateRegistry, keySerializer,
				numberOfKeyGroups, keyGroupRange, executionConfig, false);
		}

		@Override
		public <N, T> InternalListState<N, T> createListState(
			final TypeSerializer<N> namespaceSerializer,
			final ListStateDescriptor<T> stateDesc) throws Exception {

			return super.createListState(namespaceSerializer, stateDesc);
		}
	}

	/**
	 * Tests list serialization and deserialization match.
	 *
	 * @see KvStateRequestSerializerTest#testListSerialization()
	 * KvStateRequestSerializerTest#testListSerialization() using the heap state back-end
	 * test
	 */
	@Test
	public void testListSerialization() throws Exception {
		final long key = 0L;

		// objects for RocksDB state list serialisation
		DBOptions dbOptions = PredefinedOptions.DEFAULT.createDBOptions();
		dbOptions.setCreateIfMissing(true);
		ColumnFamilyOptions columnFamilyOptions = PredefinedOptions.DEFAULT.createColumnOptions();
		final RocksDBKeyedStateBackend2<Long> longHeapKeyedStateBackend =
			new RocksDBKeyedStateBackend2<>(
				"no-op",
				ClassLoader.getSystemClassLoader(),
				temporaryFolder.getRoot(),
				dbOptions,
				columnFamilyOptions,
				mock(TaskKvStateRegistry.class),
				LongSerializer.INSTANCE,
				1, new KeyGroupRange(0, 0),
				new ExecutionConfig()
			);
		longHeapKeyedStateBackend.restore(null);
		longHeapKeyedStateBackend.setCurrentKey(key);

		final InternalListState<VoidNamespace, Long> listState = longHeapKeyedStateBackend
			.createListState(VoidNamespaceSerializer.INSTANCE,
				new ListStateDescriptor<>("test", LongSerializer.INSTANCE));

		KvStateRequestSerializerTest.testListSerialization(key, listState);
	}

	/**
	 * Tests map serialization and deserialization match.
	 *
	 * @see KvStateRequestSerializerTest#testMapSerialization()
	 * KvStateRequestSerializerTest#testMapSerialization() using the heap state back-end
	 * test
	 */
	@Test
	public void testMapSerialization() throws Exception {
		final long key = 0L;

		// objects for RocksDB state list serialisation
		DBOptions dbOptions = PredefinedOptions.DEFAULT.createDBOptions();
		dbOptions.setCreateIfMissing(true);
		ColumnFamilyOptions columnFamilyOptions = PredefinedOptions.DEFAULT.createColumnOptions();
		final RocksDBKeyedStateBackend<Long> longHeapKeyedStateBackend =
			new RocksDBKeyedStateBackend<>(
				"no-op",
				ClassLoader.getSystemClassLoader(),
				temporaryFolder.getRoot(),
				dbOptions,
				columnFamilyOptions,
				mock(TaskKvStateRegistry.class),
				LongSerializer.INSTANCE,
				1, new KeyGroupRange(0, 0),
				new ExecutionConfig(),
				false);
		longHeapKeyedStateBackend.restore(null);
		longHeapKeyedStateBackend.setCurrentKey(key);

		final InternalMapState<VoidNamespace, Long, String> mapState = (InternalMapState<VoidNamespace, Long, String>)
				longHeapKeyedStateBackend.getPartitionedState(
						VoidNamespace.INSTANCE,
						VoidNamespaceSerializer.INSTANCE,
						new MapStateDescriptor<>("test", LongSerializer.INSTANCE, StringSerializer.INSTANCE));

		KvStateRequestSerializerTest.testMapSerialization(key, mapState);
	}
}
