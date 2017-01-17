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

package org.apache.flink.test.query;


import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.contrib.streaming.state.PredefinedOptions;
import org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackend;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.query.netty.message.KvStateRequestSerializer;
import org.apache.flink.runtime.query.netty.message.KvStateRequestSerializerTest;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;

import java.io.File;

import static org.mockito.Mockito.mock;

/**
 * Additional tests for the serialization and deserialization of {@link
 * KvStateRequestSerializer} with a RocksDB state back-end.
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
	final static class RocksDBKeyedStateBackend2<K> extends RocksDBKeyedStateBackend<K> {

		RocksDBKeyedStateBackend2(final JobID jobId,
			final String operatorIdentifier,
			final ClassLoader userCodeClassLoader,
			final File instanceBasePath, final DBOptions dbOptions,
			final ColumnFamilyOptions columnFamilyOptions,
			final TaskKvStateRegistry kvStateRegistry,
			final TypeSerializer<K> keySerializer, final int numberOfKeyGroups,
			final KeyGroupRange keyGroupRange) throws Exception {
			super(jobId, operatorIdentifier, userCodeClassLoader,
				instanceBasePath,
				dbOptions, columnFamilyOptions, kvStateRegistry, keySerializer,
				numberOfKeyGroups, keyGroupRange);
		}

		@Override
		public <N, T> ListState<T> createListState(
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
		final long key = 0l;
		TypeSerializer<Long> valueSerializer = LongSerializer.INSTANCE;

		// objects for RocksDB state list serialisation
		DBOptions dbOptions = PredefinedOptions.DEFAULT.createDBOptions();
		dbOptions.setCreateIfMissing(true);
		ColumnFamilyOptions columnFamilyOptions = PredefinedOptions.DEFAULT.createColumnOptions();
		final RocksDBKeyedStateBackend2<Long> longHeapKeyedStateBackend =
			new RocksDBKeyedStateBackend2<>(
				new JobID(), "no-op",
				ClassLoader.getSystemClassLoader(),
				temporaryFolder.getRoot(),
				dbOptions,
				columnFamilyOptions,
				mock(TaskKvStateRegistry.class),
				LongSerializer.INSTANCE,
				1, new KeyGroupRange(0, 0)
			);
		longHeapKeyedStateBackend.setCurrentKey(key);

		final ListState<Long> listState = longHeapKeyedStateBackend
			.createListState(VoidNamespaceSerializer.INSTANCE,
				new ListStateDescriptor<>("test", LongSerializer.INSTANCE));
		KvStateRequestSerializerTest.testListSerialization(key, listState);
	}
}
