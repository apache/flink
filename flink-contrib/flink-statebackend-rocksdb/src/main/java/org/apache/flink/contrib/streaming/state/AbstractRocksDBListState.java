/*
 * Copyright 2018 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.internal.InternalListState;

import org.rocksdb.ColumnFamilyHandle;

import java.util.List;

/**
 * A parent class for {@link RockDBListState} and {@link LargeRocksDBListState}.
 */
public abstract class AbstractRocksDBListState<K, N, V>
		extends AbstractRocksDBState<K, N, ListState<V>, ListStateDescriptor<V>, List<V>>
		implements InternalListState<N, V> {

	/** Serializer for the values. */
	final TypeSerializer<V> valueSerializer;

	/**
	 * Separator of StringAppendTestOperator in RocksDB.
	 */
	static final byte DELIMITER = ',';

	/**
	 * Creates a new {@code RocksDBListState}.
	 *
	 * @param namespaceSerializer The serializer for the namespace.
	 * @param stateDesc The state identifier for the state. This contains name
	 *                     and can create a default state value.
	 */
	public AbstractRocksDBListState(ColumnFamilyHandle columnFamily,
			TypeSerializer<N> namespaceSerializer,
			ListStateDescriptor<V> stateDesc,
			RocksDBKeyedStateBackend<K> backend) {

		super(columnFamily, namespaceSerializer, stateDesc, backend);
		this.valueSerializer = stateDesc.getElementSerializer();

		writeOptions.setDisableWAL(true);
	}

}
