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

package org.apache.flink.table.runtime.typeutils;

import org.apache.flink.api.common.typeutils.NestedSerializersSnapshotDelegate;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.java.typeutils.runtime.DataInputViewStream;
import org.apache.flink.api.java.typeutils.runtime.DataOutputViewStream;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;
import java.util.Comparator;
import java.util.SortedMap;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * Snapshot class for the {@link SortedMapSerializer}.
 */
public class SortedMapSerializerSnapshot<K, V> implements TypeSerializerSnapshot<SortedMap<K, V>> {

	private Comparator<K> comparator;

	private NestedSerializersSnapshotDelegate nestedSerializersSnapshotDelegate;

	private static final int CURRENT_VERSION = 3;

	@SuppressWarnings("unused")
	public SortedMapSerializerSnapshot() {
		// this constructor is used when restoring from a checkpoint/savepoint.
	}

	SortedMapSerializerSnapshot(SortedMapSerializer<K, V> sortedMapSerializer) {
		this.comparator = sortedMapSerializer.getComparator();
		TypeSerializer[] typeSerializers =
				new TypeSerializer<?>[] { sortedMapSerializer.getKeySerializer(), sortedMapSerializer.getValueSerializer() };
		this.nestedSerializersSnapshotDelegate = new NestedSerializersSnapshotDelegate(typeSerializers);
	}

	@Override
	public int getCurrentVersion() {
		return CURRENT_VERSION;
	}

	@Override
	public void writeSnapshot(DataOutputView out) throws IOException {
		checkState(comparator != null, "Comparator cannot be null.");
		InstantiationUtil.serializeObject(new DataOutputViewStream(out), comparator);
		nestedSerializersSnapshotDelegate.writeNestedSerializerSnapshots(out);
	}

	@Override
	public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader) throws IOException {
		try {
			comparator = InstantiationUtil.deserializeObject(
					new DataInputViewStream(in), userCodeClassLoader);
		} catch (ClassNotFoundException e) {
			throw new IOException(e);
		}
		this.nestedSerializersSnapshotDelegate = NestedSerializersSnapshotDelegate.readNestedSerializerSnapshots(
				in,
				userCodeClassLoader);
	}

	@Override
	public SortedMapSerializer restoreSerializer() {
		TypeSerializer<?>[] nestedSerializers = nestedSerializersSnapshotDelegate.getRestoredNestedSerializers();
		@SuppressWarnings("unchecked")
		TypeSerializer<K> keySerializer = (TypeSerializer<K>) nestedSerializers[0];

		@SuppressWarnings("unchecked")
		TypeSerializer<V> valueSerializer = (TypeSerializer<V>) nestedSerializers[1];

		return new SortedMapSerializer(comparator, keySerializer, valueSerializer);
	}

	@Override
	public TypeSerializerSchemaCompatibility<SortedMap<K, V>> resolveSchemaCompatibility(
			TypeSerializer<SortedMap<K, V>> newSerializer) {
		if (!(newSerializer instanceof SortedMapSerializer)) {
			return TypeSerializerSchemaCompatibility.incompatible();
		}
		SortedMapSerializer newSortedMapSerializer = (SortedMapSerializer) newSerializer;
		if (!comparator.equals(newSortedMapSerializer.getComparator())) {
			return TypeSerializerSchemaCompatibility.incompatible();
		} else {
			return TypeSerializerSchemaCompatibility.compatibleAsIs();
		}
	}
}
