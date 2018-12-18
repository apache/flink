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

package org.apache.flink.table.dataview;

import org.apache.flink.api.common.typeutils.CompositeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.table.api.dataview.ListView;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * A {@link TypeSerializerSnapshot} for the {@link ListViewSerializer}.
 *
 * @param <T> the type of the list elements.
 */
public final class ListViewSerializerSnapshot<T> implements TypeSerializerSnapshot<ListView<T>> {

	private static final int CURRENT_VERSION = 1;

	private CompositeSerializerSnapshot nestedListSerializerSnapshot;

	/**
	 * Constructor for read instantiation.
	 */
	public ListViewSerializerSnapshot() {}

	/**
	 * Constructor to create the snapshot for writing.
	 */
	public ListViewSerializerSnapshot(TypeSerializer<List<T>> listSerializer) {
		this.nestedListSerializerSnapshot = new CompositeSerializerSnapshot(Preconditions.checkNotNull(listSerializer));
	}

	@Override
	public int getCurrentVersion() {
		return CURRENT_VERSION;
	}

	@Override
	public TypeSerializer<ListView<T>> restoreSerializer() {
		return new ListViewSerializer<>(nestedListSerializerSnapshot.getRestoreSerializer(0));
	}

	@Override
	public TypeSerializerSchemaCompatibility<ListView<T>> resolveSchemaCompatibility(TypeSerializer<ListView<T>> newSerializer) {
		checkState(nestedListSerializerSnapshot != null);

		if (newSerializer instanceof ListViewSerializer) {
			ListViewSerializer<T> serializer = (ListViewSerializer<T>) newSerializer;

			return nestedListSerializerSnapshot.resolveCompatibilityWithNested(
				TypeSerializerSchemaCompatibility.compatibleAsIs(),
				serializer.getListSerializer());
		}
		else {
			return TypeSerializerSchemaCompatibility.incompatible();
		}
	}

	@Override
	public void writeSnapshot(DataOutputView out) throws IOException {
		nestedListSerializerSnapshot.writeCompositeSnapshot(out);
	}

	@Override
	public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader) throws IOException {
		this.nestedListSerializerSnapshot = CompositeSerializerSnapshot.readCompositeSnapshot(in, userCodeClassLoader);
	}
}
