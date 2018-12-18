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

package org.apache.flink.runtime.state;

import org.apache.flink.api.common.typeutils.CompositeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.util.ArrayList;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * Snapshot class for the {@link ArrayListSerializer}.
 */
public class ArrayListSerializerSnapshot<T> implements TypeSerializerSnapshot<ArrayList<T>> {

	private static final int CURRENT_VERSION = 1;

	private CompositeSerializerSnapshot nestedElementSerializerSnapshot;

	/**
	 * Constructor for read instantiation.
	 */
	public ArrayListSerializerSnapshot() {}

	/**
	 * Constructor for creating the snapshot for writing.
	 */
	public ArrayListSerializerSnapshot(TypeSerializer<T> elementSerializer) {
		this.nestedElementSerializerSnapshot = new CompositeSerializerSnapshot(elementSerializer);
	}

	@Override
	public int getCurrentVersion() {
		return CURRENT_VERSION;
	}

	@Override
	public TypeSerializer<ArrayList<T>> restoreSerializer() {
		return new ArrayListSerializer<>(nestedElementSerializerSnapshot.getRestoreSerializer(0));
	}

	@Override
	public TypeSerializerSchemaCompatibility<ArrayList<T>> resolveSchemaCompatibility(TypeSerializer<ArrayList<T>> newSerializer) {
		checkState(nestedElementSerializerSnapshot != null);

		if (newSerializer instanceof ArrayListSerializer) {
			ArrayListSerializer<T> serializer = (ArrayListSerializer<T>) newSerializer;

			return nestedElementSerializerSnapshot.resolveCompatibilityWithNested(
				TypeSerializerSchemaCompatibility.compatibleAsIs(),
				serializer.getElementSerializer());
		}
		else {
			return TypeSerializerSchemaCompatibility.incompatible();
		}
	}

	@Override
	public void writeSnapshot(DataOutputView out) throws IOException {
		nestedElementSerializerSnapshot.writeCompositeSnapshot(out);
	}

	@Override
	public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader) throws IOException {
		this.nestedElementSerializerSnapshot = CompositeSerializerSnapshot.readCompositeSnapshot(in, userCodeClassLoader);
	}
}
