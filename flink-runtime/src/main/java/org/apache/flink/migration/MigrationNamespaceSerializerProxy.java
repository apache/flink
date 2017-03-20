/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.migration;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.io.Serializable;

/**
 * The purpose of this class is the be filled in as a placeholder for the namespace serializer when migrating from
 * Flink 1.1 savepoint (which did not include the namespace serializer) to Flink 1.2 (which always must include a
 * (non-null) namespace serializer. This is then replaced as soon as the user is re-registering her state again for
 * the first run under Flink 1.2 and provides again the real namespace serializer.
 *
 * @deprecated Internal class for savepoint backwards compatibility. Don't use for other purposes.
 */
@Deprecated
@SuppressWarnings("deprecation")
public class MigrationNamespaceSerializerProxy extends TypeSerializer<Serializable> {

	public static final MigrationNamespaceSerializerProxy INSTANCE = new MigrationNamespaceSerializerProxy();

	private static final long serialVersionUID = -707800010807094491L;

	private MigrationNamespaceSerializerProxy() {
	}

	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public TypeSerializer<Serializable> duplicate() {
		throw new UnsupportedOperationException(
				"This is just a proxy used during migration until the real type serializer is provided by the user.");
	}

	@Override
	public Serializable createInstance() {
		throw new UnsupportedOperationException(
				"This is just a proxy used during migration until the real type serializer is provided by the user.");
	}

	@Override
	public Serializable copy(Serializable from) {
		throw new UnsupportedOperationException(
				"This is just a proxy used during migration until the real type serializer is provided by the user.");
	}

	@Override
	public Serializable copy(Serializable from, Serializable reuse) {
		throw new UnsupportedOperationException(
				"This is just a proxy used during migration until the real type serializer is provided by the user.");
	}

	@Override
	public int getLength() {
		return -1;
	}

	@Override
	public void serialize(Serializable record, DataOutputView target) throws IOException {
		throw new UnsupportedOperationException(
				"This is just a proxy used during migration until the real type serializer is provided by the user.");
	}

	@Override
	public Serializable deserialize(DataInputView source) throws IOException {
		throw new UnsupportedOperationException(
				"This is just a proxy used during migration until the real type serializer is provided by the user.");
	}

	@Override
	public Serializable deserialize(Serializable reuse, DataInputView source) throws IOException {
		throw new UnsupportedOperationException(
				"This is just a proxy used during migration until the real type serializer is provided by the user.");
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		throw new UnsupportedOperationException(
				"This is just a proxy used during migration until the real type serializer is provided by the user.");
	}

	@Override
	public boolean equals(Object obj) {
		return obj instanceof MigrationNamespaceSerializerProxy;
	}

	@Override
	public boolean canEqual(Object obj) {
		return true;
	}

	@Override
	public int hashCode() {
		return 42;
	}
}
