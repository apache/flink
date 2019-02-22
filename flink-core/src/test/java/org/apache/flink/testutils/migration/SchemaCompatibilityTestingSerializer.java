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

package org.apache.flink.testutils.migration;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

/**
 * The {@link SchemaCompatibilityTestingSerializer} is a mock serializer that can be used
 * for schema compatibility and serializer upgrade related tests.
 *
 * <p>To use this, tests should always first instantiate an {@link InitialSerializer} serializer
 * instance and then take a snapshot of it. Next, depending on what the new serializer is provided
 * to the taken snapshot, tests can expect the following compatibility results:
 * <ul>
 *     <li>{@link InitialSerializer} as the new serializer: the compatibility result will be
 *         {@link TypeSerializerSchemaCompatibility#compatibleAsIs()}.</li>
 *     <li>{@link UpgradedSchemaSerializer} as the new serializer: the compatibility result will be
 *         {@link TypeSerializerSchemaCompatibility#compatibleAfterMigration()}.</li>
 *     <li>{@link ReconfigurationRequiringSerializer} as the new serializer: the compatibility result
 *         will be {@link TypeSerializerSchemaCompatibility#compatibleWithReconfiguredSerializer(TypeSerializer)},
 *         with a new instance of the {@link InitialSerializer} as the wrapped reconfigured serializer.</li>
 *     <li>{@link IncompatibleSerializer} as the new serializer: the compatibility result will be
 *         {@link TypeSerializerSchemaCompatibility#incompatible()}.</li>
 * </ul>
 */
public abstract class SchemaCompatibilityTestingSerializer<T> extends TypeSerializer<T> {

	private static final long serialVersionUID = 1L;

	// ------------------------------------------------------------------------------------------------
	//  Irrelevant serializer methods
	// ------------------------------------------------------------------------------------------------

	@Override
	public TypeSerializer<T> duplicate() {
		throw new UnsupportedOperationException(
			"this is a SchemaCompatibilityTestingSerializer; should have only been used for snapshotting / compatibility test purposes.");
	}

	@Override
	public void serialize(T record, DataOutputView target) throws IOException {
		throw new UnsupportedOperationException(
			"this is a SchemaCompatibilityTestingSerializer; should have only been used for snapshotting / compatibility test purposes.");

	}

	@Override
	public T deserialize(T reuse, DataInputView source) throws IOException {
		throw new UnsupportedOperationException(
			"this is a SchemaCompatibilityTestingSerializer; should have only been used for snapshotting / compatibility test purposes.");

	}

	@Override
	public T deserialize(DataInputView source) throws IOException {
		throw new UnsupportedOperationException(
			"this is a SchemaCompatibilityTestingSerializer; should have only been used for snapshotting / compatibility test purposes.");

	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		throw new UnsupportedOperationException(
			"this is a SchemaCompatibilityTestingSerializer; should have only been used for snapshotting / compatibility test purposes.");

	}

	@Override
	public T copy(T from) {
		throw new UnsupportedOperationException(
			"this is a SchemaCompatibilityTestingSerializer; should have only been used for snapshotting / compatibility test purposes.");

	}

	@Override
	public T copy(T from, T reuse) {
		throw new UnsupportedOperationException(
			"this is a SchemaCompatibilityTestingSerializer; should have only been used for snapshotting / compatibility test purposes.");

	}

	@Override
	public T createInstance() {
		throw new UnsupportedOperationException(
			"this is a SchemaCompatibilityTestingSerializer; should have only been used for snapshotting / compatibility test purposes.");

	}

	@Override
	public boolean isImmutableType() {
		throw new UnsupportedOperationException(
			"this is a SchemaCompatibilityTestingSerializer; should have only been used for snapshotting / compatibility test purposes.");

	}

	@Override
	public int getLength() {
		throw new UnsupportedOperationException(
			"this is a SchemaCompatibilityTestingSerializer; should have only been used for snapshotting / compatibility test purposes.");

	}

	// ------------------------------------------------------------------------------------------------
	//  Test serializer variants
	// ------------------------------------------------------------------------------------------------

	public static class InitialSerializer<T> extends SchemaCompatibilityTestingSerializer<T> {

		private static final long serialVersionUID = 1L;

		@Override
		public TypeSerializerSnapshot<T> snapshotConfiguration() {
			return new InitialSerializerSnapshot();
		}

		@Override
		public boolean equals(Object obj) {
			return obj != null && obj.getClass() == InitialSerializer.class;
		}

		@Override
		public int hashCode() {
			return getClass().hashCode();
		}
	}

	public static class UpgradedSchemaSerializer<T> extends SchemaCompatibilityTestingSerializer<T> {

		private static final long serialVersionUID = 1L;

		@Override
		public TypeSerializerSnapshot<T> snapshotConfiguration() {
			return new UpgradedSchemaSerializerSnapshot();
		}

		@Override
		public boolean equals(Object obj) {
			return obj != null && obj.getClass() == UpgradedSchemaSerializer.class;
		}

		@Override
		public int hashCode() {
			return getClass().hashCode();
		}
	}

	public static class ReconfigurationRequiringSerializer<T> extends SchemaCompatibilityTestingSerializer<T> {

		private static final long serialVersionUID = 1L;

		@Override
		public TypeSerializerSnapshot<T> snapshotConfiguration() {
			throw new UnsupportedOperationException(
				"this is a ReconfigurationRequiringSerializer; should not have been snapshotted.");
		}

		@Override
		public boolean equals(Object obj) {
			return obj != null && obj.getClass() == ReconfigurationRequiringSerializer.class;
		}

		@Override
		public int hashCode() {
			return getClass().hashCode();
		}
	}

	public static class IncompatibleSerializer<T> extends SchemaCompatibilityTestingSerializer<T> {

		private static final long serialVersionUID = 1L;

		@Override
		public TypeSerializerSnapshot<T> snapshotConfiguration() {
			throw new UnsupportedOperationException(
				"this is a IncompatibleSerializer; should not have been snapshotted.");
		}

		@Override
		public boolean equals(Object obj) {
			return obj != null && obj.getClass() == IncompatibleSerializer.class;
		}

		@Override
		public int hashCode() {
			return getClass().hashCode();
		}
	}

	// ------------------------------------------------------------------------------------------------
	//  Test serializer snapshots
	// ------------------------------------------------------------------------------------------------

	public class InitialSerializerSnapshot implements TypeSerializerSnapshot<T> {

		@Override
		public int getCurrentVersion() {
			return 1;
		}

		@Override
		public void writeSnapshot(DataOutputView out) throws IOException {}

		@Override
		public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader) throws IOException {}

		@Override
		public TypeSerializer<T> restoreSerializer() {
			return new SchemaCompatibilityTestingSerializer.InitialSerializer<>();
		}

		@Override
		public TypeSerializerSchemaCompatibility<T> resolveSchemaCompatibility(TypeSerializer<T> newSerializer) {
			if (newSerializer instanceof SchemaCompatibilityTestingSerializer.InitialSerializer) {
				return TypeSerializerSchemaCompatibility.compatibleAsIs();
			}

			if (newSerializer instanceof SchemaCompatibilityTestingSerializer.UpgradedSchemaSerializer) {
				return TypeSerializerSchemaCompatibility.compatibleAfterMigration();
			}

			if (newSerializer instanceof SchemaCompatibilityTestingSerializer.ReconfigurationRequiringSerializer) {
				return TypeSerializerSchemaCompatibility.compatibleWithReconfiguredSerializer(new SchemaCompatibilityTestingSerializer.InitialSerializer<T>());
			}

			if (newSerializer instanceof SchemaCompatibilityTestingSerializer.IncompatibleSerializer) {
				return TypeSerializerSchemaCompatibility.incompatible();
			}

			// reaches here if newSerializer isn't a SchemaCompatibilityTestingSerializer
			return TypeSerializerSchemaCompatibility.incompatible();
		}
	}

	public class UpgradedSchemaSerializerSnapshot implements TypeSerializerSnapshot<T> {

		@Override
		public int getCurrentVersion() {
			return 1;
		}

		@Override
		public void writeSnapshot(DataOutputView out) throws IOException {}

		@Override
		public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader) throws IOException {}

		@Override
		public TypeSerializer<T> restoreSerializer() {
			return new SchemaCompatibilityTestingSerializer.UpgradedSchemaSerializer<>();
		}

		@Override
		public TypeSerializerSchemaCompatibility<T> resolveSchemaCompatibility(TypeSerializer<T> newSerializer) {
			if (newSerializer instanceof SchemaCompatibilityTestingSerializer.InitialSerializer) {
				return TypeSerializerSchemaCompatibility.incompatible();
			}

			if (newSerializer instanceof SchemaCompatibilityTestingSerializer.UpgradedSchemaSerializer) {
				return TypeSerializerSchemaCompatibility.compatibleAsIs();
			}

			if (newSerializer instanceof SchemaCompatibilityTestingSerializer.ReconfigurationRequiringSerializer) {
				return TypeSerializerSchemaCompatibility.compatibleWithReconfiguredSerializer(new SchemaCompatibilityTestingSerializer.UpgradedSchemaSerializer<T>());
			}

			if (newSerializer instanceof SchemaCompatibilityTestingSerializer.IncompatibleSerializer) {
				return TypeSerializerSchemaCompatibility.incompatible();
			}

			// reaches here if newSerializer isn't a SchemaCompatibilityTestingSerializer
			return TypeSerializerSchemaCompatibility.incompatible();
		}
	}
}
