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

package org.apache.flink.formats.avro.typeutils;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import org.apache.avro.Schema;
import org.apache.avro.SchemaCompatibility;
import org.apache.avro.SchemaCompatibility.SchemaPairCompatibility;

import java.io.IOException;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An {@code Avro} specific implementation of a {@link TypeSerializerSnapshot}.
 *
 * @param <T> The data type that the originating serializer of this configuration serializes.
 */
public final class AvroSerializerSnapshot<T> implements TypeSerializerSnapshot<T> {
	private Class<T> runtimeType;
	private Schema schema;

	@SuppressWarnings("WeakerAccess")
	public AvroSerializerSnapshot() {
		// this constructor is used when restoring from a checkpoint.
	}

	AvroSerializerSnapshot(Schema schema, Class<T> runtimeType) {
		this.schema = schema;
		this.runtimeType = runtimeType;
	}

	@Override
	public int getCurrentVersion() {
		return 1;
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		checkNotNull(runtimeType);
		checkNotNull(schema);

		out.writeUTF(runtimeType.getName());
		out.writeUTF(schema.toString(false));
	}

	@Override
	public void read(int readVersion, DataInputView in, ClassLoader userCodeClassLoader) throws IOException {
		final String previousRuntimeClassName = in.readUTF();
		final String previousSchemaDefinition = in.readUTF();

		this.runtimeType = findClassOrThrow(userCodeClassLoader, previousRuntimeClassName);
		this.schema = parseAvroSchema(previousSchemaDefinition);
	}

	@Override
	public <NS extends TypeSerializer<T>> TypeSerializerSchemaCompatibility<T, NS>
	resolveSchemaCompatibility(NS newSerializer) {
		if (!(newSerializer instanceof AvroSerializer)) {
			return TypeSerializerSchemaCompatibility.incompatible();
		}
		AvroSerializer<?> newAvroSerializer = (AvroSerializer<?>) newSerializer;
		return resolveSchemaCompatibility(schema, newAvroSerializer.getAvroSchema());
	}

	@Override
	public TypeSerializer<T> restoreSerializer() {
		checkNotNull(runtimeType);
		checkNotNull(schema);

		if (AvroSerializer.isGenericRecord(runtimeType)) {
			return new AvroSerializer<>(runtimeType, schema);
		}
		else {
			return new AvroSerializer<>(runtimeType);
		}
	}

	// ------------------------------------------------------------------------------------------------------------
	// Helpers
	// ------------------------------------------------------------------------------------------------------------

	/**
	 * Resolves writer/reader schema compatibly.
	 *
	 * <p>Checks whenever a new version of a schema (reader) can read values serialized with the old schema (writer).
	 * If the schemas are compatible according to {@code Avro} schema resolution rules
	 * (@see <a href="https://avro.apache.org/docs/current/spec.html#Schema+Resolution">Schema Resolution</a>).
	 */
	@VisibleForTesting
	static <T, NS extends TypeSerializer<T>> TypeSerializerSchemaCompatibility<T, NS> resolveSchemaCompatibility(
		Schema writerSchema,
		Schema readerSchema) {

		if (Objects.equals(writerSchema, readerSchema)) {
			return TypeSerializerSchemaCompatibility.compatibleAsIs();
		}

		final SchemaPairCompatibility compatibility =
			SchemaCompatibility.checkReaderWriterCompatibility(readerSchema, writerSchema);

		return avroCompatibilityToFlinkCompatibility(compatibility);
	}

	private static <T, NS extends TypeSerializer<T>> TypeSerializerSchemaCompatibility<T, NS>
	avroCompatibilityToFlinkCompatibility(SchemaPairCompatibility compatibility) {
		switch (compatibility.getType()) {
			case COMPATIBLE: {
				// The new serializer would be able to read data persisted with *this* serializer, therefore no migration
				// is required.
				return TypeSerializerSchemaCompatibility.compatibleAsIs();
			}
			case INCOMPATIBLE: {
				return TypeSerializerSchemaCompatibility.compatibleAfterMigration();
			}
			case RECURSION_IN_PROGRESS:
			default:
				return TypeSerializerSchemaCompatibility.incompatible();
		}
	}

	private static Schema parseAvroSchema(String previousSchemaDefinition) {
		Schema.Parser parser = new Schema.Parser();
		return parser.parse(previousSchemaDefinition);
	}

	@SuppressWarnings("unchecked")
	private static <T> Class<T> findClassOrThrow(ClassLoader userCodeClassLoader, String className) {
		try {
			Class<?> runtimeTarget = Class.forName(className, false, userCodeClassLoader);
			return (Class<T>) runtimeTarget;
		}
		catch (ClassNotFoundException e) {
			throw new IllegalStateException(""
				+ "Unable to find the class '" + className + "' which is used to deserialize "
				+ "the elements of this serializer. "
				+ "Were the class was moved or renamed?", e);
		}
	}
}
