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
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecord;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.Objects;

import static org.apache.flink.formats.avro.typeutils.AvroSerializer.isGenericRecord;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An {@code Avro} specific implementation of a {@link TypeSerializerSnapshot}.
 *
 * @param <T> The data type that the originating serializer of this configuration serializes.
 */
public class AvroSerializerSnapshot<T> implements TypeSerializerSnapshot<T> {
	private Class<T> runtimeType;
	private Schema schema;
	private Schema runtimeSchema;

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
		return 2;
	}

	@Override
	public void writeSnapshot(DataOutputView out) throws IOException {
		checkNotNull(runtimeType);
		checkNotNull(schema);

		out.writeUTF(runtimeType.getName());
		out.writeUTF(schema.toString(false));
	}

	@SuppressWarnings("unchecked")
	@Override
	public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader) throws IOException {
		switch (readVersion) {
			case 1: {
				readV1(in, userCodeClassLoader);
				return;
			}
			case 2: {
				readV2(in, userCodeClassLoader);
				return;
			}
			default:
				throw new IllegalArgumentException("unknown snapshot version for AvroSerializerSnapshot " + readVersion);
		}
	}

	private void readV1(DataInputView in, ClassLoader userCodeClassLoader) throws IOException {
		final String previousSchemaDefinition = in.readUTF();
		this.schema = parseAvroSchema(previousSchemaDefinition);
		this.runtimeType = findClassOrFallbackToGeneric(userCodeClassLoader, schema.getFullName());
		this.runtimeSchema = tryExtractAvroSchema(userCodeClassLoader, runtimeType);
	}

	private void readV2(DataInputView in, ClassLoader userCodeClassLoader) throws IOException {
		final String previousRuntimeTypeName = in.readUTF();
		final String previousSchemaDefinition = in.readUTF();

		this.runtimeType = findClassOrThrow(userCodeClassLoader, previousRuntimeTypeName);
		this.schema = parseAvroSchema(previousSchemaDefinition);
		this.runtimeSchema = tryExtractAvroSchema(userCodeClassLoader, runtimeType);
	}

	@Override
	public TypeSerializerSchemaCompatibility<T>
	resolveSchemaCompatibility(TypeSerializer<T> newSerializer) {
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

		if (runtimeSchema != null) {
			return new AvroSerializer<>(runtimeType, new SerializableAvroSchema(runtimeSchema), new SerializableAvroSchema(schema));
		}
		else {
			return new AvroSerializer<>(runtimeType, new SerializableAvroSchema(schema), new SerializableAvroSchema(schema));
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
	static <T> TypeSerializerSchemaCompatibility<T> resolveSchemaCompatibility(
		Schema writerSchema,
		Schema readerSchema) {

		if (Objects.equals(writerSchema, readerSchema)) {
			return TypeSerializerSchemaCompatibility.compatibleAsIs();
		}

		final SchemaPairCompatibility compatibility =
			SchemaCompatibility.checkReaderWriterCompatibility(readerSchema, writerSchema);

		return avroCompatibilityToFlinkCompatibility(compatibility);
	}

	private static <T> TypeSerializerSchemaCompatibility<T>
	avroCompatibilityToFlinkCompatibility(SchemaPairCompatibility compatibility) {

		switch (compatibility.getType()) {
			case COMPATIBLE: {
				// The new serializer would be able to read data persisted with *this* serializer, therefore no migration
				// is required.
				return TypeSerializerSchemaCompatibility.compatibleAfterMigration();
			}
			case INCOMPATIBLE: {
				return TypeSerializerSchemaCompatibility.incompatible();
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

	private static Schema tryExtractAvroSchema(ClassLoader cl, Class<?> runtimeType) {
		if (isGenericRecord(runtimeType)) {
			return null;
		}
		if (isSpecificRecord(runtimeType)) {
			SpecificData d = new SpecificData(cl);
			return d.getSchema(runtimeType);
		}
		ReflectData d = new ReflectData(cl);
		return d.getSchema(runtimeType);
	}

	@SuppressWarnings("unchecked")
	@Nonnull
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

	@SuppressWarnings("unchecked")
	@Nonnull
	private static <T> Class<T> findClassOrFallbackToGeneric(ClassLoader userCodeClassLoader, String className) {
		try {
			Class<?> runtimeTarget = Class.forName(className, false, userCodeClassLoader);
			return (Class<T>) runtimeTarget;
		}
		catch (ClassNotFoundException e) {
			return (Class<T>) GenericRecord.class;
		}
	}

	private static boolean isSpecificRecord(Class<?> runtimeType) {
		return SpecificRecord.class.isAssignableFrom(runtimeType);
	}
}
