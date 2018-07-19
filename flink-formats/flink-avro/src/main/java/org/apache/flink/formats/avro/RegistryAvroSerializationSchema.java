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

package org.apache.flink.formats.avro;

import org.apache.avro.io.Encoder;
import org.apache.avro.specific.SpecificRecord;

import java.io.IOException;

/**
 * Serialization schema that serializes from Avro binary format.
 *
 * @param <T> type of record it produces
 */
public class RegistryAvroSerializationSchema<T> extends AvroSerializationSchema<T> {

	private static final long serialVersionUID = -6766681879020862312L;

	/** Provider for schema coder. Used for initializing in each task. */
	private final SchemaCoder.SchemaCoderProvider schemaCoderProvider;

	/** Class to serialize to. */
	private final Class<T> recordClazz;

	/**
	 * Creates Avro Serialization schema.
	 *
	 * @param recordClazz         class to which serialize which is
	 *                            {@link SpecificRecord}.
	 * @param schemaCoderProvider schema provider that allows instantiation of {@link SchemaCoder} that will be used for
	 *                            schema writing
	 */
	public RegistryAvroSerializationSchema(Class<T> recordClazz, SchemaCoder.SchemaCoderProvider schemaCoderProvider) {
		super(recordClazz);
		this.schemaCoderProvider = schemaCoderProvider;
		this.recordClazz = recordClazz;

	}

	@Override
	public byte[] serialize(T object) {
		checkAvroInitialized();

		if (object == null) {
			return null;
		} else {
			try {
				Encoder encoder = getEncoder();
				byte[] bytes = schemaCoderProvider.get()
					.writeSchema(recordClazz, getOutputStream());
				getDatumWriter().write(object, encoder);
				encoder.flush();

				return bytes;
			} catch (RuntimeException | IOException e) {
				throw new RuntimeException("Failed to serialize schema registry.", e);
			}
		}
	}
}
