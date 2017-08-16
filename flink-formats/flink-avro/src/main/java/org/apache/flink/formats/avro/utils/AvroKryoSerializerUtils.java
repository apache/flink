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

package org.apache.flink.formats.avro.utils;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.typeutils.runtime.kryo.Serializers;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;

import java.io.Serializable;

/**
 * Utilities for integrating Avro serializers in Kryo.
 */
public class AvroKryoSerializerUtils {

	public static void addAvroSerializers(ExecutionConfig reg, Class<?> type) {
		// Avro POJOs contain java.util.List which have GenericData.Array as their runtime type
		// because Kryo is not able to serialize them properly, we use this serializer for them
		reg.registerTypeWithKryoSerializer(GenericData.Array.class, Serializers.SpecificInstanceCollectionSerializerForArrayList.class);

		// We register this serializer for users who want to use untyped Avro records (GenericData.Record).
		// Kryo is able to serialize everything in there, except for the Schema.
		// This serializer is very slow, but using the GenericData.Records of Kryo is in general a bad idea.
		// we add the serializer as a default serializer because Avro is using a private sub-type at runtime.
		reg.addDefaultKryoSerializer(Schema.class, AvroSchemaSerializer.class);
	}

	/**
	 * Slow serialization approach for Avro schemas.
	 * This is only used with {{@link org.apache.avro.generic.GenericData.Record}} types.
	 * Having this serializer, we are able to handle avro Records.
	 */
	public static class AvroSchemaSerializer extends Serializer<Schema> implements Serializable {
		private static final long serialVersionUID = 1L;

		@Override
		public void write(Kryo kryo, Output output, Schema object) {
			String schemaAsString = object.toString(false);
			output.writeString(schemaAsString);
		}

		@Override
		public Schema read(Kryo kryo, Input input, Class<Schema> type) {
			String schemaAsString = input.readString();
			// the parser seems to be stateful, to we need a new one for every type.
			Schema.Parser sParser = new Schema.Parser();
			return sParser.parse(schemaAsString);
		}
	}
}
