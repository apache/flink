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

package org.apache.flink.streaming.python.util.serialization;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.FlinkRuntimeException;

import java.io.IOException;

/**
 * A Python deserialization schema, which implements {@link DeserializationSchema}. It converts a
 * serialized form of {@code PyObject} into its Java Object representation.
 */
public class PythonDeserializationSchema implements DeserializationSchema<Object> {
	private static final long serialVersionUID = -9180596504893036458L;
	private final TypeInformation<Object> resultType = TypeInformation.of(new TypeHint<Object>() {});

	private final byte[] serSchema;
	private transient DeserializationSchema<Object> schema;

	public PythonDeserializationSchema(DeserializationSchema<Object> schema) throws IOException {
		this.serSchema = SerializationUtils.serializeObject(schema);
	}

	@SuppressWarnings("unchecked")
	public Object deserialize(byte[] message) throws IOException {
		if (this.schema == null) {
			try {
				this.schema = SerializationUtils.deserializeObject(this.serSchema);
			} catch (Exception e) {
				throw new FlinkRuntimeException("Schema could not be deserialized.", e);
			}
		}
		return this.schema.deserialize(message);
	}

	@Override
	public boolean isEndOfStream(Object nextElement) {
		return this.schema.isEndOfStream(nextElement);
	}

	@Override
	public TypeInformation<Object> getProducedType() {
		return resultType;
	}
}
