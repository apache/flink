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

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.util.FlinkRuntimeException;

import org.python.core.PyObject;

import java.io.IOException;

/**
 * A {@link SerializationSchema} for {@link PyObject}s.
 */
public class PythonSerializationSchema implements SerializationSchema<PyObject> {
	private static final long serialVersionUID = -9170596504893036458L;

	private final byte[] serSchema;
	private transient SerializationSchema<PyObject> schema;

	public PythonSerializationSchema(SerializationSchema<PyObject> schema) throws IOException {
		this.serSchema = SerializationUtils.serializeObject(schema);
	}

	@Override
	@SuppressWarnings("unchecked")
	public byte[] serialize(PyObject element) {
		if (this.schema == null) {
			try {
				this.schema = SerializationUtils.deserializeObject(this.serSchema);
			} catch (Exception e) {
				throw new FlinkRuntimeException("Schema could not be deserialized.", e);
			}
		}
		return this.schema.serialize(element);
	}
}
