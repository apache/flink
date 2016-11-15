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

import java.io.IOException;

import org.apache.flink.streaming.util.serialization.SerializationSchema;
import org.python.core.PyObject;

/**
 * A Python serialization schema, which implements {@link SerializationSchema}. It converts
 * a {@code PyObject} into its serialized form.
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
				this.schema = (SerializationSchema<PyObject>)SerializationUtils.deserializeObject(this.serSchema);
			} catch (IOException e) {
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
		}
		return this.schema.serialize(element);
	}
}
