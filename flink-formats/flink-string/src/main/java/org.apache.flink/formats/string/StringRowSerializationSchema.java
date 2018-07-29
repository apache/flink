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

package org.apache.flink.formats.string;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.util.Objects;

/**
 * Serialization schema that serializes an object of Flink types into a string bytes.
 *
 * <p>Serializes the input Flink object into a string and
 * converts it into <code>byte[]</code>.
 *
 * <p>Result <code>byte[]</code> messages can be deserialized using {@link StringRowDeserializationSchema}.
 */
public class StringRowSerializationSchema implements SerializationSchema<Row> {

	private final String encoding;

	public StringRowSerializationSchema(String encoding) {
		Preconditions.checkNotNull(encoding);
		this.encoding = encoding;
	}

	@Override
	public byte[] serialize(Row element) {
		byte[] bytes = null;
		if (element != null && element.getField(0) != null) {
			try {
				bytes = element.getField(0).toString().getBytes(encoding);
			} catch (Exception e) {
				throw new RuntimeException("Could not serialize row '" + element + "'. " +
					"Make sure that the schema matches the input.", e);
			}
		}
		return bytes;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		StringRowSerializationSchema that = (StringRowSerializationSchema) o;
		return Objects.equals(encoding, that.encoding);
	}

	@Override
	public int hashCode() {
		return Objects.hash(encoding);
	}
}
