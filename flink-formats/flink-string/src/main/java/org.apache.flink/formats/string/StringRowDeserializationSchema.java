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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.Objects;

/**
 * Deserialization schema from String to Flink types.
 *
 * <p>Deserializes a <code>byte[]</code> message as a String object and reads
 * the specified fields.
 *
 * <p>Failure during deserialization are forwarded as wrapped IOExceptions.
 */
@PublicEvolving
public class StringRowDeserializationSchema implements DeserializationSchema<Row> {

	private static final long serialVersionUID = -7539382334470617511L;

	/** Type information describing the result type. */
	private final TypeInformation<Row> typeInfo;

	/** encodings of bytes. **/
	private final String encoding;

	/** Flag indicating whether to fail on null. */
	private boolean failOnNull;

	/** Flag indicating whether to fail on empty. */
	private boolean failOnEmpty;

	public StringRowDeserializationSchema(String schema, String encoding) {
		Preconditions.checkNotNull(schema);
		Preconditions.checkNotNull(encoding);
		this.typeInfo = new RowTypeInfo(
			new TypeInformation<?>[] {Types.STRING},
			new String[] {schema}
		);
		this.encoding = encoding;
	}

	public StringRowDeserializationSchema setFailOnNull(boolean failOnNull) {
		this.failOnNull = failOnNull;
		return this;
	}

	public StringRowDeserializationSchema setFailOnEmpty(boolean failOnEmpty) {
		this.failOnEmpty = failOnEmpty;
		return this;
	}

	@Override
	public Row deserialize(byte[] message) throws IOException {
		String result = null;
		if (message == null) {
			if (failOnNull) {
				throw new IOException("message can't be null");
			}
		} else if (message.length == 0) {
			if (failOnEmpty) {
				throw new IOException("message can't be empty");
			}
			result = "";
		} else {
			try {
				result = new String(message, encoding);
			} catch (Exception e) {
				throw new IOException("Failed to deserialize string object.", e);
			}
		}
		return Row.of(result);
	}

	@Override
	public boolean isEndOfStream(Row nextElement) {
		return false;
	}

	@Override
	public TypeInformation<Row> getProducedType() {
		return this.typeInfo;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		final StringRowDeserializationSchema that = (StringRowDeserializationSchema) o;
		return failOnNull == that.failOnNull && failOnEmpty == that.failOnEmpty &&
			encoding.equals(that.encoding) && Objects.equals(typeInfo, that.typeInfo);
	}

	@Override
	public int hashCode() {
		return Objects.hash(typeInfo, encoding, failOnEmpty, failOnNull);
	}
}
