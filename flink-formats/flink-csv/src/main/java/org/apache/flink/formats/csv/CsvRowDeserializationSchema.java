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

package org.apache.flink.formats.csv;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectReader;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.TextNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvSchema;

import java.io.IOException;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Objects;

/**
 * Deserialization schema from CSV to Flink types.
 *
 * <p>Deserializes a <code>byte[]</code> message as a {@link JsonNode} and
 * converts it to {@link Row}.
 *
 * <p>Failure during deserialization are forwarded as wrapped {@link IOException}s.
 */
@PublicEvolving
public final class CsvRowDeserializationSchema implements DeserializationSchema<Row> {

	private static final long serialVersionUID = 2135553495874539201L;

	/** Type information describing the result type. */
	private final TypeInformation<Row> typeInfo;

	/** Schema describing the input CSV data. */
	private final CsvSchema csvSchema;

	/** Object reader used to read rows. It is configured by {@link CsvSchema}. */
	private final ObjectReader objectReader;

	/** Flag indicating whether to ignore invalid fields/rows (default: throw an exception). */
	private final boolean ignoreParseErrors;

	private CsvRowDeserializationSchema(
			TypeInformation<Row> typeInfo,
			CsvSchema csvSchema,
			boolean ignoreParseErrors) {
		this.typeInfo = typeInfo;
		this.csvSchema = csvSchema;
		this.objectReader = new CsvMapper().readerFor(JsonNode.class).with(csvSchema);
		this.ignoreParseErrors = ignoreParseErrors;
	}

	/**
	 * A builder for creating a {@link CsvRowDeserializationSchema}.
	 */
	@PublicEvolving
	public static class Builder {

		private final TypeInformation<Row> typeInfo;
		private CsvSchema csvSchema;
		private boolean ignoreParseErrors;

		/**
		 * Creates a CSV deserialization schema for the given {@link TypeInformation} with
		 * optional parameters.
		 */
		public Builder(TypeInformation<Row> typeInfo) {
			Preconditions.checkNotNull(typeInfo, "Type information must not be null.");

			if (!(typeInfo instanceof RowTypeInfo)) {
				throw new IllegalArgumentException("Row type information expected.");
			}

			this.typeInfo = typeInfo;
			this.csvSchema = CsvRowSchemaConverter.convert((RowTypeInfo) typeInfo);
		}

		public Builder setFieldDelimiter(char delimiter) {
			this.csvSchema = this.csvSchema.rebuild().setColumnSeparator(delimiter).build();
			return this;
		}

		public Builder setAllowComments(boolean allowComments) {
			this.csvSchema = this.csvSchema.rebuild().setAllowComments(allowComments).build();
			return this;
		}

		public Builder setArrayElementDelimiter(String delimiter) {
			Preconditions.checkNotNull(delimiter, "Array element delimiter must not be null.");
			this.csvSchema = this.csvSchema.rebuild().setArrayElementSeparator(delimiter).build();
			return this;
		}

		public Builder setQuoteCharacter(char c) {
			this.csvSchema = this.csvSchema.rebuild().setQuoteChar(c).build();
			return this;
		}

		public Builder setEscapeCharacter(char c) {
			this.csvSchema = this.csvSchema.rebuild().setEscapeChar(c).build();
			return this;
		}

		public Builder setNullLiteral(String nullLiteral) {
			Preconditions.checkNotNull(nullLiteral, "Null literal must not be null.");
			this.csvSchema = this.csvSchema.rebuild().setNullValue(nullLiteral).build();
			return this;
		}

		public Builder setIgnoreParseErrors(boolean ignoreParseErrors) {
			this.ignoreParseErrors = ignoreParseErrors;
			return this;
		}

		public CsvRowDeserializationSchema build() {
			return new CsvRowDeserializationSchema(
				typeInfo,
				csvSchema,
				ignoreParseErrors);
		}
	}

	@Override
	public Row deserialize(byte[] message) throws IOException {
		try {
			final JsonNode root = objectReader.readValue(message);
			return convertRow(root, (RowTypeInfo) typeInfo, true);
		} catch (Throwable t) {
			if (ignoreParseErrors) {
				return null;
			}
			throw new IOException("Failed to deserialize CSV row '" + new String(message) + "'.", t);
		}
	}

	@Override
	public boolean isEndOfStream(Row nextElement) {
		return false;
	}

	@Override
	public TypeInformation<Row> getProducedType() {
		return typeInfo;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || o.getClass() != this.getClass()) {
			return false;
		}
		final CsvRowDeserializationSchema that = (CsvRowDeserializationSchema) o;
		final CsvSchema otherSchema = that.csvSchema;

		return typeInfo.equals(that.typeInfo) &&
			ignoreParseErrors == that.ignoreParseErrors &&
			csvSchema.getColumnSeparator() == otherSchema.getColumnSeparator() &&
			csvSchema.allowsComments() == otherSchema.allowsComments() &&
			csvSchema.getArrayElementSeparator().equals(otherSchema.getArrayElementSeparator()) &&
			csvSchema.getQuoteChar() == otherSchema.getQuoteChar() &&
			csvSchema.getEscapeChar() == otherSchema.getEscapeChar() &&
			Arrays.equals(csvSchema.getNullValue(), otherSchema.getNullValue());
	}

	@Override
	public int hashCode() {
		return Objects.hash(
			typeInfo,
			ignoreParseErrors,
			csvSchema.getColumnSeparator(),
			csvSchema.allowsComments(),
			csvSchema.getArrayElementSeparator(),
			csvSchema.getQuoteChar(),
			csvSchema.getEscapeChar(),
			csvSchema.getNullValue());
	}

	// --------------------------------------------------------------------------------------------

	private Object convert(JsonNode node, TypeInformation<?> info) {
		if (info == Types.VOID || node.isNull()) {
			return null;
		} else if (info == Types.STRING) {
			return node.asText();
		} else if (info == Types.BOOLEAN) {
			return Boolean.valueOf(node.asText().trim());
		} else if (info == Types.BYTE) {
			return Byte.valueOf(node.asText().trim());
		} else if (info == Types.SHORT) {
			return Short.valueOf(node.asText().trim());
		} else if (info == Types.INT) {
			return Integer.valueOf(node.asText().trim());
		} else if (info == Types.LONG) {
			return Long.valueOf(node.asText().trim());
		} else if (info == Types.FLOAT) {
			return Float.valueOf(node.asText().trim());
		} else if (info == Types.DOUBLE) {
			return Double.valueOf(node.asText().trim());
		} else if (info == Types.BIG_DEC) {
			return new BigDecimal(node.asText().trim());
		} else if (info == Types.BIG_INT) {
			return new BigInteger(node.asText().trim());
		} else if (info == Types.SQL_DATE) {
			return Date.valueOf(node.asText());
		} else if (info == Types.SQL_TIME) {
			return Time.valueOf(node.asText());
		} else if (info == Types.SQL_TIMESTAMP) {
			return Timestamp.valueOf(node.asText());
		} else if (info instanceof RowTypeInfo) {
			return convertRow(node, (RowTypeInfo) info, false);
		} else if (info instanceof BasicArrayTypeInfo) {
			return convertObjectArray(node, ((BasicArrayTypeInfo) info).getComponentInfo());
		} else if (info instanceof ObjectArrayTypeInfo) {
			return convertObjectArray(node, ((ObjectArrayTypeInfo) info).getComponentInfo());
		} else if (info instanceof PrimitiveArrayTypeInfo &&
				((PrimitiveArrayTypeInfo) info).getComponentType() == Types.BYTE) {
			return convertByteArray((TextNode) node);
		} else {
			throw new RuntimeException("Unsupported type information '" + info + "' for node: " + node);
		}
	}

	private Row convertRow(JsonNode node, RowTypeInfo info, boolean mapByName) {
		final String[] fields = info.getFieldNames();
		final TypeInformation<?>[] types = info.getFieldTypes();
		final int nodeSize = node.size();
		if (types.length != nodeSize && !ignoreParseErrors) {
			throw new RuntimeException("Row length mismatch. " + types.length +
				" fields expected but was " + nodeSize + ".");
		}
		final Row row = new Row(types.length);
		final int len = Math.min(types.length, nodeSize);
		for (int i = 0; i < len; i++) {
			Object value = null;
			try {
				if (mapByName) {
					value = convert(node.get(fields[i]), types[i]);
				} else {
					value = convert(node.get(i), types[i]);
				}
			} catch (Throwable t) {
				if (!ignoreParseErrors) {
					throw t;
				}
			}
			row.setField(i, value);
		}
		return row;
	}

	private Object[] convertObjectArray(JsonNode node, TypeInformation<?> elementType) {
		final int nodeSize = node.size();
		final Object[] array = (Object[]) Array.newInstance(elementType.getTypeClass(), nodeSize);
		for (int i = 0; i < nodeSize; i++) {
			array[i] = convert(node.get(i), elementType);
		}
		return array;
	}

	private byte[] convertByteArray(TextNode node) {
		try {
			return node.binaryValue();
		} catch (IOException e) {
			throw new RuntimeException("Unable to deserialize byte array.", e);
		}
	}
}
