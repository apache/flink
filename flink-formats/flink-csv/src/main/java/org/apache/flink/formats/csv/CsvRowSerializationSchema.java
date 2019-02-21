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
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ContainerNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvSchema;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Objects;

/**
 * Serialization schema that serializes an object of Flink types into a CSV bytes.
 *
 * <p>Serializes the input row into a {@link ObjectNode} and
 * converts it into <code>byte[]</code>.
 *
 * <p>Result <code>byte[]</code> messages can be deserialized using {@link CsvRowDeserializationSchema}.
 */
@PublicEvolving
public final class CsvRowSerializationSchema implements SerializationSchema<Row> {

	private static final long serialVersionUID = 2098447220136965L;

	/** Type information describing the input CSV data. */
	private final TypeInformation<Row> typeInfo;

	/** CsvMapper used to write {@link JsonNode} into bytes. */
	private final CsvMapper csvMapper;

	/** Schema describing the input CSV data. */
	private final CsvSchema csvSchema;

	/** Object writer used to write rows. It is configured by {@link CsvSchema}. */
	private final ObjectWriter objectWriter;

	/** Reusable object node. */
	private transient ObjectNode root;

	private CsvRowSerializationSchema(
			TypeInformation<Row> typeInfo,
			CsvSchema csvSchema) {
		this.typeInfo = typeInfo;
		this.csvMapper = new CsvMapper();
		this.csvSchema = csvSchema;
		this.objectWriter = csvMapper.writer(csvSchema);
	}

	/**
	 * A builder for creating a {@link CsvRowSerializationSchema}.
	 */
	@PublicEvolving
	public static class Builder {

		private final TypeInformation<Row> typeInfo;
		private CsvSchema csvSchema;

		/**
		 * Creates a {@link CsvRowSerializationSchema} expecting the given {@link TypeInformation}.
		 *
		 * @param typeInfo type information used to create schema.
		 */
		public Builder(TypeInformation<Row> typeInfo) {
			Preconditions.checkNotNull(typeInfo, "Type information must not be null.");
			this.typeInfo = typeInfo;

			if (!(typeInfo instanceof RowTypeInfo)) {
				throw new IllegalArgumentException("Row type information expected.");
			}

			this.csvSchema = CsvRowSchemaConverter.convert((RowTypeInfo) typeInfo);
		}

		public Builder setFieldDelimiter(char c) {
			this.csvSchema = this.csvSchema.rebuild().setColumnSeparator(c).build();
			return this;
		}

		public Builder setLineDelimiter(String delimiter) {
			Preconditions.checkNotNull(delimiter, "Delimiter must not be null.");
			if (!delimiter.equals("\n") && !delimiter.equals("\r") && !delimiter.equals("\r\n")) {
				throw new IllegalArgumentException(
					"Unsupported new line delimiter. Only \\n, \\r, or \\r\\n are supported.");
			}
			this.csvSchema = this.csvSchema.rebuild().setLineSeparator(delimiter).build();
			return this;
		}

		public Builder setArrayElementDelimiter(String delimiter) {
			Preconditions.checkNotNull(delimiter, "Delimiter must not be null.");
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

		public Builder setNullLiteral(String s) {
			this.csvSchema = this.csvSchema.rebuild().setNullValue(s).build();
			return this;
		}

		public CsvRowSerializationSchema build() {
			return new CsvRowSerializationSchema(
				typeInfo,
				csvSchema);
		}
	}

	@Override
	public byte[] serialize(Row row) {
		if (root == null) {
			root = csvMapper.createObjectNode();
		}
		try {
			convertNestedRow(root, row, (RowTypeInfo) typeInfo);
			return objectWriter.writeValueAsBytes(root);
		} catch (Throwable t) {
			throw new RuntimeException("Could not serialize row '" + row + "'.", t);
		}
	}

	@Override
	public boolean equals(Object o) {
		if (o == null || o.getClass() != this.getClass()) {
			return false;
		}
		if (this == o) {
			return true;
		}
		final CsvRowSerializationSchema that = (CsvRowSerializationSchema) o;
		final CsvSchema otherSchema = that.csvSchema;

		return typeInfo.equals(that.typeInfo) &&
			csvSchema.getColumnSeparator() == otherSchema.getColumnSeparator() &&
			Arrays.equals(csvSchema.getLineSeparator(), otherSchema.getLineSeparator()) &&
			csvSchema.getArrayElementSeparator().equals(otherSchema.getArrayElementSeparator()) &&
			csvSchema.getQuoteChar() == otherSchema.getQuoteChar() &&
			csvSchema.getEscapeChar() == otherSchema.getEscapeChar() &&
			Arrays.equals(csvSchema.getNullValue(), otherSchema.getNullValue());
	}

	@Override
	public int hashCode() {
		return Objects.hash(
			typeInfo,
			csvSchema.getColumnSeparator(),
			csvSchema.getLineSeparator(),
			csvSchema.getArrayElementSeparator(),
			csvSchema.getQuoteChar(),
			csvSchema.getEscapeChar(),
			csvSchema.getNullValue());
	}

	// --------------------------------------------------------------------------------------------

	private void convertNestedRow(ObjectNode reuse, Row row, RowTypeInfo rowTypeInfo) {
		final TypeInformation[] types = rowTypeInfo.getFieldTypes();
		if (row.getArity() != types.length) {
			throw new RuntimeException("Row length mismatch. " + types.length +
				" fields expected but was " + row.getArity() + ".");
		}

		final String[] fields = rowTypeInfo.getFieldNames();
		for (int i = 0; i < types.length; i++) {
			final String columnName = fields[i];
			final Object obj = row.getField(i);
			reuse.set(columnName, convert(reuse, obj, types[i]));
		}
	}

	/**
	 * Converts an object to a JsonNode.
	 *
	 * @param container {@link ContainerNode} that creates {@link JsonNode}.
	 * @param obj Object to be converted to {@link JsonNode}.
	 * @param info Type information that decides the type of {@link JsonNode}.
	 * @return result after converting.
	 */
	private JsonNode convert(ContainerNode<?> container, Object obj, TypeInformation<?> info) {
		if (info == Types.VOID || obj == null) {
			return container.nullNode();
		} else if (info == Types.STRING) {
			return container.textNode((String) obj);
		} else if (info == Types.BOOLEAN) {
			return container.booleanNode((Boolean) obj);
		} else if (info == Types.BYTE) {
			return container.numberNode((Byte) obj);
		} else if (info == Types.SHORT) {
			return container.numberNode((Short) obj);
		} else if (info == Types.INT) {
			return container.numberNode((Integer) obj);
		} else if (info == Types.LONG) {
			return container.numberNode((Long) obj);
		} else if (info == Types.FLOAT) {
			return container.numberNode((Float) obj);
		} else if (info == Types.DOUBLE) {
			return container.numberNode((Double) obj);
		} else if (info == Types.BIG_DEC) {
			return container.numberNode((BigDecimal) obj);
		} else if (info == Types.BIG_INT) {
			return container.numberNode((BigInteger) obj);
		} else if (info == Types.SQL_DATE) {
			return container.textNode(obj.toString());
		} else if (info == Types.SQL_TIME) {
			return container.textNode(obj.toString());
		} else if (info == Types.SQL_TIMESTAMP) {
			return container.textNode(obj.toString());
		} else if (info instanceof RowTypeInfo){
			return convertNestedRow((Row) obj, (RowTypeInfo) info);
		} else if (info instanceof BasicArrayTypeInfo) {
			return convertObjectArray((Object[]) obj, ((BasicArrayTypeInfo) info).getComponentInfo());
		} else if (info instanceof ObjectArrayTypeInfo) {
			return convertObjectArray((Object[]) obj, ((ObjectArrayTypeInfo) info).getComponentInfo());
		} else if (info instanceof PrimitiveArrayTypeInfo &&
				((PrimitiveArrayTypeInfo) info).getComponentType() == Types.BYTE) {
			return container.binaryNode((byte[]) obj);
		} else {
			throw new RuntimeException("Unsupported type information '" + info + "' for object: " + obj);
		}
	}

	private ArrayNode convertNestedRow(Row row, RowTypeInfo rowTypeInfo) {
		final ArrayNode arrayNode = csvMapper.createArrayNode();
		final TypeInformation[] types = rowTypeInfo.getFieldTypes();
		if (row.getArity() != types.length) {
			throw new RuntimeException("Row length mismatch. " + types.length +
				" fields expected but was " + row.getArity() + ".");
		}
		for (int i = 0; i < types.length; i++) {
			arrayNode.add(convert(arrayNode, row.getField(i), types[i]));
		}
		return arrayNode;
	}

	private ArrayNode convertObjectArray(Object[] array, TypeInformation<?> elementInfo) {
		final ArrayNode arrayNode = csvMapper.createArrayNode();
		for (Object element : array) {
			arrayNode.add(convert(arrayNode, element, elementInfo));
		}
		return arrayNode;
	}
}
