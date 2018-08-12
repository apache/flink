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

import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

/**
 * Deserialization schema from CSV to Flink types.
 *
 * <p>Deserializes a <code>byte[]</code> message as a {@link JsonNode} and
 * convert it to {@link Row}.
 *
 * <p>Failure during deserialization are forwarded as wrapped IOExceptions.
 */
@Public
public final class CsvRowDeserializationSchema implements DeserializationSchema<Row> {

	/** Schema describing the input csv data. */
	private CsvSchema csvSchema;

	/** Type information describing the input csv data. */
	private TypeInformation<Row> rowTypeInfo;

	/** ObjectReader used to read message, it will be changed when csvSchema is changed. */
	private ObjectReader objectReader;

	/** Charset for byte[]. */
	private String charset = "UTF-8";

	/**
	 * Create a csv row DeserializationSchema with given {@link TypeInformation}.
	 */
	public CsvRowDeserializationSchema(TypeInformation<Row> rowTypeInfo) {
		Preconditions.checkNotNull(rowTypeInfo, "rowTypeInfo must not be null !");
		CsvMapper csvMapper = new CsvMapper();
		this.rowTypeInfo = rowTypeInfo;
		this.csvSchema = CsvRowSchemaConverter.rowTypeToCsvSchema((RowTypeInfo) rowTypeInfo);
		this.objectReader = csvMapper.readerFor(JsonNode.class).with(csvSchema);
		this.setNullValue("null");
	}

	@Override
	public Row deserialize(byte[] message) throws IOException {
		JsonNode root = objectReader.readValue(message);
		return convertRow(root, (RowTypeInfo) rowTypeInfo);
	}

	@Override
	public boolean isEndOfStream(Row nextElement) {
		return false;
	}

	@Override
	public TypeInformation<Row> getProducedType() {
		return rowTypeInfo;
	}

	private Row convertRow(JsonNode root, RowTypeInfo rowTypeInfo) {
		String[] fields = rowTypeInfo.getFieldNames();
		TypeInformation<?>[] types = rowTypeInfo.getFieldTypes();
		Row row = new Row(fields.length);

		for (int i = 0; i < fields.length; i++) {
			String columnName = fields[i];
			JsonNode node = root.get(columnName);
			row.setField(i, convert(node, types[i]));
		}
		return row;
	}

	private Row convertRow(ArrayNode node, RowTypeInfo rowTypeInfo) {
		TypeInformation[] types = rowTypeInfo.getFieldTypes();
		String[] fields = rowTypeInfo.getFieldNames();
		Row row = new Row(fields.length);
		for (int i = 0; i < fields.length; i++) {
			row.setField(i, convert(node.get(i), types[i]));
		}
		return row;
	}

	/**
	 * Converts json node to object with given type information.
	 * @param node json node to be converted.
	 * @param info type information for the json data.
	 * @return converted object
	 */
	private Object convert(JsonNode node, TypeInformation<?> info) {
		if (node instanceof NullNode) {
			return null;
		}
		if (info == Types.STRING) {
			return node.asText();
		} else if (info == Types.LONG) {
			return node.asLong();
		} else if (info == Types.INT) {
			return node.asInt();
		} else if (info == Types.DOUBLE) {
			return node.asDouble();
		} else if (info == Types.FLOAT) {
			return Double.valueOf(node.asDouble()).floatValue();
		} else if (info == Types.BIG_DEC) {
			return BigDecimal.valueOf(node.asDouble());
		} else if (info == Types.BIG_INT) {
			return BigInteger.valueOf(node.asLong());
		} else if (info == Types.SQL_DATE) {
			return Date.valueOf(node.asText());
		} else if (info == Types.SQL_TIME) {
			return Time.valueOf(node.asText());
		} else if (info == Types.SQL_TIMESTAMP) {
			return Timestamp.valueOf(node.asText());
		} else if (info == Types.BOOLEAN) {
			return node.asBoolean();
		} else if (info instanceof RowTypeInfo) {
			return convertRow((ArrayNode) node, (RowTypeInfo) info);
		} else if (info instanceof BasicArrayTypeInfo) {
			return convertArray((ArrayNode) node, ((BasicArrayTypeInfo) info).getComponentInfo());
		} else if (info instanceof PrimitiveArrayTypeInfo &&
			((PrimitiveArrayTypeInfo) info).getComponentType() == Types.BYTE) {
			return convertByteArray((TextNode) node);
		} else {
			throw new RuntimeException("Unable to support type " + info.toString() + " yet");
		}
	}

	private Object[] convertArray(ArrayNode node, TypeInformation<?> elementType) {
		final Object[] array = (Object[]) Array.newInstance(elementType.getTypeClass(), node.size());
		for (int i = 0; i < node.size(); i++) {
			array[i] = convert(node.get(i), elementType);
		}
		return array;
	}

	private byte[] convertByteArray(TextNode node) {
		try {
			return node.asText().getBytes(charset);
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException("Unsupport encoding charset" + charset, e);
		}
	}

	public void setCharset(String charset) {
		this.charset = charset;
	}

	public void setFieldDelimiter(String s) {
		if (s.length() != 1) {
			throw new RuntimeException("FieldDelimiter's length must be one !");
		}
		this.csvSchema = this.csvSchema.rebuild().setColumnSeparator(s.charAt(0)).build();
	}

	public void setArrayElementDelimiter(String s) {
		this.csvSchema = this.csvSchema.rebuild().setArrayElementSeparator(s).build();
		this.objectReader = objectReader.with(csvSchema);
	}

	public void setQuoteCharacter(char c) {
		this.csvSchema = this.csvSchema.rebuild().setQuoteChar(c).build();
		this.objectReader = objectReader.with(csvSchema);
	}

	public void setEscapeCharacter(char c) {
		this.csvSchema = this.csvSchema.rebuild().setEscapeChar(c).build();
		this.objectReader = objectReader.with(csvSchema);
	}

	public void setNullValue(String s) {
		this.csvSchema = this.csvSchema.rebuild().setNullValue(s).build();
		this.objectReader = objectReader.with(csvSchema);
	}

	@Override
	public boolean equals(Object o) {
		if (o == null || o.getClass() != this.getClass()) {
			return false;
		}
		if (this == o) {
			return true;
		}
		final CsvRowDeserializationSchema that = (CsvRowDeserializationSchema) o;
		return rowTypeInfo.equals(that.rowTypeInfo) &&
			csvSchema.toString().equals(that.csvSchema.toString());
	}
}
