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
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ContainerNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

/**
 * Serialization schema that serializes an object of Flink types into a CSV bytes.
 *
 * <p>Serializes the input row into a {@link ObjectNode} and
 * converts it into <code>byte[]</code>.
 *
 * <p>Result <code>byte[]</code> messages can be deserialized using {@link CsvRowDeserializationSchema}.
 */
@PublicEvolving
public class CsvRowSerializationSchema implements SerializationSchema<Row> {

	/** Schema describing the input csv data. */
	private CsvSchema csvSchema;

	/** Type information describing the input csv data. */
	private TypeInformation<Row> rowTypeInfo;

	/** CsvMapper used to write {@link JsonNode} into bytes. */
	private CsvMapper csvMapper = new CsvMapper();

	/** Reusable object node. */
	private ObjectNode root;

	/** Charset for byte[]. */
	private String charset = "UTF-8";

	/**
	 * Create a {@link CsvRowSerializationSchema} with given {@link TypeInformation}.
	 * @param rowTypeInfo type information used to create schem.
	 */
	CsvRowSerializationSchema(TypeInformation<Row> rowTypeInfo) {
		Preconditions.checkNotNull(rowTypeInfo, "rowTypeInfo must not be null !");
		this.rowTypeInfo = rowTypeInfo;
		this.csvSchema = CsvRowSchemaConverter.rowTypeToCsvSchema((RowTypeInfo) rowTypeInfo);
	}

	@Override
	public byte[] serialize(Row row) {
		if (root == null) {
			root = csvMapper.createObjectNode();
		}
		try {
			convertRow(root, row, (RowTypeInfo) rowTypeInfo);
			return csvMapper.writer(csvSchema).writeValueAsBytes(root);
		} catch (JsonProcessingException e) {
			throw new RuntimeException("Could not serialize row '" + row + "'. " +
				"Make sure that the schema matches the input.", e);
		}
	}

	private void convertRow(ObjectNode reuse, Row row, RowTypeInfo rowTypeInfo) {
		if (reuse == null) {
			reuse = csvMapper.createObjectNode();
		}
		if (row.getArity() != rowTypeInfo.getFieldNames().length) {
			throw new IllegalStateException(String.format(
				"Number of elements in the row '%s' is different from number of field names: %d",
				row, rowTypeInfo.getFieldNames().length));
		}
		TypeInformation[] types = rowTypeInfo.getFieldTypes();
		String[] fields = rowTypeInfo.getFieldNames();
		for (int i = 0; i < types.length; i++) {
			String columnName = fields[i];
			Object obj = row.getField(i);
			reuse.set(columnName, convert(reuse, obj, types[i], false));
		}
	}

	/**
	 * Converts an object to a JsonNode.
	 * @param container {@link ContainerNode} that creates {@link JsonNode}.
	 * @param obj Object that used to {@link JsonNode}.
	 * @param info Type infomation that decides the type of {@link JsonNode}.
	 * @param nested variable that indicates whether the obj is in a nested structure
	 *               like a string in an array.
	 * @return result after converting.
	 */
	private JsonNode convert(ContainerNode<?> container, Object obj, TypeInformation info, Boolean nested) {
		if (info == Types.STRING) {
			return container.textNode((String) obj);
		} else if (info == Types.LONG) {
			return container.numberNode((Long) obj);
		} else if (info == Types.INT) {
			return container.numberNode((Integer) obj);
		} else if (info == Types.DOUBLE) {
			return container.numberNode((Double) obj);
		} else if (info == Types.FLOAT) {
			return container.numberNode((Float) obj);
		} else if (info == Types.BIG_DEC) {
			return container.numberNode(BigDecimal.valueOf(Double.valueOf(String.valueOf(obj))));
		} else if (info == Types.BIG_INT) {
			return container.numberNode(BigInteger.valueOf(Long.valueOf(String.valueOf(obj))));
		} else if (info == Types.SQL_DATE) {
			return container.textNode(Date.valueOf(String.valueOf(obj)).toString());
		} else if (info == Types.SQL_TIME) {
			return container.textNode(Time.valueOf(String.valueOf(obj)).toString());
		} else if (info == Types.SQL_TIMESTAMP) {
			return container.textNode(Timestamp.valueOf(String.valueOf(obj)).toString());
		} else if (info == Types.BOOLEAN) {
			return container.booleanNode((Boolean) obj);
		} else if (info instanceof RowTypeInfo){
			if (nested) {
				throw new RuntimeException("Unable to support nested row type " + info.toString() + " yet");
			}
			return convertArray((Row) obj, (RowTypeInfo) info);
		} else if (info instanceof BasicArrayTypeInfo) {
			if (nested) {
				throw new RuntimeException("Unable to support nested array type " + info.toString() + " yet");
			}
			return convertArray((Object[]) obj,
				((BasicArrayTypeInfo) info).getComponentInfo());
		} else if (info instanceof PrimitiveArrayTypeInfo &&
			((PrimitiveArrayTypeInfo) info).getComponentType() == Types.BYTE) {
			/* We converts byte[] to TextNode instead of BinaryNode here,
			  because the instance of BinaryNode will be serialized to base64 string in
			  {@link com.fasterxml.jackson.databind.node.BinaryNode#serialize(JsonGenerator, SerializerProvider)},
			  which is unacceptable for users.
			 */
			try {
				return container.textNode(new String((byte[]) obj, charset));
			} catch (UnsupportedEncodingException e) {
				throw new RuntimeException("Unsupport encoding charset " + charset, e);
			}
		} else {
			throw new RuntimeException("Unable to support type " + info.toString() + " yet");
		}
	}

	/**
	 * Use {@link ArrayNode} to represents a row.
	 */
	private ArrayNode convertArray(Row row, RowTypeInfo rowTypeInfo) {
		ArrayNode arrayNode = csvMapper.createArrayNode();
		TypeInformation[] types = rowTypeInfo.getFieldTypes();
		String[] fields = rowTypeInfo.getFieldNames();
		for (int i = 0; i < fields.length; i++) {
			arrayNode.add(convert(arrayNode, row.getField(i), types[i], true));
		}
		return arrayNode;
	}

	/**
	 * Use {@link ArrayNode} to represents an array.
	 */
	private ArrayNode convertArray(Object[] obj, TypeInformation elementInfo) {
		ArrayNode arrayNode = csvMapper.createArrayNode();
		for (Object elementObj : obj) {
			arrayNode.add(convert(arrayNode, elementObj, elementInfo, true));
		}
		return arrayNode;
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
	}

	public void setQuoteCharacter(char c) {
		this.csvSchema = this.csvSchema.rebuild().setQuoteChar(c).build();
	}

	public void setEscapeCharacter(char c) {
		this.csvSchema = this.csvSchema.rebuild().setEscapeChar(c).build();
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

		return rowTypeInfo.equals(that.rowTypeInfo) &&
			csvSchema.toString().equals(that.csvSchema.toString());
	}
}
