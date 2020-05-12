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

package org.apache.flink.orc;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ListColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.MapColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.StructColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.orc.TypeDescription;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.function.DoubleFunction;
import java.util.function.Function;
import java.util.function.LongFunction;

/**
 * A class that provides utility methods for orc file reading.
 */
class OrcBatchReader {

	private static final long MILLIS_PER_DAY = 86400000; // = 24 * 60 * 60 * 1000
	private static final TimeZone LOCAL_TZ = TimeZone.getDefault();

	/**
	 * Converts an ORC schema to a Flink TypeInformation.
	 *
	 * @param schema The ORC schema.
	 * @return The TypeInformation that corresponds to the ORC schema.
	 */
	static TypeInformation schemaToTypeInfo(TypeDescription schema) {
		switch (schema.getCategory()) {
			case BOOLEAN:
				return BasicTypeInfo.BOOLEAN_TYPE_INFO;
			case BYTE:
				return BasicTypeInfo.BYTE_TYPE_INFO;
			case SHORT:
				return BasicTypeInfo.SHORT_TYPE_INFO;
			case INT:
				return BasicTypeInfo.INT_TYPE_INFO;
			case LONG:
				return BasicTypeInfo.LONG_TYPE_INFO;
			case FLOAT:
				return BasicTypeInfo.FLOAT_TYPE_INFO;
			case DOUBLE:
				return BasicTypeInfo.DOUBLE_TYPE_INFO;
			case DECIMAL:
				return BasicTypeInfo.BIG_DEC_TYPE_INFO;
			case STRING:
			case CHAR:
			case VARCHAR:
				return BasicTypeInfo.STRING_TYPE_INFO;
			case DATE:
				return SqlTimeTypeInfo.DATE;
			case TIMESTAMP:
				return SqlTimeTypeInfo.TIMESTAMP;
			case BINARY:
				return PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO;
			case STRUCT:
				List<TypeDescription> fieldSchemas = schema.getChildren();
				TypeInformation[] fieldTypes = new TypeInformation[fieldSchemas.size()];
				for (int i = 0; i < fieldSchemas.size(); i++) {
					fieldTypes[i] = schemaToTypeInfo(fieldSchemas.get(i));
				}
				String[] fieldNames = schema.getFieldNames().toArray(new String[]{});
				return new RowTypeInfo(fieldTypes, fieldNames);
			case LIST:
				TypeDescription elementSchema = schema.getChildren().get(0);
				TypeInformation<?> elementType = schemaToTypeInfo(elementSchema);
				// arrays of primitive types are handled as object arrays to support null values
				return ObjectArrayTypeInfo.getInfoFor(elementType);
			case MAP:
				TypeDescription keySchema = schema.getChildren().get(0);
				TypeDescription valSchema = schema.getChildren().get(1);
				TypeInformation<?> keyType = schemaToTypeInfo(keySchema);
				TypeInformation<?> valType = schemaToTypeInfo(valSchema);
				return new MapTypeInfo<>(keyType, valType);
			case UNION:
				throw new UnsupportedOperationException("UNION type is not supported yet.");
			default:
				throw new IllegalArgumentException("Unknown type " + schema);
		}
	}

	/**
	 * Fills an ORC batch into an array of Row.
	 *
	 * @param rows The batch of rows need to be filled.
	 * @param schema The schema of the ORC data.
	 * @param batch The ORC data.
	 * @param selectedFields The list of selected ORC fields.
	 * @return The number of rows that were filled.
	 */
	static int fillRows(Row[] rows, TypeDescription schema, VectorizedRowBatch batch, int[] selectedFields) {

		int rowsToRead = Math.min((int) batch.count(), rows.length);

		List<TypeDescription> fieldTypes = schema.getChildren();
		// read each selected field
		for (int fieldIdx = 0; fieldIdx < selectedFields.length; fieldIdx++) {
			int orcIdx = selectedFields[fieldIdx];
			readField(rows, fieldIdx, fieldTypes.get(orcIdx), batch.cols[orcIdx], rowsToRead);
		}
		return rowsToRead;
	}

	/**
	 * Reads a vector of data into an array of objects.
	 *
	 * @param vals The array that needs to be filled.
	 * @param fieldIdx If the vals array is an array of Row, the index of the field that needs to be filled.
	 *                 Otherwise a -1 must be passed and the data is directly filled into the array.
	 * @param schema The schema of the vector to read.
	 * @param vector The vector to read.
	 * @param childCount The number of vector entries to read.
	 */
	private static void readField(Object[] vals, int fieldIdx, TypeDescription schema, ColumnVector vector, int childCount) {

		// check the type of the vector to decide how to read it.
		switch (schema.getCategory()) {
			case BOOLEAN:
				if (vector.noNulls) {
					readNonNullLongColumn(vals, fieldIdx, (LongColumnVector) vector, childCount, OrcBatchReader::readBoolean);
				} else {
					readLongColumn(vals, fieldIdx, (LongColumnVector) vector, childCount, OrcBatchReader::readBoolean);
				}
				break;
			case BYTE:
				if (vector.noNulls) {
					readNonNullLongColumn(vals, fieldIdx, (LongColumnVector) vector, childCount, OrcBatchReader::readByte);
				} else {
					readLongColumn(vals, fieldIdx, (LongColumnVector) vector, childCount, OrcBatchReader::readByte);
				}
				break;
			case SHORT:
				if (vector.noNulls) {
					readNonNullLongColumn(vals, fieldIdx, (LongColumnVector) vector, childCount, OrcBatchReader::readShort);
				} else {
					readLongColumn(vals, fieldIdx, (LongColumnVector) vector, childCount, OrcBatchReader::readShort);
				}
				break;
			case INT:
				if (vector.noNulls) {
					readNonNullLongColumn(vals, fieldIdx, (LongColumnVector) vector, childCount, OrcBatchReader::readInt);
				} else {
					readLongColumn(vals, fieldIdx, (LongColumnVector) vector, childCount, OrcBatchReader::readInt);
				}
				break;
			case LONG:
				if (vector.noNulls) {
					readNonNullLongColumn(vals, fieldIdx, (LongColumnVector) vector, childCount, OrcBatchReader::readLong);
				} else {
					readLongColumn(vals, fieldIdx, (LongColumnVector) vector, childCount, OrcBatchReader::readLong);
				}
				break;
			case FLOAT:
				if (vector.noNulls) {
					readNonNullDoubleColumn(vals, fieldIdx, (DoubleColumnVector) vector, childCount, OrcBatchReader::readFloat);
				} else {
					readDoubleColumn(vals, fieldIdx, (DoubleColumnVector) vector, childCount, OrcBatchReader::readFloat);
				}
				break;
			case DOUBLE:
				if (vector.noNulls) {
					readNonNullDoubleColumn(vals, fieldIdx, (DoubleColumnVector) vector, childCount, OrcBatchReader::readDouble);
				} else {
					readDoubleColumn(vals, fieldIdx, (DoubleColumnVector) vector, childCount, OrcBatchReader::readDouble);
				}
				break;
			case CHAR:
			case VARCHAR:
			case STRING:
				if (vector.noNulls) {
					readNonNullBytesColumnAsString(vals, fieldIdx, (BytesColumnVector) vector, childCount);
				} else {
					readBytesColumnAsString(vals, fieldIdx, (BytesColumnVector) vector, childCount);
				}
				break;
			case DATE:
				if (vector.noNulls) {
					readNonNullLongColumnAsDate(vals, fieldIdx, (LongColumnVector) vector, childCount);
				} else {
					readLongColumnAsDate(vals, fieldIdx, (LongColumnVector) vector, childCount);
				}
				break;
			case TIMESTAMP:
				if (vector.noNulls) {
					readNonNullTimestampColumn(vals, fieldIdx, (TimestampColumnVector) vector, childCount);
				} else {
					readTimestampColumn(vals, fieldIdx, (TimestampColumnVector) vector, childCount);
				}
				break;
			case BINARY:
				if (vector.noNulls) {
					readNonNullBytesColumnAsBinary(vals, fieldIdx, (BytesColumnVector) vector, childCount);
				} else {
					readBytesColumnAsBinary(vals, fieldIdx, (BytesColumnVector) vector, childCount);
				}
				break;
			case DECIMAL:
				if (vector.noNulls) {
					readNonNullDecimalColumn(vals, fieldIdx, (DecimalColumnVector) vector, childCount);
				} else {
					readDecimalColumn(vals, fieldIdx, (DecimalColumnVector) vector, childCount);
				}
				break;
			case STRUCT:
				if (vector.noNulls) {
					readNonNullStructColumn(vals, fieldIdx, (StructColumnVector) vector, schema, childCount);
				} else {
					readStructColumn(vals, fieldIdx, (StructColumnVector) vector, schema, childCount);
				}
				break;
			case LIST:
				if (vector.noNulls) {
					readNonNullListColumn(vals, fieldIdx, (ListColumnVector) vector, schema, childCount);
				} else {
					readListColumn(vals, fieldIdx, (ListColumnVector) vector, schema, childCount);
				}
				break;
			case MAP:
				if (vector.noNulls) {
					readNonNullMapColumn(vals, fieldIdx, (MapColumnVector) vector, schema, childCount);
				} else {
					readMapColumn(vals, fieldIdx, (MapColumnVector) vector, schema, childCount);
				}
				break;
			case UNION:
				throw new UnsupportedOperationException("UNION type not supported yet");
			default:
				throw new IllegalArgumentException("Unknown type " + schema);
		}
	}

	private static <T> void readNonNullLongColumn(Object[] vals, int fieldIdx, LongColumnVector vector,
													int childCount, LongFunction<T> reader) {

		if (vector.isRepeating) { // fill complete column with first value
			T repeatingValue = reader.apply(vector.vector[0]);
			fillColumnWithRepeatingValue(vals, fieldIdx, repeatingValue, childCount);
		} else {
			if (fieldIdx == -1) { // set as an object
				for (int i = 0; i < childCount; i++) {
					vals[i] = reader.apply(vector.vector[i]);
				}
			} else { // set as a field of Row
				Row[] rows = (Row[]) vals;
				for (int i = 0; i < childCount; i++) {
					rows[i].setField(fieldIdx, reader.apply(vector.vector[i]));
				}
			}
		}
	}

	private static <T> void readNonNullDoubleColumn(Object[] vals, int fieldIdx, DoubleColumnVector vector,
													int childCount, DoubleFunction<T> reader) {

		if (vector.isRepeating) { // fill complete column with first value
			T repeatingValue = reader.apply(vector.vector[0]);
			fillColumnWithRepeatingValue(vals, fieldIdx, repeatingValue, childCount);
		} else {
			if (fieldIdx == -1) { // set as an object
				for (int i = 0; i < childCount; i++) {
					vals[i] = reader.apply(vector.vector[i]);
				}
			} else { // set as a field of Row
				Row[] rows = (Row[]) vals;
				for (int i = 0; i < childCount; i++) {
					rows[i].setField(fieldIdx, reader.apply(vector.vector[i]));
				}
			}
		}
	}

	private static void readNonNullBytesColumnAsString(Object[] vals, int fieldIdx, BytesColumnVector bytes, int childCount) {
		if (bytes.isRepeating) { // fill complete column with first value
			String repeatingValue = readString(bytes.vector[0], bytes.start[0], bytes.length[0]);
			fillColumnWithRepeatingValue(vals, fieldIdx, repeatingValue, childCount);
		} else {
			if (fieldIdx == -1) { // set as an object
				for (int i = 0; i < childCount; i++) {
					vals[i] = readString(bytes.vector[i], bytes.start[i], bytes.length[i]);
				}
			} else { // set as a field of Row
				Row[] rows = (Row[]) vals;
				for (int i = 0; i < childCount; i++) {
					rows[i].setField(fieldIdx, readString(bytes.vector[i], bytes.start[i], bytes.length[i]));
				}
			}
		}
	}

	private static void readNonNullBytesColumnAsBinary(Object[] vals, int fieldIdx, BytesColumnVector bytes, int childCount) {
		if (bytes.isRepeating) { // fill complete column with first value
			if (fieldIdx == -1) { // set as an object
				for (int i = 0; i < childCount; i++) {
					// don't reuse repeating val to avoid object mutation
					vals[i] = readBinary(bytes.vector[0], bytes.start[0], bytes.length[0]);
				}
			} else { // set as a field of Row
				Row[] rows = (Row[]) vals;
				for (int i = 0; i < childCount; i++) {
					// don't reuse repeating val to avoid object mutation
					rows[i].setField(fieldIdx, readBinary(bytes.vector[0], bytes.start[0], bytes.length[0]));
				}
			}
		} else {
			if (fieldIdx == -1) { // set as an object
				for (int i = 0; i < childCount; i++) {
					vals[i] = readBinary(bytes.vector[i], bytes.start[i], bytes.length[i]);
				}
			} else { // set as a field of Row
				Row[] rows = (Row[]) vals;
				for (int i = 0; i < childCount; i++) {
					rows[i].setField(fieldIdx, readBinary(bytes.vector[i], bytes.start[i], bytes.length[i]));
				}
			}
		}
	}

	private static void readNonNullLongColumnAsDate(Object[] vals, int fieldIdx, LongColumnVector vector, int childCount) {

		if (vector.isRepeating) { // fill complete column with first value
			if (fieldIdx == -1) { // set as an object
				for (int i = 0; i < childCount; i++) {
					// do not reuse repeated value due to mutability of Date
					vals[i] = readDate(vector.vector[0]);
				}
			} else { // set as a field of Row
				Row[] rows = (Row[]) vals;
				for (int i = 0; i < childCount; i++) {
					// do not reuse repeated value due to mutability of Date
					rows[i].setField(fieldIdx, readDate(vector.vector[0]));
				}
			}
		} else {
			if (fieldIdx == -1) { // set as an object
				for (int i = 0; i < childCount; i++) {
					vals[i] = readDate(vector.vector[i]);
				}
			} else { // set as a field of Row
				Row[] rows = (Row[]) vals;
				for (int i = 0; i < childCount; i++) {
					rows[i].setField(fieldIdx, readDate(vector.vector[i]));
				}
			}
		}
	}

	private static void readNonNullTimestampColumn(Object[] vals, int fieldIdx, TimestampColumnVector vector, int childCount) {

		if (vector.isRepeating) { // fill complete column with first value
			if (fieldIdx == -1) { // set as an object
				for (int i = 0; i < childCount; i++) {
					// do not reuse value to prevent object mutation
					vals[i] = readTimestamp(vector.time[0], vector.nanos[0]);
				}
			} else { // set as a field of Row
				Row[] rows = (Row[]) vals;
				for (int i = 0; i < childCount; i++) {
					// do not reuse value to prevent object mutation
					rows[i].setField(fieldIdx, readTimestamp(vector.time[0], vector.nanos[0]));
				}
			}
		} else {
			if (fieldIdx == -1) { // set as an object
				for (int i = 0; i < childCount; i++) {
					vals[i] = readTimestamp(vector.time[i], vector.nanos[i]);
				}
			} else { // set as a field of Row
				Row[] rows = (Row[]) vals;
				for (int i = 0; i < childCount; i++) {
					rows[i].setField(fieldIdx, readTimestamp(vector.time[i], vector.nanos[i]));
				}
			}
		}
	}

	private static void readNonNullDecimalColumn(Object[] vals, int fieldIdx, DecimalColumnVector vector, int childCount) {

		if (vector.isRepeating) { // fill complete column with first value
			fillColumnWithRepeatingValue(vals, fieldIdx, readBigDecimal(vector.vector[0]), childCount);
		} else {
			if (fieldIdx == -1) { // set as an object
				for (int i = 0; i < childCount; i++) {
					vals[i] = readBigDecimal(vector.vector[i]);
				}
			} else { // set as a field of Row
				Row[] rows = (Row[]) vals;
				for (int i = 0; i < childCount; i++) {
					rows[i].setField(fieldIdx, readBigDecimal(vector.vector[i]));
				}
			}
		}
	}

	private static void readNonNullStructColumn(Object[] vals, int fieldIdx, StructColumnVector structVector, TypeDescription schema, int childCount) {

		List<TypeDescription> childrenTypes = schema.getChildren();

		int numFields = childrenTypes.size();
		// create a batch of Rows to read the structs
		Row[] structs = new Row[childCount];
		// TODO: possible improvement: reuse existing Row objects
		for (int i = 0; i < childCount; i++) {
			structs[i] = new Row(numFields);
		}

		// read struct fields
		// we don't have to handle isRepeating because ORC assumes that it is propagated into the children.
		for (int i = 0; i < numFields; i++) {
			readField(structs, i, childrenTypes.get(i), structVector.fields[i], childCount);
		}

		if (fieldIdx == -1) { // set struct as an object
			System.arraycopy(structs, 0, vals, 0, childCount);
		} else { // set struct as a field of Row
			Row[] rows = (Row[]) vals;
			for (int i = 0; i < childCount; i++) {
				rows[i].setField(fieldIdx, structs[i]);
			}
		}
	}

	private static void readNonNullListColumn(Object[] vals, int fieldIdx, ListColumnVector list, TypeDescription schema, int childCount) {

		TypeDescription fieldType = schema.getChildren().get(0);
		// get class of list elements
		Class<?> classType = getClassForType(fieldType);

		if (list.isRepeating) {

			int offset = (int) list.offsets[0];
			int length = (int) list.lengths[0];
			// we only need to read until offset + length.
			int entriesToRead = offset + length;

			// read children
			Object[] children = (Object[]) Array.newInstance(classType, entriesToRead);
			readField(children, -1, fieldType, list.child, entriesToRead);

			// get function to copy list
			Function<Object, Object> copyList = getCopyFunction(schema);

			// create first list that will be copied
			Object[] first;
			if (offset == 0) {
				first = children;
			} else {
				first = (Object[]) Array.newInstance(classType, length);
				System.arraycopy(children, offset, first, 0, length);
			}

			// create copies of first list and set copies as result
			for (int i = 0; i < childCount; i++) {
				Object[] copy = (Object[]) copyList.apply(first);
				if (fieldIdx == -1) {
					vals[i] = copy;
				} else {
					((Row) vals[i]).setField(fieldIdx, copy);
				}
			}
		} else {

			// read children
			Object[] children = (Object[]) Array.newInstance(classType, list.childCount);
			readField(children, -1, fieldType, list.child, list.childCount);

			// fill lists with children
			for (int i = 0; i < childCount; i++) {
				int offset = (int) list.offsets[i];
				int length = (int) list.lengths[i];

				Object[] temp = (Object[]) Array.newInstance(classType, length);
				System.arraycopy(children, offset, temp, 0, length);
				if (fieldIdx == -1) {
					vals[i] = temp;
				} else {
					((Row) vals[i]).setField(fieldIdx, temp);
				}
			}
		}
	}

	private static void readNonNullMapColumn(Object[] vals, int fieldIdx, MapColumnVector mapsVector, TypeDescription schema, int childCount) {

		List<TypeDescription> fieldType = schema.getChildren();
		TypeDescription keyType = fieldType.get(0);
		TypeDescription valueType = fieldType.get(1);

		ColumnVector keys = mapsVector.keys;
		ColumnVector values = mapsVector.values;

		if (mapsVector.isRepeating) {
			// first map is repeated

			// get map copy function
			Function<Object, Object> copyMap = getCopyFunction(schema);

			// set all key and value entries except those of the first map to null
			int offset = (int) mapsVector.offsets[0];
			int length = (int) mapsVector.lengths[0];
			// we only need to read until offset + length.
			int entriesToRead = offset + length;

			Object[] keyRows = new Object[entriesToRead];
			Object[] valueRows = new Object[entriesToRead];

			// read map keys and values
			readField(keyRows, -1, keyType, keys, entriesToRead);
			readField(valueRows, -1, valueType, values, entriesToRead);

			// create first map that will be copied
			HashMap map = readHashMap(keyRows, valueRows, offset, length);

			// copy first map and set copy as result
			for (int i = 0; i < childCount; i++) {
				if (fieldIdx == -1) {
					vals[i] = copyMap.apply(map);
				} else {
					((Row) vals[i]).setField(fieldIdx, copyMap.apply(map));
				}
			}

		} else {

			Object[] keyRows = new Object[mapsVector.childCount];
			Object[] valueRows = new Object[mapsVector.childCount];

			// read map keys and values
			readField(keyRows, -1, keyType, keys, keyRows.length);
			readField(valueRows, -1, valueType, values, valueRows.length);

			long[] lengthVectorMap = mapsVector.lengths;
			int offset = 0;

			for (int i = 0; i < childCount; i++) {
				long numMapEntries = lengthVectorMap[i];
				HashMap map = readHashMap(keyRows, valueRows, offset, numMapEntries);
				offset += numMapEntries;

				if (fieldIdx == -1) {
					vals[i] = map;
				} else {
					((Row) vals[i]).setField(fieldIdx, map);
				}
			}
		}

	}

	private static <T> void readLongColumn(Object[] vals, int fieldIdx, LongColumnVector vector,
											int childCount, LongFunction<T> reader) {

		if (vector.isRepeating) { // fill complete column with first value
			if (vector.isNull[0]) {
				// fill vals with null values
				fillColumnWithRepeatingValue(vals, fieldIdx, null, childCount);
			} else {
				// read repeating non-null value by forwarding call.
				readNonNullLongColumn(vals, fieldIdx, vector, childCount, reader);
			}
		} else {
			boolean[] isNullVector = vector.isNull;
			if (fieldIdx == -1) { // set as an object
				for (int i = 0; i < childCount; i++) {
					if (isNullVector[i]) {
						vals[i] = null;
					} else {
						vals[i] = reader.apply(vector.vector[i]);
					}
				}
			} else { // set as a field of Row
				Row[] rows = (Row[]) vals;
				for (int i = 0; i < childCount; i++) {
					if (isNullVector[i]) {
						rows[i].setField(fieldIdx, null);
					} else {
						rows[i].setField(fieldIdx, reader.apply(vector.vector[i]));
					}
				}
			}
		}
	}

	private static <T> void readDoubleColumn(Object[] vals, int fieldIdx, DoubleColumnVector vector,
												int childCount, DoubleFunction<T> reader) {

		if (vector.isRepeating) { // fill complete column with first value
			if (vector.isNull[0]) {
				// fill vals with null values
				fillColumnWithRepeatingValue(vals, fieldIdx, null, childCount);
			} else {
				// read repeating non-null value by forwarding call
				readNonNullDoubleColumn(vals, fieldIdx, vector, childCount, reader);
			}
		} else {
			boolean[] isNullVector = vector.isNull;
			if (fieldIdx == -1) { // set as an object
				for (int i = 0; i < childCount; i++) {
					if (isNullVector[i]) {
						vals[i] = null;
					} else {
						vals[i] = reader.apply(vector.vector[i]);
					}
				}
			} else { // set as a field of Row
				Row[] rows = (Row[]) vals;
				for (int i = 0; i < childCount; i++) {
					if (isNullVector[i]) {
						rows[i].setField(fieldIdx, null);
					} else {
						rows[i].setField(fieldIdx, reader.apply(vector.vector[i]));
					}
				}
			}
		}
	}

	private static void readBytesColumnAsString(Object[] vals, int fieldIdx, BytesColumnVector bytes, int childCount) {

		if (bytes.isRepeating) { // fill complete column with first value
			if (bytes.isNull[0]) {
				// fill vals with null values
				fillColumnWithRepeatingValue(vals, fieldIdx, null, childCount);
			} else {
				// read repeating non-null value by forwarding call
				readNonNullBytesColumnAsString(vals, fieldIdx, bytes, childCount);
			}
		} else {
			boolean[] isNullVector = bytes.isNull;
			if (fieldIdx == -1) { // set as an object
				for (int i = 0; i < childCount; i++) {
					if (isNullVector[i]) {
						vals[i] = null;
					} else {
						vals[i] = readString(bytes.vector[i], bytes.start[i], bytes.length[i]);
					}
				}
			} else { // set as a field of Row
				Row[] rows = (Row[]) vals;
				for (int i = 0; i < childCount; i++) {
					if (isNullVector[i]) {
						rows[i].setField(fieldIdx, null);
					} else {
						rows[i].setField(fieldIdx, readString(bytes.vector[i], bytes.start[i], bytes.length[i]));
					}
				}
			}
		}
	}

	private static void readBytesColumnAsBinary(Object[] vals, int fieldIdx, BytesColumnVector bytes, int childCount) {

		if (bytes.isRepeating) { // fill complete column with first value
			if (bytes.isNull[0]) {
				// fill vals with null values
				fillColumnWithRepeatingValue(vals, fieldIdx, null, childCount);
			} else {
				// read repeating non-null value by forwarding call
				readNonNullBytesColumnAsBinary(vals, fieldIdx, bytes, childCount);
			}
		} else {
			boolean[] isNullVector = bytes.isNull;
			if (fieldIdx == -1) { // set as an object
				for (int i = 0; i < childCount; i++) {
					if (isNullVector[i]) {
						vals[i] = null;
					} else {
						vals[i] = readBinary(bytes.vector[i], bytes.start[i], bytes.length[i]);
					}
				}
			} else { // set as a field of Row
				Row[] rows = (Row[]) vals;
				for (int i = 0; i < childCount; i++) {
					if (isNullVector[i]) {
						rows[i].setField(fieldIdx, null);
					} else {
						rows[i].setField(fieldIdx, readBinary(bytes.vector[i], bytes.start[i], bytes.length[i]));
					}
				}
			}
		}
	}

	private static void readLongColumnAsDate(Object[] vals, int fieldIdx, LongColumnVector vector, int childCount) {

		if (vector.isRepeating) { // fill complete column with first value
			if (vector.isNull[0]) {
				// fill vals with null values
				fillColumnWithRepeatingValue(vals, fieldIdx, null, childCount);
			} else {
				// read repeating non-null value by forwarding call
				readNonNullLongColumnAsDate(vals, fieldIdx, vector, childCount);
			}
		} else {
			boolean[] isNullVector = vector.isNull;
			if (fieldIdx == -1) { // set as an object
				for (int i = 0; i < childCount; i++) {
					if (isNullVector[i]) {
						vals[i] = null;
					} else {
						vals[i] = readDate(vector.vector[i]);
					}
				}
			} else { // set as a field of Row
				Row[] rows = (Row[]) vals;
				for (int i = 0; i < childCount; i++) {
					if (isNullVector[i]) {
						rows[i].setField(fieldIdx, null);
					} else {
						rows[i].setField(fieldIdx, readDate(vector.vector[i]));
					}
				}
			}
		}
	}

	private static void readTimestampColumn(Object[] vals, int fieldIdx, TimestampColumnVector vector, int childCount) {

		if (vector.isRepeating) { // fill complete column with first value
			if (vector.isNull[0]) {
				// fill vals with null values
				fillColumnWithRepeatingValue(vals, fieldIdx, null, childCount);
			} else {
				// read repeating non-null value by forwarding call
				readNonNullTimestampColumn(vals, fieldIdx, vector, childCount);
			}
		} else {
			boolean[] isNullVector = vector.isNull;
			if (fieldIdx == -1) { // set as an object
				for (int i = 0; i < childCount; i++) {
					if (isNullVector[i]) {
						vals[i] = null;
					} else {
						Timestamp ts = readTimestamp(vector.time[i], vector.nanos[i]);
						vals[i] = ts;
					}
				}
			} else { // set as a field of Row
				Row[] rows = (Row[]) vals;
				for (int i = 0; i < childCount; i++) {
					if (isNullVector[i]) {
						rows[i].setField(fieldIdx, null);
					} else {
						Timestamp ts = readTimestamp(vector.time[i], vector.nanos[i]);
						rows[i].setField(fieldIdx, ts);
					}
				}
			}
		}
	}

	private static void readDecimalColumn(Object[] vals, int fieldIdx, DecimalColumnVector vector, int childCount) {

		if (vector.isRepeating) { // fill complete column with first value
			if (vector.isNull[0]) {
				// fill vals with null values
				fillColumnWithRepeatingValue(vals, fieldIdx, null, childCount);
			} else {
				// read repeating non-null value by forwarding call
				readNonNullDecimalColumn(vals, fieldIdx, vector, childCount);
			}
		} else {
			boolean[] isNullVector = vector.isNull;
			if (fieldIdx == -1) { // set as an object
				for (int i = 0; i < childCount; i++) {
					if (isNullVector[i]) {
						vals[i] = null;
					} else {
						vals[i] = readBigDecimal(vector.vector[i]);
					}
				}
			} else { // set as a field of Row
				Row[] rows = (Row[]) vals;
				for (int i = 0; i < childCount; i++) {
					if (isNullVector[i]) {
						rows[i].setField(fieldIdx, null);
					} else {
						rows[i].setField(fieldIdx, readBigDecimal(vector.vector[i]));
					}
				}
			}
		}
	}

	private static void readStructColumn(Object[] vals, int fieldIdx, StructColumnVector structVector, TypeDescription schema, int childCount) {

		List<TypeDescription> childrenTypes = schema.getChildren();

		int numFields = childrenTypes.size();

		// Early out if struct column is repeating and always null.
		// This is the only repeating case we need to handle.
		// ORC assumes that repeating values have been pushed to the children.
		if (structVector.isRepeating && structVector.isNull[0]) {
			if (fieldIdx < 0) {
				for (int i = 0; i < childCount; i++) {
					vals[i] = null;
				}
			} else {
				for (int i = 0; i < childCount; i++) {
					((Row) vals[i]).setField(fieldIdx, null);
				}
			}
			return;
		}

		// create a batch of Rows to read the structs
		Row[] structs = new Row[childCount];
		// TODO: possible improvement: reuse existing Row objects
		for (int i = 0; i < childCount; i++) {
			structs[i] = new Row(numFields);
		}

		// read struct fields
		for (int i = 0; i < numFields; i++) {
			ColumnVector fieldVector = structVector.fields[i];
			if (!fieldVector.isRepeating) {
				// Reduce fieldVector reads by setting all entries null where struct is null.
				if (fieldVector.noNulls) {
					// fieldVector had no nulls. Just use struct null information.
					System.arraycopy(structVector.isNull, 0, fieldVector.isNull, 0, structVector.isNull.length);
					structVector.fields[i].noNulls = false;
				} else {
					// fieldVector had nulls. Merge field nulls with struct nulls.
					for (int j = 0; j < structVector.isNull.length; j++) {
						structVector.fields[i].isNull[j] = structVector.isNull[j] || structVector.fields[i].isNull[j];
					}
				}
			}
			readField(structs, i, childrenTypes.get(i), structVector.fields[i], childCount);
		}

		boolean[] isNullVector = structVector.isNull;

		if (fieldIdx == -1) { // set struct as an object
			for (int i = 0; i < childCount; i++) {
				if (isNullVector[i]) {
					vals[i] = null;
				} else {
					vals[i] = structs[i];
				}
			}
		} else { // set struct as a field of Row
			Row[] rows = (Row[]) vals;
			for (int i = 0; i < childCount; i++) {
				if (isNullVector[i]) {
					rows[i].setField(fieldIdx, null);
				} else {
					rows[i].setField(fieldIdx, structs[i]);
				}
			}
		}
	}

	private static void readListColumn(Object[] vals, int fieldIdx, ListColumnVector list, TypeDescription schema, int childCount) {

		TypeDescription fieldType = schema.getChildren().get(0);
		// get class of list elements
		Class<?> classType = getClassForType(fieldType);

		if (list.isRepeating) {
			// list values are repeating. we only need to read the first list and copy it.

			if (list.isNull[0]) {
				// Even better. The first list is null and so are all lists are null
				for (int i = 0; i < childCount; i++) {
					if (fieldIdx == -1) {
						vals[i] = null;
					} else {
						((Row) vals[i]).setField(fieldIdx, null);
					}
				}

			} else {
				// Get function to copy list
				Function<Object, Object> copyList = getCopyFunction(schema);

				int offset = (int) list.offsets[0];
				int length = (int) list.lengths[0];
				// we only need to read until offset + length.
				int entriesToRead = offset + length;

				// read entries
				Object[] children = (Object[]) Array.newInstance(classType, entriesToRead);
				readField(children, -1, fieldType, list.child, entriesToRead);

				// create first list which will be copied
				Object[] temp;
				if (offset == 0) {
					temp = children;
				} else {
					temp = (Object[]) Array.newInstance(classType, length);
					System.arraycopy(children, offset, temp, 0, length);
				}

				// copy repeated list and set copy as result
				for (int i = 0; i < childCount; i++) {
					Object[] copy = (Object[]) copyList.apply(temp);
					if (fieldIdx == -1) {
						vals[i] = copy;
					} else {
						((Row) vals[i]).setField(fieldIdx, copy);
					}
				}
			}

		} else {
			if (!list.child.isRepeating) {
				boolean[] childIsNull = new boolean[list.childCount];
				Arrays.fill(childIsNull, true);
				// forward info of null lists into child vector
				for (int i = 0; i < childCount; i++) {
					// preserve isNull info of entries of non-null lists
					if (!list.isNull[i]) {
						int offset = (int) list.offsets[i];
						int length = (int) list.lengths[i];
						System.arraycopy(list.child.isNull, offset, childIsNull, offset, length);
					}
				}
				// override isNull of children vector
				list.child.isNull = childIsNull;
				list.child.noNulls = false;
			}

			// read children
			Object[] children = (Object[]) Array.newInstance(classType, list.childCount);
			readField(children, -1, fieldType, list.child, list.childCount);

			Object[] temp;
			// fill lists with children
			for (int i = 0; i < childCount; i++) {

				if (list.isNull[i]) {
					temp = null;
				} else {
					int offset = (int) list.offsets[i];
					int length = (int) list.lengths[i];

					temp = (Object[]) Array.newInstance(classType, length);
					System.arraycopy(children, offset, temp, 0, length);
				}

				if (fieldIdx == -1) {
					vals[i] = temp;
				} else {
					((Row) vals[i]).setField(fieldIdx, temp);
				}
			}
		}
	}

	private static void readMapColumn(Object[] vals, int fieldIdx, MapColumnVector map, TypeDescription schema, int childCount) {

		List<TypeDescription> fieldType = schema.getChildren();
		TypeDescription keyType = fieldType.get(0);
		TypeDescription valueType = fieldType.get(1);

		ColumnVector keys = map.keys;
		ColumnVector values = map.values;

		if (map.isRepeating) {
			// map values are repeating. we only need to read the first map and copy it.

			if (map.isNull[0]) {
				// Even better. The first map is null and so are all maps are null
				for (int i = 0; i < childCount; i++) {
					if (fieldIdx == -1) {
						vals[i] = null;
					} else {
						((Row) vals[i]).setField(fieldIdx, null);
					}
				}

			} else {
				// Get function to copy map
				Function<Object, Object> copyMap = getCopyFunction(schema);

				int offset = (int) map.offsets[0];
				int length = (int) map.lengths[0];
				// we only need to read until offset + length.
				int entriesToRead = offset + length;

				Object[] keyRows = new Object[entriesToRead];
				Object[] valueRows = new Object[entriesToRead];

				// read map keys and values
				readField(keyRows, -1, keyType, keys, entriesToRead);
				readField(valueRows, -1, valueType, values, entriesToRead);

				// create first map which will be copied
				HashMap temp = readHashMap(keyRows, valueRows, offset, length);

				// copy repeated map and set copy as result
				for (int i = 0; i < childCount; i++) {
					if (fieldIdx == -1) {
						vals[i] = copyMap.apply(temp);
					} else {
						((Row) vals[i]).setField(fieldIdx, copyMap.apply(temp));
					}
				}
			}
		} else {
			// ensure only keys and values that are referenced by non-null maps are set to non-null

			if (!keys.isRepeating) {
				// propagate is null info of map into keys vector
				boolean[] keyIsNull = new boolean[map.childCount];
				Arrays.fill(keyIsNull, true);
				for (int i = 0; i < childCount; i++) {
					// preserve isNull info for keys of non-null maps
					if (!map.isNull[i]) {
						int offset = (int) map.offsets[i];
						int length = (int) map.lengths[i];
						System.arraycopy(keys.isNull, offset, keyIsNull, offset, length);
					}
				}
				// override isNull of keys vector
				keys.isNull = keyIsNull;
				keys.noNulls = false;
			}
			if (!values.isRepeating) {
				// propagate is null info of map into values vector
				boolean[] valIsNull = new boolean[map.childCount];
				Arrays.fill(valIsNull, true);
				for (int i = 0; i < childCount; i++) {
					// preserve isNull info for vals of non-null maps
					if (!map.isNull[i]) {
						int offset = (int) map.offsets[i];
						int length = (int) map.lengths[i];
						System.arraycopy(values.isNull, offset, valIsNull, offset, length);
					}
				}
				// override isNull of values vector
				values.isNull = valIsNull;
				values.noNulls = false;
			}

			Object[] keyRows = new Object[map.childCount];
			Object[] valueRows = new Object[map.childCount];

			// read map keys and values
			readField(keyRows, -1, keyType, keys, keyRows.length);
			readField(valueRows, -1, valueType, values, valueRows.length);

			boolean[] isNullVector = map.isNull;
			long[] lengths = map.lengths;
			long[] offsets = map.offsets;

			if (fieldIdx == -1) { // set map as an object
				for (int i = 0; i < childCount; i++) {
					if (isNullVector[i]) {
						vals[i] = null;
					} else {
						vals[i] = readHashMap(keyRows, valueRows, (int) offsets[i], lengths[i]);
					}
				}
			} else { // set map as a field of Row
				Row[] rows = (Row[]) vals;
				for (int i = 0; i < childCount; i++) {
					if (isNullVector[i]) {
						rows[i].setField(fieldIdx, null);
					} else {
						rows[i].setField(fieldIdx, readHashMap(keyRows, valueRows, (int) offsets[i], lengths[i]));
					}
				}
			}
		}
	}

	/**
	 * Sets a repeating value to all objects or row fields of the passed vals array.
	 *
	 * @param vals The array of objects or Rows.
	 * @param fieldIdx If the objs array is an array of Row, the index of the field that needs to be filled.
	 *                 Otherwise a -1 must be passed and the data is directly filled into the array.
	 * @param repeatingValue The value that is set.
	 * @param childCount The number of times the value is set.
	 */
	private static void fillColumnWithRepeatingValue(Object[] vals, int fieldIdx, Object repeatingValue, int childCount) {

		if (fieldIdx == -1) {
			// set value as an object
			Arrays.fill(vals, 0, childCount, repeatingValue);
		} else {
			// set value as a field of Row
			Row[] rows = (Row[]) vals;
			for (int i = 0; i < childCount; i++) {
				rows[i].setField(fieldIdx, repeatingValue);
			}
		}
	}

	private static Class<?> getClassForType(TypeDescription schema) {

		// check the type of the vector to decide how to read it.
		switch (schema.getCategory()) {
			case BOOLEAN:
				return Boolean.class;
			case BYTE:
				return Byte.class;
			case SHORT:
				return Short.class;
			case INT:
				return Integer.class;
			case LONG:
				return Long.class;
			case FLOAT:
				return Float.class;
			case DOUBLE:
				return Double.class;
			case CHAR:
			case VARCHAR:
			case STRING:
				return String.class;
			case DATE:
				return Date.class;
			case TIMESTAMP:
				return Timestamp.class;
			case BINARY:
				return byte[].class;
			case DECIMAL:
				return BigDecimal.class;
			case STRUCT:
				return Row.class;
			case LIST:
				Class<?> childClass = getClassForType(schema.getChildren().get(0));
				return Array.newInstance(childClass, 0).getClass();
			case MAP:
				return HashMap.class;
			case UNION:
				throw new UnsupportedOperationException("UNION type not supported yet");
			default:
				throw new IllegalArgumentException("Unknown type " + schema);
		}
	}

	private static Boolean readBoolean(long l) {
		return l != 0;
	}

	private static Byte readByte(long l) {
		return (byte) l;
	}

	private static Short readShort(long l) {
		return (short) l;
	}

	private static Integer readInt(long l) {
		return (int) l;
	}

	private static Long readLong(long l) {
		return l;
	}

	private static Float readFloat(double d) {
		return (float) d;
	}

	private static Double readDouble(double d) {
		return d;
	}

	private static Date readDate(long l) {
		// day to milliseconds
		final long t = l * MILLIS_PER_DAY;
		// adjust by local timezone
		return new java.sql.Date(t - LOCAL_TZ.getOffset(t));
	}

	private static String readString(byte[] bytes, int start, int length) {
		return new String(bytes, start, length, StandardCharsets.UTF_8);
	}

	private static byte[] readBinary(byte[] src, int srcPos, int length) {
		byte[] result = new byte[length];
		System.arraycopy(src, srcPos, result, 0, length);
		return result;
	}

	private static BigDecimal readBigDecimal(HiveDecimalWritable hiveDecimalWritable) {
		HiveDecimal hiveDecimal = hiveDecimalWritable.getHiveDecimal();
		return hiveDecimal.bigDecimalValue();
	}

	private static Timestamp readTimestamp(long time, int nanos) {
		Timestamp ts = new Timestamp(time);
		ts.setNanos(nanos);
		return ts;
	}

	private static HashMap readHashMap(Object[] keyRows, Object[] valueRows, int offset, long length) {
		HashMap<Object, Object> resultMap = new HashMap<>();
		for (int j = 0; j < length; j++) {
			resultMap.put(keyRows[offset], valueRows[offset]);
			offset++;
		}
		return resultMap;
	}

	@SuppressWarnings("unchecked")
	private static Function<Object, Object> getCopyFunction(TypeDescription schema) {
		// check the type of the vector to decide how to read it.
		switch (schema.getCategory()) {
			case BOOLEAN:
			case BYTE:
			case SHORT:
			case INT:
			case LONG:
			case FLOAT:
			case DOUBLE:
			case CHAR:
			case VARCHAR:
			case STRING:
			case DECIMAL:
				return OrcBatchReader::returnImmutable;
			case DATE:
				return OrcBatchReader::copyDate;
			case TIMESTAMP:
				return OrcBatchReader::copyTimestamp;
			case BINARY:
				return OrcBatchReader::copyBinary;
			case STRUCT:
				List<TypeDescription> fieldTypes = schema.getChildren();
				Function<Object, Object>[] copyFields = new Function[fieldTypes.size()];
				for (int i = 0; i < fieldTypes.size(); i++) {
					copyFields[i] = getCopyFunction(fieldTypes.get(i));
				}
				return new CopyStruct(copyFields);
			case LIST:
				TypeDescription entryType = schema.getChildren().get(0);
				Function<Object, Object> copyEntry = getCopyFunction(entryType);
				Class entryClass = getClassForType(entryType);
				return new CopyList(copyEntry, entryClass);
			case MAP:
				TypeDescription keyType = schema.getChildren().get(0);
				TypeDescription valueType = schema.getChildren().get(1);
				Function<Object, Object> copyKey = getCopyFunction(keyType);
				Function<Object, Object> copyValue = getCopyFunction(valueType);
				return new CopyMap(copyKey, copyValue);
			case UNION:
				throw new UnsupportedOperationException("UNION type not supported yet");
			default:
				throw new IllegalArgumentException("Unknown type " + schema);
		}
	}

	private static Object returnImmutable(Object o) {
		return o;
	}

	private static Date copyDate(Object o) {
		if (o == null) {
			return null;
		} else {
			long date = ((Date) o).getTime();
			return new Date(date);
		}
	}

	private static Timestamp copyTimestamp(Object o) {
		if (o == null) {
			return null;
		} else {
			long millis = ((Timestamp) o).getTime();
			int nanos = ((Timestamp) o).getNanos();
			Timestamp copy = new Timestamp(millis);
			copy.setNanos(nanos);
			return copy;
		}
	}

	private static byte[] copyBinary(Object o) {
		if (o == null) {
			return null;
		} else {
			int length = ((byte[]) o).length;
			return Arrays.copyOf((byte[]) o, length);
		}
	}

	private static class CopyStruct implements Function<Object, Object> {

		private final Function<Object, Object>[] copyFields;

		CopyStruct(Function<Object, Object>[] copyFields) {
			this.copyFields = copyFields;
		}

		@Override
		public Object apply(Object o) {
			if (o == null) {
				return null;
			} else {
				Row r = (Row) o;
				Row copy = new Row(copyFields.length);
				for (int i = 0; i < copyFields.length; i++) {
					copy.setField(i, copyFields[i].apply(r.getField(i)));
				}
				return copy;
			}
		}
	}

	private static class CopyList implements Function<Object, Object> {

		private final Function<Object, Object> copyEntry;
		private final Class entryClass;

		CopyList(Function<Object, Object> copyEntry, Class entryClass) {
			this.copyEntry = copyEntry;
			this.entryClass = entryClass;
		}

		@Override
		public Object apply(Object o) {
			if (o == null) {
				return null;
			} else {
				Object[] l = (Object[]) o;
				Object[] copy = (Object[]) Array.newInstance(entryClass, l.length);
				for (int i = 0; i < l.length; i++) {
					copy[i] = copyEntry.apply(l[i]);
				}
				return copy;
			}
		}
	}

	@SuppressWarnings("unchecked")
	private static class CopyMap implements Function<Object, Object> {

		private final Function<Object, Object> copyKey;
		private final Function<Object, Object> copyValue;

		CopyMap(Function<Object, Object> copyKey, Function<Object, Object> copyValue) {
			this.copyKey = copyKey;
			this.copyValue = copyValue;
		}

		@Override
		public Object apply(Object o) {
			if (o == null) {
				return null;
			} else {
				Map<Object, Object> m = (Map<Object, Object>) o;
				HashMap<Object, Object> copy = new HashMap<>(m.size());

				for (Map.Entry<Object, Object> e : m.entrySet()) {
					Object keyCopy = copyKey.apply(e.getKey());
					Object valueCopy = copyValue.apply(e.getValue());
					copy.put(keyCopy, valueCopy);
				}
				return copy;
			}
		}
	}
}
