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
import java.util.TimeZone;
import java.util.function.DoubleFunction;
import java.util.function.IntFunction;
import java.util.function.LongFunction;

/**
 * A class that provides utility methods for orc file reading.
 */
class OrcUtils {

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
		for (int rowIdx = 0; rowIdx < selectedFields.length; rowIdx++) {
			int orcIdx = selectedFields[rowIdx];
			readField(rows, rowIdx, fieldTypes.get(orcIdx), batch.cols[orcIdx], null, rowsToRead);
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
	 * @param lengthVector If the vector is of type List or Map, the number of sub-elements to read for each field. Otherwise, it must be null.
	 * @param childCount The number of vector entries to read.
	 */
	private static void readField(Object[] vals, int fieldIdx, TypeDescription schema, ColumnVector vector, long[] lengthVector, int childCount) {

		// check the type of the vector to decide how to read it.
		switch (schema.getCategory()) {
			case BOOLEAN:
				if (vector.noNulls) {
					readNonNullLongColumn(vals, fieldIdx, (LongColumnVector) vector, lengthVector, childCount, OrcUtils::readBoolean, OrcUtils::boolArray);
				} else {
					readLongColumn(vals, fieldIdx, (LongColumnVector) vector, lengthVector, childCount, OrcUtils::readBoolean, OrcUtils::boolArray);
				}
				break;
			case BYTE:
				if (vector.noNulls) {
					readNonNullLongColumn(vals, fieldIdx, (LongColumnVector) vector, lengthVector, childCount, OrcUtils::readByte, OrcUtils::byteArray);
				} else {
					readLongColumn(vals, fieldIdx, (LongColumnVector) vector, lengthVector, childCount, OrcUtils::readByte, OrcUtils::byteArray);
				}
				break;
			case SHORT:
				if (vector.noNulls) {
					readNonNullLongColumn(vals, fieldIdx, (LongColumnVector) vector, lengthVector, childCount, OrcUtils::readShort, OrcUtils::shortArray);
				} else {
					readLongColumn(vals, fieldIdx, (LongColumnVector) vector, lengthVector, childCount, OrcUtils::readShort, OrcUtils::shortArray);
				}
				break;
			case INT:
				if (vector.noNulls) {
					readNonNullLongColumn(vals, fieldIdx, (LongColumnVector) vector, lengthVector, childCount, OrcUtils::readInt, OrcUtils::intArray);
				} else {
					readLongColumn(vals, fieldIdx, (LongColumnVector) vector, lengthVector, childCount, OrcUtils::readInt, OrcUtils::intArray);
				}
				break;
			case LONG:
				if (vector.noNulls) {
					readNonNullLongColumn(vals, fieldIdx, (LongColumnVector) vector, lengthVector, childCount, OrcUtils::readLong, OrcUtils::longArray);
				} else {
					readLongColumn(vals, fieldIdx, (LongColumnVector) vector, lengthVector, childCount, OrcUtils::readLong, OrcUtils::longArray);
				}
				break;
			case FLOAT:
				if (vector.noNulls) {
					readNonNullDoubleColumn(vals, fieldIdx, (DoubleColumnVector) vector, lengthVector, childCount, OrcUtils::readFloat, OrcUtils::floatArray);
				} else {
					readDoubleColumn(vals, fieldIdx, (DoubleColumnVector) vector, lengthVector, childCount, OrcUtils::readFloat, OrcUtils::floatArray);
				}
				break;
			case DOUBLE:
				if (vector.noNulls) {
					readNonNullDoubleColumn(vals, fieldIdx, (DoubleColumnVector) vector, lengthVector, childCount, OrcUtils::readDouble, OrcUtils::doubleArray);
				} else {
					readDoubleColumn(vals, fieldIdx, (DoubleColumnVector) vector, lengthVector, childCount, OrcUtils::readDouble, OrcUtils::doubleArray);
				}
				break;
			case CHAR:
			case VARCHAR:
			case STRING:
				if (vector.noNulls) {
					readNonNullBytesColumnAsString(vals, fieldIdx, (BytesColumnVector) vector, lengthVector, childCount);
				} else {
					readBytesColumnAsString(vals, fieldIdx, (BytesColumnVector) vector, lengthVector, childCount);
				}
				break;
			case DATE:
				if (vector.noNulls) {
					readNonNullLongColumnAsDate(vals, fieldIdx, (LongColumnVector) vector, lengthVector, childCount);
				} else {
					readLongColumnAsDate(vals, fieldIdx, (LongColumnVector) vector, lengthVector, childCount);
				}
				break;
			case TIMESTAMP:
				if (vector.noNulls) {
					readNonNullTimestampColumn(vals, fieldIdx, (TimestampColumnVector) vector, lengthVector, childCount);
				} else {
					readTimestampColumn(vals, fieldIdx, (TimestampColumnVector) vector, lengthVector, childCount);
				}
				break;
			case BINARY:
				if (vector.noNulls) {
					readNonNullBytesColumnAsBinary(vals, fieldIdx, (BytesColumnVector) vector, lengthVector, childCount);
				} else {
					readBytesColumnAsBinary(vals, fieldIdx, (BytesColumnVector) vector, lengthVector, childCount);
				}
				break;
			case DECIMAL:
				if (vector.noNulls) {
					readNonNullDecimalColumn(vals, fieldIdx, (DecimalColumnVector) vector, lengthVector, childCount);
				} else {
					readDecimalColumn(vals, fieldIdx, (DecimalColumnVector) vector, lengthVector, childCount);
				}
				break;
			case STRUCT:
				if (vector.noNulls) {
					readNonNullStructColumn(vals, fieldIdx, (StructColumnVector) vector, schema, lengthVector, childCount);
				} else {
					readStructColumn(vals, fieldIdx, (StructColumnVector) vector, schema, lengthVector, childCount);
				}
				break;
			case LIST:
				if (vector.noNulls) {
					readNonNullListColumn(vals, fieldIdx, (ListColumnVector) vector, schema, lengthVector, childCount);
				} else {
					readListColumn(vals, fieldIdx, (ListColumnVector) vector, schema, lengthVector, childCount);
				}
				break;
			case MAP:
				if (vector.noNulls) {
					readNonNullMapColumn(vals, fieldIdx, (MapColumnVector) vector, schema, lengthVector, childCount);
				} else {
					readMapColumn(vals, fieldIdx, (MapColumnVector) vector, schema, lengthVector, childCount);
				}
				break;
			case UNION:
				throw new UnsupportedOperationException("UNION type not supported yet");
			default:
				throw new IllegalArgumentException("Unknown type " + schema);
		}
	}

	private static <T> void readNonNullLongColumn(Object[] vals, int fieldIdx, LongColumnVector vector, long[] lengthVector, int childCount,
													LongFunction<T> reader, IntFunction<T[]> array) {

		// check if the values need to be read into lists or as single values
		if (lengthVector == null) {
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
		} else { // in a list
			T[] temp;
			int offset = 0;
			if (vector.isRepeating) { // fill complete list with first value
				T repeatingValue = reader.apply(vector.vector[0]);
				for (int i = 0; offset < childCount; i++) {
					temp = array.apply((int) lengthVector[i]);
					Arrays.fill(temp, repeatingValue);
					offset += temp.length;
					if (fieldIdx == -1) {
						vals[i] = temp;
					} else {
						((Row) vals[i]).setField(fieldIdx, temp);
					}
				}
			} else {
				for (int i = 0; offset < childCount; i++) {
					temp = array.apply((int) lengthVector[i]);
					for (int j = 0; j < temp.length; j++) {
						temp[j] = reader.apply(vector.vector[offset++]);
					}
					if (fieldIdx == -1) {
						vals[i] = temp;
					} else {
						((Row) vals[i]).setField(fieldIdx, temp);
					}
				}
			}
		}
	}

	private static <T> void readNonNullDoubleColumn(Object[] vals, int fieldIdx, DoubleColumnVector vector, long[] lengthVector, int childCount,
													DoubleFunction<T> reader, IntFunction<T[]> array) {

		// check if the values need to be read into lists or as single values
		if (lengthVector == null) {
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
		} else { // in a list
			T[] temp;
			int offset = 0;
			if (vector.isRepeating) { // fill complete list with first value
				T repeatingValue = reader.apply(vector.vector[0]);
				for (int i = 0; offset < childCount; i++) {
					temp = array.apply((int) lengthVector[i]);
					Arrays.fill(temp, repeatingValue);
					offset += temp.length;
					if (fieldIdx == -1) {
						vals[i] = temp;
					} else {
						((Row) vals[i]).setField(fieldIdx, temp);
					}
				}
			} else {
				for (int i = 0; offset < childCount; i++) {
					temp = array.apply((int) lengthVector[i]);
					for (int j = 0; j < temp.length; j++) {
						temp[j] = reader.apply(vector.vector[offset++]);
					}
					if (fieldIdx == -1) {
						vals[i] = temp;
					} else {
						((Row) vals[i]).setField(fieldIdx, temp);
					}
				}
			}
		}
	}

	private static void readNonNullBytesColumnAsString(Object[] vals, int fieldIdx, BytesColumnVector bytes, long[] lengthVector, int childCount) {
		// check if the values need to be read into lists or as single values
		if (lengthVector == null) {
			if (bytes.isRepeating) { // fill complete column with first value
				String repeatingValue = new String(bytes.vector[0], bytes.start[0], bytes.length[0]);
				fillColumnWithRepeatingValue(vals, fieldIdx, repeatingValue, childCount);
			} else {
				if (fieldIdx == -1) { // set as an object
					for (int i = 0; i < childCount; i++) {
						vals[i] = new String(bytes.vector[i], bytes.start[i], bytes.length[i], StandardCharsets.UTF_8);
					}
				} else { // set as a field of Row
					Row[] rows = (Row[]) vals;
					for (int i = 0; i < childCount; i++) {
						rows[i].setField(fieldIdx, new String(bytes.vector[i], bytes.start[i], bytes.length[i], StandardCharsets.UTF_8));
					}
				}
			}
		} else {
			String[] temp;
			int offset = 0;
			if (bytes.isRepeating) { // fill complete list with first value
				String repeatingValue = new String(bytes.vector[0], bytes.start[0], bytes.length[0], StandardCharsets.UTF_8);
				for (int i = 0; offset < childCount; i++) {
					temp = new String[(int) lengthVector[i]];
					Arrays.fill(temp, repeatingValue);
					offset += temp.length;
					if (fieldIdx == -1) {
						vals[i] = temp;
					} else {
						((Row) vals[i]).setField(fieldIdx, temp);
					}
				}
			} else {
				for (int i = 0; offset < childCount; i++) {
					temp = new String[(int) lengthVector[i]];
					for (int j = 0; j < temp.length; j++) {
						temp[j] = new String(bytes.vector[offset], bytes.start[offset], bytes.length[offset], StandardCharsets.UTF_8);
						offset++;
					}
					if (fieldIdx == -1) {
						vals[i] = temp;
					} else {
						((Row) vals[i]).setField(fieldIdx, temp);
					}
				}
			}
		}
	}

	private static void readNonNullBytesColumnAsBinary(Object[] vals, int fieldIdx, BytesColumnVector bytes, long[] lengthVector, int childCount) {
		// check if the values need to be read into lists or as single values
		if (lengthVector == null) {
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
		} else {
			byte[][] temp;
			int offset = 0;
			if (bytes.isRepeating) { // fill complete list with first value
				for (int i = 0; offset < childCount; i++) {
					temp = new byte[(int) lengthVector[i]][];
					for (int j = 0; j < temp.length; j++) {
						temp[j] = readBinary(bytes.vector[0], bytes.start[0], bytes.length[0]);
					}
					offset += temp.length;
					if (fieldIdx == -1) {
						vals[i] = temp;
					} else {
						((Row) vals[i]).setField(fieldIdx, temp);
					}
				}
			} else {
				for (int i = 0; offset < childCount; i++) {
					temp = new byte[(int) lengthVector[i]][];
					for (int j = 0; j < temp.length; j++) {
						temp[j] = readBinary(bytes.vector[offset], bytes.start[offset], bytes.length[offset]);
						offset++;
					}
					if (fieldIdx == -1) {
						vals[i] = temp;
					} else {
						((Row) vals[i]).setField(fieldIdx, temp);
					}
				}
			}
		}
	}

	private static void readNonNullLongColumnAsDate(Object[] vals, int fieldIdx, LongColumnVector vector, long[] lengthVector, int childCount) {

		// check if the values need to be read into lists or as single values
		if (lengthVector == null) {
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
		} else { // in a list
			Date[] temp;
			int offset = 0;
			if (vector.isRepeating) { // fill complete list with first value
				for (int i = 0; offset < childCount; i++) {
					temp = new Date[(int) lengthVector[i]];
					for (int j = 0; j < temp.length; j++) {
						temp[j] = readDate(vector.vector[0]);
					}
					offset += temp.length;
					if (fieldIdx == -1) {
						vals[i] = temp;
					} else {
						((Row) vals[i]).setField(fieldIdx, temp);
					}
				}
			} else {
				for (int i = 0; offset < childCount; i++) {
					temp = new Date[(int) lengthVector[i]];
					for (int j = 0; j < temp.length; j++) {
						temp[j] = readDate(vector.vector[offset++]);
					}
					if (fieldIdx == -1) {
						vals[i] = temp;
					} else {
						((Row) vals[i]).setField(fieldIdx, temp);
					}
				}
			}
		}
	}

	private static void readNonNullTimestampColumn(Object[] vals, int fieldIdx, TimestampColumnVector vector, long[] lengthVector, int childCount) {

		// check if the timestamps need to be read into lists or as single values
		if (lengthVector == null) {
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
		} else {
			Timestamp[] temp;
			int offset = 0;
			if (vector.isRepeating) { // fill complete list with first value
				for (int i = 0; offset < childCount; i++) {
					temp = new Timestamp[(int) lengthVector[i]];
					for (int j = 0; j < temp.length; j++) {
						// do not reuse value to prevent object mutation
						temp[j] = readTimestamp(vector.time[0], vector.nanos[0]);
					}
					offset += temp.length;
					if (fieldIdx == -1) {
						vals[i] = temp;
					} else {
						((Row) vals[i]).setField(fieldIdx, temp);
					}
				}
			} else {
				for (int i = 0; offset < childCount; i++) {
					temp = new Timestamp[(int) lengthVector[i]];
					for (int j = 0; j < temp.length; j++) {
						temp[j] = readTimestamp(vector.time[offset], vector.nanos[offset]);
						offset++;
					}
					if (fieldIdx == -1) {
						vals[i] = temp;
					} else {
						((Row) vals[i]).setField(fieldIdx, temp);
					}
				}
			}
		}
	}

	private static void readNonNullDecimalColumn(Object[] vals, int fieldIdx, DecimalColumnVector vector, long[] lengthVector, int childCount) {

		// check if the decimals need to be read into lists or as single values
		if (lengthVector == null) {
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
		} else {
			BigDecimal[] temp;
			int offset = 0;
			if (vector.isRepeating) { // fill complete list with first value
				BigDecimal repeatingValue = readBigDecimal(vector.vector[0]);
				for (int i = 0; offset < childCount; i++) {
					temp = new BigDecimal[(int) lengthVector[i]];
					Arrays.fill(temp, repeatingValue);
					offset += temp.length;
					if (fieldIdx == -1) {
						vals[i] = temp;
					} else {
						((Row) vals[i]).setField(fieldIdx, temp);
					}
				}
			} else {
				for (int i = 0; offset < childCount; i++) {
					temp = new BigDecimal[(int) lengthVector[i]];
					for (int j = 0; j < temp.length; j++) {
						temp[j] = readBigDecimal(vector.vector[offset++]);
					}
					if (fieldIdx == -1) {
						vals[i] = temp;
					} else {
						((Row) vals[i]).setField(fieldIdx, temp);
					}
				}
			}
		}

	}

	private static void readNonNullStructColumn(Object[] vals, int fieldIdx, StructColumnVector structVector, TypeDescription schema, long[] lengthVector, int childCount) {

		List<TypeDescription> childrenTypes = schema.getChildren();

		int numFields = childrenTypes.size();
		// create a batch of Rows to read the structs
		Row[] structs = new Row[childCount];
		// TODO: possible improvement: reuse existing Row objects
		for (int i = 0; i < childCount; i++) {
			structs[i] = new Row(numFields);
		}

		// read struct fields
		for (int i = 0; i < numFields; i++) {
			readField(structs, i, childrenTypes.get(i), structVector.fields[i], null, childCount);
		}

		// check if the structs need to be read into lists or as single values
		if (lengthVector == null) {
			if (fieldIdx == -1) { // set struct as an object
				System.arraycopy(structs, 0, vals, 0, childCount);
			} else { // set struct as a field of Row
				Row[] rows = (Row[]) vals;
				for (int i = 0; i < childCount; i++) {
					rows[i].setField(fieldIdx, structs[i]);
				}
			}
		} else { // struct in a list
			int offset = 0;
			Row[] temp;
			for (int i = 0; offset < childCount; i++) {
				temp = new Row[(int) lengthVector[i]];
				System.arraycopy(structs, offset, temp, 0, temp.length);
				offset = offset + temp.length;
				if (fieldIdx == -1) {
					vals[i] = temp;
				} else {
					((Row) vals[i]).setField(fieldIdx, temp);
				}
			}
		}
	}

	private static void readNonNullListColumn(Object[] vals, int fieldIdx, ListColumnVector list, TypeDescription schema, long[] lengthVector, int childCount) {

		TypeDescription fieldType = schema.getChildren().get(0);
		// check if the list need to be read into lists or as single values
		if (lengthVector == null) {
			long[] lengthVectorNested = list.lengths;
			readField(vals, fieldIdx, fieldType, list.child, lengthVectorNested, list.childCount);
		} else { // list in a list
			Object[] nestedLists = new Object[childCount];
			// length vector for nested list
			long[] lengthVectorNested = list.lengths;
			// read nested list
			readField(nestedLists, -1, fieldType, list.child, lengthVectorNested, list.childCount);
			// get type of nestedList
			Class<?> classType = nestedLists[0].getClass();

			// fill outer list with nested list
			int offset = 0;
			int length;
			for (int i = 0; offset < childCount; i++) {
				length = (int) lengthVector[i];
				Object[] temp = (Object[]) Array.newInstance(classType, length);
				System.arraycopy(nestedLists, offset, temp, 0, length);
				offset = offset + length;
				if (fieldIdx == -1) {
					vals[i] = temp;
				} else {
					((Row) vals[i]).setField(fieldIdx, temp);
				}
			}
		}
	}

	private static void readNonNullMapColumn(Object[] vals, int fieldIdx, MapColumnVector mapsVector, TypeDescription schema, long[] lengthVector, int childCount) {

		List<TypeDescription> fieldType = schema.getChildren();
		TypeDescription keyType = fieldType.get(0);
		TypeDescription valueType = fieldType.get(1);

		ColumnVector keys = mapsVector.keys;
		ColumnVector values = mapsVector.values;
		Object[] keyRows = new Object[mapsVector.childCount];
		Object[] valueRows = new Object[mapsVector.childCount];

		// read map keys and values
		readField(keyRows, -1, keyType, keys, null, keyRows.length);
		readField(valueRows, -1, valueType, values, null, valueRows.length);

		// check if the maps need to be read into lists or as single values
		if (lengthVector == null) {
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
		} else { // list of map

			long[] lengthVectorMap = mapsVector.lengths;
			int mapOffset = 0; // offset of map element
			int offset = 0; // offset of map
			HashMap[] temp;

			for (int i = 0; offset < childCount; i++) {
				temp = new HashMap[(int) lengthVector[i]];
				for (int j = 0; j < temp.length; j++) {
					long numMapEntries = lengthVectorMap[offset];
					temp[j] = readHashMap(keyRows, valueRows, mapOffset, numMapEntries);
					mapOffset += numMapEntries;
					offset++;
				}
				if (fieldIdx == 1) {
					vals[i] = temp;
				} else {
					((Row) vals[i]).setField(fieldIdx, temp);
				}
			}
		}
	}

	private static <T> void readLongColumn(Object[] vals, int fieldIdx, LongColumnVector vector, long[] lengthVector, int childCount,
											LongFunction<T> reader, IntFunction<T[]> array) {

		// check if the values need to be read into lists or as single values
		if (lengthVector == null) {
			if (vector.isRepeating) { // fill complete column with first value
				// since the column contains null values and has just one distinct value, the repeated value is null
				fillColumnWithRepeatingValue(vals, fieldIdx, null, childCount);
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
		} else { // in a list
			if (vector.isRepeating) { // // fill complete list with first value
				// since the column contains null values and has just one distinct value, the repeated value is null
				fillListWithRepeatingNull(vals, fieldIdx, lengthVector, childCount, array);
			} else {
				// column contain null values
				int offset = 0;
				T[] temp;
				boolean[] isNullVector = vector.isNull;
				for (int i = 0; offset < childCount; i++) {
					temp = array.apply((int) lengthVector[i]);
					for (int j = 0; j < temp.length; j++) {
						if (isNullVector[offset]) {
							offset++;
						} else {
							temp[j] = reader.apply(vector.vector[offset++]);
						}
					}
					if (fieldIdx == -1) {
						vals[i] = temp;
					} else {
						((Row) vals[i]).setField(fieldIdx, temp);
					}
				}
			}
		}
	}

	private static <T> void readDoubleColumn(Object[] vals, int fieldIdx, DoubleColumnVector vector, long[] lengthVector, int childCount,
												DoubleFunction<T> reader, IntFunction<T[]> array) {

		// check if the values need to be read into lists or as single values
		if (lengthVector == null) {
			if (vector.isRepeating) { // fill complete column with first value
				// since the column contains null values and has just one distinct value, the repeated value is null
				fillColumnWithRepeatingValue(vals, fieldIdx, null, childCount);
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
		} else { // in a list
			if (vector.isRepeating) { // // fill complete list with first value
				// since the column contains null values and has just one distinct value, the repeated value is null
				fillListWithRepeatingNull(vals, fieldIdx, lengthVector, childCount, array);
			} else {
				// column contain null values
				int offset = 0;
				T[] temp;
				boolean[] isNullVector = vector.isNull;
				for (int i = 0; offset < childCount; i++) {
					temp = array.apply((int) lengthVector[i]);
					for (int j = 0; j < temp.length; j++) {
						if (isNullVector[offset]) {
							offset++;
						} else {
							temp[j] = reader.apply(vector.vector[offset++]);
						}
					}
					if (fieldIdx == -1) {
						vals[i] = temp;
					} else {
						((Row) vals[i]).setField(fieldIdx, temp);
					}
				}
			}
		}
	}

	private static void readBytesColumnAsString(Object[] vals, int fieldIdx, BytesColumnVector bytes, long[] lengthVector, int childCount) {

		// check if the values need to be read into lists or as single values
		if (lengthVector == null) {
			if (bytes.isRepeating) { // fill complete column with first value
				// since the column contains null values and has just one distinct value, the repeated value is null
				fillColumnWithRepeatingValue(vals, fieldIdx, null, childCount);
			} else {
				boolean[] isNullVector = bytes.isNull;
				if (fieldIdx == -1) { // set as an object
					for (int i = 0; i < childCount; i++) {
						if (isNullVector[i]) {
							vals[i] = null;
						} else {
							vals[i] = new String(bytes.vector[i], bytes.start[i], bytes.length[i]);
						}
					}
				} else { // set as a field of Row
					Row[] rows = (Row[]) vals;
					for (int i = 0; i < childCount; i++) {
						if (isNullVector[i]) {
							rows[i].setField(fieldIdx, null);
						} else {
							rows[i].setField(fieldIdx, new String(bytes.vector[i], bytes.start[i], bytes.length[i]));
						}
					}
				}
			}
		} else { // in a list
			if (bytes.isRepeating) { // fill list with first value
				// since the column contains null values and has just one distinct value, the repeated value is null
				fillListWithRepeatingNull(vals, fieldIdx, lengthVector, childCount, OrcUtils::stringArray);
			} else {
				int offset = 0;
				String[] temp;
				boolean[] isNullVector = bytes.isNull;
				for (int i = 0; offset < childCount; i++) {
					temp = new String[(int) lengthVector[i]];
					for (int j = 0; j < temp.length; j++) {
						if (isNullVector[offset]) {
							offset++;
						} else {
							temp[j] = new String(bytes.vector[offset], bytes.start[offset], bytes.length[offset]);
							offset++;
						}
					}
					if (fieldIdx == -1) {
						vals[i] = temp;
					} else {
						((Row) vals[i]).setField(fieldIdx, temp);
					}
				}
			}
		}
	}

	private static void readBytesColumnAsBinary(Object[] vals, int fieldIdx, BytesColumnVector bytes, long[] lengthVector, int childCount) {

		// check if the binary need to be read into lists or as single values
		if (lengthVector == null) {
			if (bytes.isRepeating) { // fill complete column with first value
				// since the column contains null values and has just one distinct value, the repeated value is null
				fillColumnWithRepeatingValue(vals, fieldIdx, null, childCount);
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
		} else {
			if (bytes.isRepeating) { // fill complete list with first value
				// since the column contains null values and has just one distinct value, the repeated value is null
				fillListWithRepeatingNull(vals, fieldIdx, lengthVector, childCount, OrcUtils::binaryArray);
			} else {
				int offset = 0;
				byte[][] temp;
				boolean[] isNullVector = bytes.isNull;
				for (int i = 0; offset < childCount; i++) {
					temp = new byte[(int) lengthVector[i]][];
					for (int j = 0; j < temp.length; j++) {
						if (isNullVector[offset]) {
							offset++;
						} else {
							temp[j] = readBinary(bytes.vector[offset], bytes.start[offset], bytes.length[offset]);
							offset++;
						}
					}
					if (fieldIdx == -1) {
						vals[i] = temp;
					} else {
						((Row) vals[i]).setField(fieldIdx, temp);
					}
				}
			}
		}
	}

	private static void readLongColumnAsDate(Object[] vals, int fieldIdx, LongColumnVector vector, long[] lengthVector, int childCount) {

		// check if the values need to be read into lists or as single values
		if (lengthVector == null) {
			if (vector.isRepeating) { // fill complete column with first value
				// since the column contains null values and has just one distinct value, the repeated value is null
				fillColumnWithRepeatingValue(vals, fieldIdx, null, childCount);
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
		} else { // in a list
			if (vector.isRepeating) { // // fill complete list with first value
				// since the column contains null values and has just one distinct value, the repeated value is null
				fillListWithRepeatingNull(vals, fieldIdx, lengthVector, childCount, OrcUtils::dateArray);
			} else {
				// column contain null values
				int offset = 0;
				Date[] temp;
				boolean[] isNullVector = vector.isNull;
				for (int i = 0; offset < childCount; i++) {
					temp = new Date[(int) lengthVector[i]];
					for (int j = 0; j < temp.length; j++) {
						if (isNullVector[offset]) {
							offset++;
						} else {
							temp[j] = readDate(vector.vector[offset++]);
						}
					}
					if (fieldIdx == -1) {
						vals[i] = temp;
					} else {
						((Row) vals[i]).setField(fieldIdx, temp);
					}
				}
			}
		}
	}

	private static void readTimestampColumn(Object[] vals, int fieldIdx, TimestampColumnVector vector, long[] lengthVector, int childCount) {

		// check if the timestamps need to be read into lists or as single values
		if (lengthVector == null) {
			if (vector.isRepeating) { // fill complete column with first value
				// since the column contains null values and has just one distinct value, the repeated value is null
				fillColumnWithRepeatingValue(vals, fieldIdx, null, childCount);
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
		} else {
			if (vector.isRepeating) { // fill complete list with first value
				// since the column contains null values and has just one distinct value, the repeated value is null
				fillListWithRepeatingNull(vals, fieldIdx, lengthVector, childCount, OrcUtils::timestampArray);
			} else {
				int offset = 0;
				Timestamp[] temp;
				boolean[] isNullVector = vector.isNull;
				for (int i = 0; offset < childCount; i++) {
					temp = new Timestamp[(int) lengthVector[i]];
					for (int j = 0; j < temp.length; j++) {
						if (isNullVector[offset]) {
							offset++;
						} else {
							temp[j] = readTimestamp(vector.time[offset], vector.nanos[offset]);
							offset++;
						}
					}
					if (fieldIdx == -1) {
						vals[i] = temp;
					} else {
						((Row) vals[i]).setField(fieldIdx, temp);
					}
				}
			}
		}
	}

	private static void readDecimalColumn(Object[] vals, int fieldIdx, DecimalColumnVector vector, long[] lengthVector, int childCount) {

		// check if the decimals need to be read into lists or as single values
		if (lengthVector == null) {
			if (vector.isRepeating) { // fill complete column with first value
				// since the column contains null values and has just one distinct value, the repeated value is null
				fillColumnWithRepeatingValue(vals, fieldIdx, null, childCount);
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
		} else {
			if (vector.isRepeating) { // fill complete list with first value
				// since the column contains null values and has just one distinct value, the repeated value is null
				fillListWithRepeatingNull(vals, fieldIdx, lengthVector, childCount, OrcUtils::decimalArray);
			} else {
				int offset = 0;
				BigDecimal[] temp;
				boolean[] isNullVector = vector.isNull;
				for (int i = 0; offset < childCount; i++) {
					temp = new BigDecimal[(int) lengthVector[i]];
					for (int j = 0; j < temp.length; j++) {
						if (isNullVector[offset]) {
							offset++;
						} else {
							temp[j] = readBigDecimal(vector.vector[offset++]);
						}
					}
					if (fieldIdx == -1) {
						vals[i] = temp;
					} else {
						((Row) vals[i]).setField(fieldIdx, temp);
					}
				}
			}
		}
	}

	private static void readStructColumn(Object[] vals, int fieldIdx, StructColumnVector structVector, TypeDescription schema, long[] lengthVector, int childCount) {

		List<TypeDescription> childrenTypes = schema.getChildren();

		int numFields = childrenTypes.size();
		// create a batch of Rows to read the structs
		Row[] structs = new Row[childCount];
		// TODO: possible improvement: reuse existing Row objects
		for (int i = 0; i < childCount; i++) {
			structs[i] = new Row(numFields);
		}

		// read struct fields
		for (int i = 0; i < numFields; i++) {
			readField(structs, i, childrenTypes.get(i), structVector.fields[i], null, childCount);
		}

		boolean[] isNullVector = structVector.isNull;

		// check if the structs need to be read into lists or as single values
		if (lengthVector == null) {
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
		} else { // struct in a list
			int offset = 0;
			Row[] temp;
			for (int i = 0; offset < childCount; i++) {
				temp = new Row[(int) lengthVector[i]];
				for (int j = 0; j < temp.length; j++) {
					if (isNullVector[offset]) {
						temp[j] = null;
					} else {
						temp[j] = structs[offset++];
					}
				}
				if (fieldIdx == -1) { // set list of structs as an object
					vals[i] = temp;
				} else { // set list of structs as field of row
					((Row) vals[i]).setField(fieldIdx, temp);
				}
			}
		}
	}

	private static void readListColumn(Object[] vals, int fieldIdx, ListColumnVector list, TypeDescription schema, long[] lengthVector, int childCount) {

		TypeDescription fieldType = schema.getChildren().get(0);
		// check if the lists need to be read into lists or as single values
		if (lengthVector == null) {
			long[] lengthVectorNested = list.lengths;
			readField(vals, fieldIdx, fieldType, list.child, lengthVectorNested, list.childCount);
		} else { // list in a list
			Object[] nestedList = new Object[childCount];
			// length vector for nested list
			long[] lengthVectorNested = list.lengths;
			// read nested list
			readField(nestedList, -1, fieldType, list.child, lengthVectorNested, list.childCount);

			// fill outer list with nested list
			int offset = 0;
			int length;
			// get type of nestedList
			Class<?> classType = nestedList[0].getClass();
			for (int i = 0; offset < childCount; i++) {
				length = (int) lengthVector[i];
				Object[] temp = (Object[]) Array.newInstance(classType, length);
				System.arraycopy(nestedList, offset, temp, 0, length);
				offset = offset + length;
				if (fieldIdx == -1) { // set list of list as an object
					vals[i] = temp;
				} else { // set list of list as field of row
					((Row) vals[i]).setField(fieldIdx, temp);
				}
			}
		}
	}

	private static void readMapColumn(Object[] vals, int fieldIdx, MapColumnVector map, TypeDescription schema, long[] lengthVector, int childCount) {

		List<TypeDescription> fieldType = schema.getChildren();
		TypeDescription keyType = fieldType.get(0);
		TypeDescription valueType = fieldType.get(1);

		ColumnVector keys = map.keys;
		ColumnVector values = map.values;
		Object[] keyRows = new Object[map.childCount];
		Object[] valueRows = new Object[map.childCount];

		// read map kes and values
		readField(keyRows, -1, keyType, keys, null, keyRows.length);
		readField(valueRows, -1, valueType, values, null, valueRows.length);

		boolean[] isNullVector = map.isNull;

		// check if the maps need to be read into lists or as single values
		if (lengthVector == null) {
			long[] lengthVectorMap = map.lengths;
			int offset = 0;
			if (fieldIdx == -1) { // set map as an object
				for (int i = 0; i < childCount; i++) {
					if (isNullVector[i]) {
						vals[i] = null;
					} else {
						vals[i] = readHashMap(keyRows, valueRows, offset, lengthVectorMap[i]);
						offset += lengthVectorMap[i];
					}
				}
			} else { // set map as a field of Row
				Row[] rows = (Row[]) vals;
				for (int i = 0; i < childCount; i++) {
					if (isNullVector[i]) {
						rows[i].setField(fieldIdx, null);
					} else {
						rows[i].setField(fieldIdx, readHashMap(keyRows, valueRows, offset, lengthVectorMap[i]));
						offset += lengthVectorMap[i];
					}
				}
			}
		} else { // list of map
			long[] lengthVectorMap = map.lengths;
			int mapOffset = 0; // offset of map element
			int offset = 0; // offset of map
			HashMap[] temp;

			for (int i = 0; offset < childCount; i++) {
				temp = new HashMap[(int) lengthVector[i]];
				for (int j = 0; j < temp.length; j++) {
					if (isNullVector[offset]) {
						temp[j] = null;
					} else {
						temp[j] = readHashMap(keyRows, valueRows, mapOffset, lengthVectorMap[offset]);
						mapOffset += lengthVectorMap[offset];
						offset++;
					}
				}
				if (fieldIdx == -1) {
					vals[i] = temp;
				} else {
					((Row) vals[i]).setField(fieldIdx, temp);
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

	/**
	 * Sets arrays containing only null values to all objects or row fields of the passed vals array.
	 *
	 * @param vals The array of objects or Rows to which the empty arrays are set.
	 * @param fieldIdx If the objs array is an array of Row, the index of the field that needs to be filled.
	 *                 Otherwise a -1 must be passed and the data is directly filled into the array.
	 * @param lengthVector The vector containing the lengths of the individual empty arrays.
	 * @param childCount The number of objects or Rows to fill.
	 * @param array A method to create arrays of the appropriate type.
	 * @param <T> The type of the arrays to create.
	 */
	private static <T> void fillListWithRepeatingNull(Object[] vals, int fieldIdx, long[] lengthVector, int childCount, IntFunction<T[]> array) {

		if (fieldIdx == -1) {
			// set empty array as object
			for (int i = 0; i < childCount; i++) {
				vals[i] = array.apply((int) lengthVector[i]);
			}
		} else {
			// set empty array as field in Row
			Row[] rows = (Row[]) vals;
			for (int i = 0; i < childCount; i++) {
				rows[i].setField(fieldIdx, array.apply((int) lengthVector[i]));
			}
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

	private static Boolean[] boolArray(int len) {
		return new Boolean[len];
	}

	private static Byte[] byteArray(int len) {
		return new Byte[len];
	}

	private static Short[] shortArray(int len) {
		return new Short[len];
	}

	private static Integer[] intArray(int len) {
		return new Integer[len];
	}

	private static Long[] longArray(int len) {
		return new Long[len];
	}

	private static Float[] floatArray(int len) {
		return new Float[len];
	}

	private static Double[] doubleArray(int len) {
		return new Double[len];
	}

	private static Date[] dateArray(int len) {
		return new Date[len];
	}

	private static byte[][] binaryArray(int len) {
		return new byte[len][];
	}

	private static String[] stringArray(int len) {
		return new String[len];
	}

	private static BigDecimal[] decimalArray(int len) {
		return new BigDecimal[len];
	}

	private static Timestamp[] timestampArray(int len) {
		return new Timestamp[len];
	}

}
