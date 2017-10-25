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
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/**
 * A class that provides utility methods for orc file reading.
 */
public class OrcUtils {

	/**
	 * Convert ORC schema types to Flink types.
	 *
	 * @param schema schema of orc file
	 *
	 */
	public static TypeInformation schemaToTypeInfo(TypeDescription schema) {
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
				TypeInformation elementType = schemaToTypeInfo(elementSchema);
				return ObjectArrayTypeInfo.getInfoFor(elementType);
			case MAP:
				TypeDescription keySchema = schema.getChildren().get(0);
				TypeDescription valSchema = schema.getChildren().get(1);
				TypeInformation keyType = schemaToTypeInfo(keySchema);
				TypeInformation valType = schemaToTypeInfo(valSchema);
				return new MapTypeInfo(keyType, valType);
			case DECIMAL:
				return BasicTypeInfo.BIG_DEC_TYPE_INFO;
			case UNION:
				throw new UnsupportedOperationException("UNION type not supported yet.");
			default:
				throw new IllegalArgumentException("Unknown type " + schema);
		}
	}

	/**
	 * Fill rows from orc batch.
	 *
	 * @param rows the batch of rows need to be filled
	 * @param schema schema of orc file
	 * @param batch current orc batch data used to fill the rows
	 * @param fieldMapping field mapping
	 *
	 */
	public static void fillRows(Object[] rows, TypeDescription schema, VectorizedRowBatch batch, int[] fieldMapping) {

		int totalRowsInBatch = (int) batch.count();

		List<TypeDescription> fieldTypes = schema.getChildren();
		for (int outIdx = 0; outIdx < fieldMapping.length; outIdx++) {
			int inIdx = fieldMapping[outIdx];
			readField(rows, outIdx, fieldTypes.get(inIdx), batch.cols[inIdx], null, Math.min((int) totalRowsInBatch, rows.length));
		}
	}

	private static void readField(Object[] rows, int fieldIdx, TypeDescription schema, ColumnVector vector, long[] lengthVector, int childCount) {

		switch (schema.getCategory()) {
			case BOOLEAN:
				if (vector.noNulls) {
					readNonNullBooleanColumn(rows, fieldIdx, (LongColumnVector) vector, lengthVector, childCount);
				} else {
					readBooleanColumn(rows, fieldIdx, (LongColumnVector) vector, lengthVector, childCount);
				}
				break;
			case BYTE:
				if (vector.noNulls) {
					readNonNullByteColumn(rows, fieldIdx, (LongColumnVector) vector, lengthVector, childCount);
				} else {
					readByteColumn(rows, fieldIdx, (LongColumnVector) vector, lengthVector, childCount);
				}
				break;
			case SHORT:
				if (vector.noNulls) {
					readNonNullShortColumn(rows, fieldIdx, (LongColumnVector) vector, lengthVector, childCount);
				} else {
					readShortColumn(rows, fieldIdx, (LongColumnVector) vector, lengthVector, childCount);
				}
				break;
			case INT:
				if (vector.noNulls) {
					readNonNullIntColumn(rows, fieldIdx, (LongColumnVector) vector, lengthVector, childCount);
				} else {
					readIntColumn(rows, fieldIdx, (LongColumnVector) vector, lengthVector, childCount);
				}
				break;
			case LONG:
				if (vector.noNulls) {
					readNonNullLongColumn(rows, fieldIdx, (LongColumnVector) vector, lengthVector, childCount);
				} else {
					readLongColumn(rows, fieldIdx, (LongColumnVector) vector, lengthVector, childCount);
				}
				break;
			case FLOAT:
				if (vector.noNulls) {
					readNonNullFloatColumn(rows, fieldIdx, (DoubleColumnVector) vector, lengthVector, childCount);
				} else {
					readFloatColumn(rows, fieldIdx, (DoubleColumnVector) vector, lengthVector, childCount);
				}
				break;
			case DOUBLE:
				if (vector.noNulls) {
					readNonNullDoubleColumn(rows, fieldIdx, (DoubleColumnVector) vector, lengthVector, childCount);
				} else {
					readDoubleColumn(rows, fieldIdx, (DoubleColumnVector) vector, lengthVector, childCount);
				}
				break;
			case CHAR:
			case VARCHAR:
			case STRING:
				if (vector.noNulls) {
					readNonNullStringColumn(rows, fieldIdx, (BytesColumnVector) vector, lengthVector, childCount);
				} else {
					readStringColumn(rows, fieldIdx, (BytesColumnVector) vector, lengthVector, childCount);
				}
				break;
			case DATE:
				if (vector.noNulls) {
					readNonNullDateColumn(rows, fieldIdx, (LongColumnVector) vector, lengthVector, childCount);
				} else {
					readDateColumn(rows, fieldIdx, (LongColumnVector) vector, lengthVector, childCount);
				}
				break;
			case TIMESTAMP:
				if (vector.noNulls) {
					readNonNullTimestampColumn(rows, fieldIdx, (TimestampColumnVector) vector, lengthVector, childCount);
				} else {
					readTimestampColumn(rows, fieldIdx, (TimestampColumnVector) vector, lengthVector, childCount);
				}
				break;
			case BINARY:
				if (vector.noNulls) {
					readNonNullBinaryColumn(rows, fieldIdx, (BytesColumnVector) vector, lengthVector, childCount);
				} else {
					readBinaryColumn(rows, fieldIdx, (BytesColumnVector) vector, lengthVector, childCount);
				}
				break;
			case DECIMAL:
				if (vector.noNulls) {
					readNonNullDecimalColumn(rows, fieldIdx, (DecimalColumnVector) vector, lengthVector, childCount);
				}
				else {
					readDecimalColumn(rows, fieldIdx, (DecimalColumnVector) vector, lengthVector, childCount);
				}
				break;
			case STRUCT:
				if (vector.noNulls) {
					readNonNullStructColumn(rows, fieldIdx, (StructColumnVector) vector, schema, lengthVector, childCount);
				} else {
					readStructColumn(rows, fieldIdx, (StructColumnVector) vector, schema, lengthVector, childCount);
				}
				break;
			case LIST:
				if (vector.noNulls) {
					readNonNullListColumn(rows, fieldIdx, (ListColumnVector) vector, schema, lengthVector, childCount);
				}
				else {
					readListColumn(rows, fieldIdx, (ListColumnVector) vector, schema, lengthVector, childCount);
				}
				break;
			case MAP:
				if (vector.noNulls) {
					readNonNullMapColumn(rows, fieldIdx, (MapColumnVector) vector, schema, lengthVector, childCount);
				}
				else {
					readMapColumn(rows, fieldIdx, (MapColumnVector) vector, schema, lengthVector, childCount);
				}
				break;
			case UNION:
				throw new UnsupportedOperationException("UNION type not supported yet");
			default:
				throw new IllegalArgumentException("Unknown type " + schema);
		}
	}

	private static void readNonNullBooleanColumn(Object[] rows, int fieldIdx, LongColumnVector vector, long[] lengthVector, int childCount) {

		// check if boolean is directly in a list or not, e.g, array<boolean>
		if (lengthVector == null) {
			if (vector.isRepeating) { // fill complete column with first value
				boolean repeatingValue = vector.vector[0] != 0;
				fillColumnWithRepeatingValue(rows, fieldIdx, repeatingValue, childCount);
			} else {
				if (fieldIdx == -1) { // set as an object
					for (int i = 0; i < childCount; i++) {
						rows[i] = vector.vector[i] != 0;
					}
				} else { // set as a field of Row
					for (int i = 0; i < childCount; i++) {
						((Row) rows[i]).setField(fieldIdx, vector.vector[i] != 0);
					}
				}
			}
		} else { // in a list
			boolean[] temp;
			int offset = 0;
			if (vector.isRepeating) { // fill complete list with first value
				boolean repeatingValue = vector.vector[0] != 0;
				if (fieldIdx == -1) { // set list as an object
					for (int i = 0; offset < childCount; i++) {
						temp = new boolean[(int) lengthVector[i]];
						Arrays.fill(temp, repeatingValue);
						rows[i] = temp;
						offset += temp.length;
					}
				} else { // set list as a field of Row
					for (int i = 0; offset < childCount; i++) {
						temp = new boolean[(int) lengthVector[i]];
						Arrays.fill(temp, repeatingValue);
						((Row) rows[i]).setField(fieldIdx, temp);
						offset += temp.length;
					}
				}
			} else {
				if (fieldIdx == -1) { // set list as an object
					for (int i = 0; offset < childCount; i++) {
						temp = new boolean[(int) lengthVector[i]];
						for (int j = 0; j < temp.length; j++) {
							temp[j] = vector.vector[offset++] != 0;
						}
						rows[i] = temp;
					}
				} else { // set list as a field of Row
					for (int i = 0; offset < childCount; i++) {
						temp = new boolean[(int) lengthVector[i]];
						for (int j = 0; j < temp.length; j++) {
							temp[j] = vector.vector[offset++] != 0;
						}
						((Row) rows[i]).setField(fieldIdx, temp);
					}
				}
			}
		}
	}

	private static void readNonNullByteColumn(Object[] rows, int fieldIdx, LongColumnVector vector, long[] lengthVector, int childCount) {

		// check if byte is directly in a list or not, e.g, array<byte>
		if (lengthVector == null) {
			if (vector.isRepeating) { // fill complete column with first value
				byte repeatingValue = (byte) vector.vector[0];
				fillColumnWithRepeatingValue(rows, fieldIdx, repeatingValue, childCount);
			} else {
				if (fieldIdx == -1) { // set as an object
					for (int i = 0; i < childCount; i++) {
						rows[i] = (byte) vector.vector[i];
					}
				} else { // set as a field of Row
					for (int i = 0; i < childCount; i++) {
						((Row) rows[i]).setField(fieldIdx, (byte) vector.vector[i]);
					}
				}
			}
		} else { // in a list
			byte[] temp;
			int offset = 0;
			if (vector.isRepeating) { // fill complete list with first value
				byte repeatingValue = (byte) vector.vector[0];
				if (fieldIdx == -1) { // set list as an object
					for (int i = 0; offset < childCount; i++) {
						temp = new byte[(int) lengthVector[i]];
						Arrays.fill(temp, repeatingValue);
						rows[i] = temp;
						offset += temp.length;
					}
				} else { // set list as a field of Row
					for (int i = 0; offset < childCount; i++) {
						temp = new byte[(int) lengthVector[i]];
						Arrays.fill(temp, repeatingValue);
						((Row) rows[i]).setField(fieldIdx, temp);
						offset += temp.length;
					}
				}
			} else {
				if (fieldIdx == -1) { // set list as an object
					for (int i = 0; offset < childCount; i++) {
						temp = new byte[(int) lengthVector[i]];
						for (int j = 0; j < temp.length; j++) {
							temp[j] = (byte) vector.vector[offset++];
						}
						rows[i] = temp;
					}
				} else { // set list as a field of Row
					for (int i = 0; offset < childCount; i++) {
						temp = new byte[(int) lengthVector[i]];
						for (int j = 0; j < temp.length; j++) {
							temp[j] = (byte) vector.vector[offset++];
						}
						((Row) rows[i]).setField(fieldIdx, temp);
					}
				}
			}
		}
	}

	private static void readNonNullShortColumn(Object[] rows, int fieldIdx, LongColumnVector vector, long[] lengthVector, int childCount) {

		// check if short is directly in a list or not, e.g, array<short>
		if (lengthVector == null) {
			if (vector.isRepeating) { // fill complete column with first value
				short repeatingValue = (short) vector.vector[0];
				fillColumnWithRepeatingValue(rows, fieldIdx, repeatingValue, childCount);
			} else {
				if (fieldIdx == -1) { // set as an object
					for (int i = 0; i < childCount; i++) {
						rows[i] = (short) vector.vector[i];
					}
				} else { // set as a field of Row
					for (int i = 0; i < childCount; i++) {
						((Row) rows[i]).setField(fieldIdx, (short) vector.vector[i]);
					}
				}
			}
		} else { // in a list
			short[] temp;
			int offset = 0;
			if (vector.isRepeating) { // fill complete list with first value
				short repeatingValue = (short) vector.vector[0];
				if (fieldIdx == -1) { // set list as an object
					for (int i = 0; offset < childCount; i++) {
						temp = new short[(int) lengthVector[i]];
						Arrays.fill(temp, repeatingValue);
						rows[i] = temp;
						offset += temp.length;
					}
				} else { // set list as a field of Row
					for (int i = 0; offset < childCount; i++) {
						temp = new short[(int) lengthVector[i]];
						Arrays.fill(temp, repeatingValue);
						((Row) rows[i]).setField(fieldIdx, temp);
						offset += temp.length;
					}
				}
			} else {
				if (fieldIdx == -1) { // set list as an object
					for (int i = 0; offset < childCount; i++) {
						temp = new short[(int) lengthVector[i]];
						for (int j = 0; j < temp.length; j++) {
							temp[j] = (short) vector.vector[offset++];
						}
						rows[i] = temp;
					}
				} else { // set list as a field of Row
					for (int i = 0; offset < childCount; i++) {
						temp = new short[(int) lengthVector[i]];
						for (int j = 0; j < temp.length; j++) {
							temp[j] = (short) vector.vector[offset++];
						}
						((Row) rows[i]).setField(fieldIdx, temp);
					}
				}
			}
		}
	}

	private static void readNonNullIntColumn(Object[] rows, int fieldIdx, LongColumnVector vector, long[] lengthVector, int childCount) {

		// check if int is directly in a list or not, e.g, array<int>
		if (lengthVector == null) {
			if (vector.isRepeating) { // fill complete column with first value
				int repeatingValue = (int) vector.vector[0];
				fillColumnWithRepeatingValue(rows, fieldIdx, repeatingValue, childCount);
			} else {
				if (fieldIdx == -1) { // set as an object
					for (int i = 0; i < childCount; i++) {
						rows[i] = (int) vector.vector[i];
					}
				} else { // set as a field of Row
					for (int i = 0; i < childCount; i++) {
						((Row) rows[i]).setField(fieldIdx, (int) vector.vector[i]);
					}
				}
			}
		} else { // in a list
			int[] temp;
			int offset = 0;
			if (vector.isRepeating) { // fill complete list with first value
				int repeatingValue = (int) vector.vector[0];
				if (fieldIdx == -1) { // set list as an object
					for (int i = 0; offset < childCount; i++) {
						temp = new int[(int) lengthVector[i]];
						Arrays.fill(temp, repeatingValue);
						rows[i] = temp;
						offset += temp.length;
					}
				} else { // set list as a field of Row
					for (int i = 0; offset < childCount; i++) {
						temp = new int[(int) lengthVector[i]];
						Arrays.fill(temp, repeatingValue);
						((Row) rows[i]).setField(fieldIdx, temp);
						offset += temp.length;
					}
				}
			} else {
				if (fieldIdx == -1) { // set list as an object
					for (int i = 0; offset < childCount; i++) {
						temp = new int[(int) lengthVector[i]];
						for (int j = 0; j < temp.length; j++) {
							temp[j] = (int) vector.vector[offset++];
						}
						rows[i] = temp;
					}
				} else { // set list as a field of Row
					for (int i = 0; offset < childCount; i++) {
						temp = new int[(int) lengthVector[i]];
						for (int j = 0; j < temp.length; j++) {
							temp[j] = (int) vector.vector[offset++];
						}
						((Row) rows[i]).setField(fieldIdx, temp);
					}
				}
			}
		}
	}

	private static void readNonNullLongColumn(Object[] rows, int fieldIdx, LongColumnVector vector, long[] lengthVector, int childCount) {

		// check if long is directly in a list or not, e.g, array<long>
		if (lengthVector == null) {
			if (vector.isRepeating) { // fill complete column with first value
				long repeatingValue = vector.vector[0];
				fillColumnWithRepeatingValue(rows, fieldIdx, repeatingValue, childCount);
			} else {
				if (fieldIdx == -1) { // set as an object
					for (int i = 0; i < childCount; i++) {
						rows[i] = vector.vector[i];
					}
				} else { // set as a field of Row
					for (int i = 0; i < childCount; i++) {
						((Row) rows[i]).setField(fieldIdx, (Long) vector.vector[i]);
					}
				}
			}
		} else { // in a list
			long[] temp;
			int offset = 0;
			if (vector.isRepeating) { // fill complete list with first value
				long repeatingValue = vector.vector[0];
				if (fieldIdx == -1) { // set list as an object
					for (int i = 0; offset < childCount; i++) {
						temp = new long[(int) lengthVector[i]];
						Arrays.fill(temp, repeatingValue);
						rows[i] = temp;
						offset += temp.length;
					}
				} else { // set list as a field of Row
					for (int i = 0; offset < childCount; i++) {
						temp = new long[(int) lengthVector[i]];
						Arrays.fill(temp, repeatingValue);
						((Row) rows[i]).setField(fieldIdx, temp);
						offset += temp.length;
					}
				}
			} else {
				if (fieldIdx == -1) { // set list as an object
					for (int i = 0; offset < childCount; i++) {
						temp = new long[(int) lengthVector[i]];
						for (int j = 0; j < temp.length; j++) {
							temp[j] = vector.vector[offset++];
						}
						rows[i] = temp;
					}
				} else { // set list as a field of Row
					for (int i = 0; offset < childCount; i++) {
						temp = new long[(int) lengthVector[i]];
						for (int j = 0; j < temp.length; j++) {
							temp[j] = vector.vector[offset++];
						}
						((Row) rows[i]).setField(fieldIdx, temp);
					}
				}
			}
		}
	}

	private static void readNonNullFloatColumn(Object[] rows, int fieldIdx, DoubleColumnVector vector, long[] lengthVector, int childCount) {

		// check if float is directly in a list or not, e.g, array<float>
		if (lengthVector == null) {
			if (vector.isRepeating) { // fill complete column with first value
				float repeatingValue = (float) vector.vector[0];
				fillColumnWithRepeatingValue(rows, fieldIdx, repeatingValue, childCount);
			} else {
				if (fieldIdx == -1) { // set as an object
					for (int i = 0; i < childCount; i++) {
						rows[i] = (float) vector.vector[i];
					}
				} else { // set as a field of Row
					for (int i = 0; i < childCount; i++) {
						((Row) rows[i]).setField(fieldIdx, (float) vector.vector[i]);
					}
				}
			}
		} else { // in a list
			float[] temp;
			int offset = 0;
			if (vector.isRepeating) { // fill complete list with first value
				float repeatingValue = (float) vector.vector[0];
				if (fieldIdx == -1) { // set list as an object
					for (int i = 0; offset < childCount; i++) {
						temp = new float[(int) lengthVector[i]];
						Arrays.fill(temp, repeatingValue);
						rows[i] = temp;
						offset += temp.length;
					}
				} else { // set list as a field of Row
					for (int i = 0; offset < childCount; i++) {
						temp = new float[(int) lengthVector[i]];
						Arrays.fill(temp, repeatingValue);
						((Row) rows[i]).setField(fieldIdx, temp);
						offset += temp.length;
					}
				}
			} else {
				if (fieldIdx == -1) { // set list as an object
					for (int i = 0; offset < childCount; i++) {
						temp = new float[(int) lengthVector[i]];
						for (int j = 0; j < temp.length; j++) {
							temp[j] = (float) vector.vector[offset++];
						}
						rows[i] = temp;
					}
				} else { // set list as a field of Row
					for (int i = 0; offset < childCount; i++) {
						temp = new float[(int) lengthVector[i]];
						for (int j = 0; j < temp.length; j++) {
							temp[j] = (float) vector.vector[offset++];
						}
						((Row) rows[i]).setField(fieldIdx, temp);
					}
				}
			}
		}
	}

	private static void readNonNullDoubleColumn(Object[] rows, int fieldIdx, DoubleColumnVector vector, long[] lengthVector, int childCount) {

		// check if double is directly in a list or not, e.g, array<double>
		if (lengthVector == null) {
			if (vector.isRepeating) { // fill complete column with first value
				double repeatingValue = vector.vector[0];
				fillColumnWithRepeatingValue(rows, fieldIdx, repeatingValue, childCount);
			} else {
				if (fieldIdx == -1) { // set as an object
					for (int i = 0; i < childCount; i++) {
						rows[i] = vector.vector[i];
					}
				} else { // set as a field of Row
					for (int i = 0; i < childCount; i++) {
						((Row) rows[i]).setField(fieldIdx, vector.vector[i]);
					}
				}
			}
		} else { // in a list
			double[] temp;
			int offset = 0;
			if (vector.isRepeating) { // fill complete list with first value
				double repeatingValue = vector.vector[0];
				if (fieldIdx == -1) { // set list as an object
					for (int i = 0; offset < childCount; i++) {
						temp = new double[(int) lengthVector[i]];
						Arrays.fill(temp, repeatingValue);
						rows[i] = temp;
						offset += temp.length;
					}
				} else { // set list as a field of Row
					for (int i = 0; offset < childCount; i++) {
						temp = new double[(int) lengthVector[i]];
						Arrays.fill(temp, repeatingValue);
						((Row) rows[i]).setField(fieldIdx, temp);
						offset += temp.length;
					}
				}
			} else {
				if (fieldIdx == -1) { // set list as an object
					for (int i = 0; offset < childCount; i++) {
						temp = new double[(int) lengthVector[i]];
						for (int j = 0; j < temp.length; j++) {
							temp[j] = vector.vector[offset++];
						}
						rows[i] = temp;
					}
				} else { // set list as a field of Row
					for (int i = 0; offset < childCount; i++) {
						temp = new double[(int) lengthVector[i]];
						for (int j = 0; j < temp.length; j++) {
							temp[j] = vector.vector[offset++];
						}
						((Row) rows[i]).setField(fieldIdx, temp);
					}
				}
			}
		}
	}

	private static void readNonNullStringColumn(Object[] rows, int fieldIdx, BytesColumnVector bytes, long[] lengthVector, int childCount) {

		// check if string is directly in a list or not, e.g, array<string>
		if (lengthVector == null) {
			if (bytes.isRepeating) { // fill complete column with first value
				String repeatingValue = new String(bytes.vector[0], bytes.start[0], bytes.length[0]);
				fillColumnWithRepeatingValue(rows, fieldIdx, repeatingValue, childCount);
			} else {
				if (fieldIdx == -1) { // set as an object
					for (int i = 0; i < childCount; i++) {
						rows[i] = new String(bytes.vector[i], bytes.start[i], bytes.length[i]);
					}
				} else { // set as a field of Row
					for (int i = 0; i < childCount; i++) {
						((Row) rows[i]).setField(fieldIdx, new String(bytes.vector[i], bytes.start[i], bytes.length[i]));
					}
				}
			}
		}
		else { // in a list
			String[] temp;
			int offset = 0;
			if (bytes.isRepeating) { // fill list with first value
				String repeatingValue = new String(bytes.vector[0], bytes.start[0], bytes.length[0]);
				if (fieldIdx == -1) { // set list as an object
					for (int i = 0; i < childCount; i++) {
						temp = new String[(int) lengthVector[i]];
						Arrays.fill(temp, repeatingValue);
						rows[i] = temp;
						offset += temp.length;
					}
				} else { // set list as a field
					for (int i = 0; i < childCount; i++) {
						temp = new String[(int) lengthVector[i]];
						Arrays.fill(temp, repeatingValue);
						((Row) rows[i]).setField(fieldIdx, temp);
						offset += temp.length;
					}
				}
			} else {
				if (fieldIdx == -1) { // set list as an object
					for (int i = 0; offset < childCount; i++) {
						temp = new String[(int) lengthVector[i]];
						for (int j = 0; j < temp.length; j++) {
							temp[j] = new String(bytes.vector[offset], bytes.start[offset], bytes.length[offset]);
							offset++;
						}
						rows[i] = temp;
					}
				} else { // set list as a field
					for (int i = 0; offset < childCount; i++) {
						temp = new String[(int) lengthVector[i]];
						for (int j = 0; j < temp.length; j++) {
							temp[j] = new String(bytes.vector[offset], bytes.start[offset], bytes.length[offset]);
							offset++;
						}
						((Row) rows[i]).setField(fieldIdx, temp);
					}
				}
			}
		}

	}

	private static void readNonNullDateColumn(Object[] rows, int fieldIdx, LongColumnVector vector, long[] lengthVector, int childCount) {

		// check if date is directly in a list or not, e.g, array<date>
		if (lengthVector == null) {
			if (vector.isRepeating) { // fill complete column with first value
				if (fieldIdx == -1) { // set as an object
					for (int i = 0; i < childCount; i++) {
						rows[i] = readDate(vector.vector[0]);
					}
				} else { // set as a field of Row
					for (int i = 0; i < childCount; i++) {
						((Row) rows[i]).setField(fieldIdx, readDate(vector.vector[0]));
					}
				}
			} else {
				if (fieldIdx == -1) { // set as an object
					for (int i = 0; i < childCount; i++) {
						rows[i] = readDate(vector.vector[i]);
					}
				} else { // set as a field of Row
					for (int i = 0; i < childCount; i++) {
						((Row) rows[i]).setField(fieldIdx, readDate(vector.vector[i]));
					}
				}
			}
		} else {
			Date[] temp;
			int offset = 0;
			if (vector.isRepeating) { // fill complete list with first value
				if (fieldIdx == -1) { // set list as an object
					for (int i = 0; offset < childCount; i++) {
						temp = new Date[(int) lengthVector[i]];
						for (int j = 0; j < temp.length; j++) {
							temp[j] = readDate(vector.vector[0]);
						}
						rows[i] = temp;
						offset += temp.length;
					}
				} else { // set list as a field of Row
					for (int i = 0; offset < childCount; i++) {
						temp = new Date[(int) lengthVector[i]];
						for (int j = 0; j < temp.length; j++) {
							temp[j] = readDate(vector.vector[0]);
						}
						((Row) rows[i]).setField(fieldIdx, temp);
						offset += temp.length;
					}
				}
			} else {
				if (fieldIdx == -1) { // set list as an object
					for (int i = 0; offset < childCount; i++) {
						temp = new Date[(int) lengthVector[i]];
						for (int j = 0; j < temp.length; j++) {
							temp[j] = readDate(vector.vector[offset++]);
						}
						rows[i] = temp;
					}
				} else { // set list as a field of Row
					for (int i = 0; offset < childCount; i++) {
						temp = new Date[(int) lengthVector[i]];
						for (int j = 0; j < temp.length; j++) {
							temp[j] = readDate(vector.vector[offset++]);
						}
						((Row) rows[i]).setField(fieldIdx, temp);
					}
				}
			}
		}
	}

	private static void readNonNullTimestampColumn(Object[] rows, int fieldIdx, TimestampColumnVector vector, long[] lengthVector, int childCount) {

		// check if timestamp is directly in a list or not, e.g, array<timestamp>
		if (lengthVector == null) {
			if (vector.isRepeating) { // fill complete column with first value
				if (fieldIdx == -1) { // set as an object
					for (int i = 0; i < childCount; i++) {
						rows[i] = readTimeStamp(vector.time[0], vector.nanos[0]);
					}
				} else { // set as a field of Row
					for (int i = 0; i < childCount; i++) {
						((Row) rows[i]).setField(fieldIdx, readTimeStamp(vector.time[0], vector.nanos[0]));
					}
				}
			} else {
				if (fieldIdx == -1) { // set as an object
					for (int i = 0; i < childCount; i++) {
						rows[i] = readTimeStamp(vector.time[i], vector.nanos[i]);
					}
				} else { // set as a field of Row
					for (int i = 0; i < childCount; i++) {
						((Row) rows[i]).setField(fieldIdx, readTimeStamp(vector.time[i], vector.nanos[i]));
					}
				}
			}
		} else {
			Timestamp[] temp;
			int offset = 0;
			if (vector.isRepeating) { // fill complete list with first value
				if (fieldIdx == -1) { // set list as an object
					for (int i = 0; offset < childCount; i++) {
						temp = new Timestamp[(int) lengthVector[i]];
						for (int j = 0; j < temp.length; j++) {
							temp[j] = readTimeStamp(vector.time[0], vector.nanos[0]);
						}
						rows[i] = temp;
						offset += temp.length;
					}
				} else { // set list as a field of Row
					for (int i = 0; offset < childCount; i++) {
						temp = new Timestamp[(int) lengthVector[i]];
						for (int j = 0; j < temp.length; j++) {
							temp[j] = readTimeStamp(vector.time[0], vector.nanos[0]);
						}
						((Row) rows[i]).setField(fieldIdx, temp);
						offset += temp.length;
					}
				}
			} else {
				if (fieldIdx == -1) { // set list as an object
					for (int i = 0; offset < childCount; i++) {
						temp = new Timestamp[(int) lengthVector[i]];
						for (int j = 0; j < temp.length; j++) {
							temp[j] = readTimeStamp(vector.time[offset], vector.nanos[offset]);
							offset++;
						}
						rows[i] = temp;
					}
				} else { // set list as a field of Row
					for (int i = 0; offset < childCount; i++) {
						temp = new Timestamp[(int) lengthVector[i]];
						for (int j = 0; j < temp.length; j++) {
							temp[j] = readTimeStamp(vector.time[offset], vector.nanos[offset]);
							offset++;
						}
						((Row) rows[i]).setField(fieldIdx, temp);
					}
				}
			}
		}
	}

	private static void readNonNullBinaryColumn(Object[] rows, int fieldIdx, BytesColumnVector bytes, long[] lengthVector, int childCount) {

		// check if string is directly in a list or not, e.g, array<string>
		if (lengthVector == null) {
			if (bytes.isRepeating) { // fill complete column with first value
				if (fieldIdx == -1) { // set as an object
					for (int i = 0; i < childCount; i++) {
						rows[i] = readBinary(bytes.vector[0], bytes.start[0], bytes.length[0]);
					}
				} else { // set as a field of Row
					for (int i = 0; i < childCount; i++) {
						((Row) rows[i]).setField(fieldIdx, readBinary(bytes.vector[0], bytes.start[0], bytes.length[0]));
					}
				}
			} else {
				if (fieldIdx == -1) { // set as an object
					for (int i = 0; i < childCount; i++) {
						rows[i] = readBinary(bytes.vector[i], bytes.start[i], bytes.length[i]);
					}
				} else { // set as a field of Row
					for (int i = 0; i < childCount; i++) {
						((Row) rows[i]).setField(fieldIdx, readBinary(bytes.vector[i], bytes.start[i], bytes.length[i]));
					}
				}
			}
		} else {
			byte[][] temp;
			int offset = 0;
			if (bytes.isRepeating) { // fill complete list with first value
				if (fieldIdx == -1) { // set list as an object
					for (int i = 0; offset < childCount; i++) {
						temp = new byte[(int) lengthVector[i]][];
						for (int j = 0; j < temp.length; j++) {
							temp[j] = readBinary(bytes.vector[0], bytes.start[0], bytes.length[0]);
						}
						rows[i] = temp;
						offset += temp.length;
					}
				} else { // set list as a field
					for (int i = 0; offset < childCount; i++) {
						temp = new byte[(int) lengthVector[i]][];
						for (int j = 0; j < temp.length; j++) {
							temp[j] = readBinary(bytes.vector[0], bytes.start[0], bytes.length[0]);
						}
						((Row) rows[i]).setField(fieldIdx, temp);
						offset += temp.length;
					}
				}
			} else {
				if (fieldIdx == -1) { // set list as an object
					for (int i = 0; offset < childCount; i++) {
						temp = new byte[(int) lengthVector[i]][];
						for (int j = 0; j < temp.length; j++) {
							temp[j] = readBinary(bytes.vector[offset], bytes.start[offset], bytes.length[offset]);
							offset++;
						}
						rows[i] = temp;
					}
				} else { // set list as a field
					for (int i = 0; offset < childCount; i++) {
						temp = new byte[(int) lengthVector[i]][];
						for (int j = 0; j < temp.length; j++) {
							temp[j] = readBinary(bytes.vector[offset], bytes.start[offset], bytes.length[offset]);
							offset++;
						}
						((Row) rows[i]).setField(fieldIdx, temp);
					}
				}
			}
		}

	}

	private static void readNonNullDecimalColumn(Object[] rows, int fieldIdx, DecimalColumnVector vector, long[] lengthVector, int childCount) {

		// check if decimal is directly in a list or not, e.g, array<decimal>
		if (lengthVector == null) {
			if (vector.isRepeating) { // fill complete column with first value
				fillColumnWithRepeatingValue(rows, fieldIdx, readBigDecimal(vector.vector[0]), childCount);
			} else {
				if (fieldIdx == -1) { // set as an object
					for (int i = 0; i < childCount; i++) {
						rows[i] = readBigDecimal(vector.vector[i]);
					}
				} else { // set as a field of Row
					for (int i = 0; i < childCount; i++) {
						((Row) rows[i]).setField(fieldIdx, readBigDecimal(vector.vector[i]));
					}
				}
			}
		} else {
			BigDecimal[] temp;
			int offset = 0;
			if (vector.isRepeating) { // fill complete list with first value
				BigDecimal repeatingValue = readBigDecimal(vector.vector[0]);
				if (fieldIdx == -1) { // set list as an object
					for (int i = 0; offset < childCount; i++) {
						temp = new BigDecimal[(int) lengthVector[i]];
						Arrays.fill(temp, repeatingValue);
						rows[i] = temp;
						offset += temp.length;
					}
				} else { // set list as a field of Row
					for (int i = 0; offset < childCount; i++) {
						temp = new BigDecimal[(int) lengthVector[i]];
						Arrays.fill(temp, repeatingValue);
						((Row) rows[i]).setField(fieldIdx, temp);
						offset += temp.length;
					}
				}
			} else {
				if (fieldIdx == -1) { // set list as an object
					for (int i = 0; offset < childCount; i++) {
						temp = new BigDecimal[(int) lengthVector[i]];
						for (int j = 0; j < temp.length; j++) {
							temp[j] = readBigDecimal(vector.vector[offset++]);
						}
						rows[i] = temp;
					}
				} else { // set list as a field of Row
					for (int i = 0; offset < childCount; i++) {
						temp = new BigDecimal[(int) lengthVector[i]];
						for (int j = 0; j < temp.length; j++) {
							temp[j] = readBigDecimal(vector.vector[offset++]);
						}
						((Row) rows[i]).setField(fieldIdx, temp);
					}
				}
			}
		}

	}

	private static void readNonNullStructColumn(Object[] rows, int fieldIdx, StructColumnVector struct, TypeDescription schema, long[] lengthVector, int childCount) {

		List<TypeDescription> childrenTypes = schema.getChildren();

		int numChildren = childrenTypes.size();
		Row[] nestedFields = new Row[childCount];
		for (int i = 0; i < childCount; i++) {
			nestedFields[i] = new Row(numChildren);
		}
		for (int i = 0; i < numChildren; i++) {
			readField(nestedFields, i, childrenTypes.get(i), struct.fields[i], null, childCount);
		}

		// check if struct is directly in a list or not, e.g, array<struct<dt>>
		if (lengthVector == null) {
			if (fieldIdx == -1) { // set struct as an object
				System.arraycopy(nestedFields, 0, rows, 0, childCount);
			}
			else { // set struct as a field of Row
				for (int i = 0; i < childCount; i++) {
					((Row) rows[i]).setField(fieldIdx, nestedFields[i]);
				}
			}
		}
		else { // struct in a list
			int offset = 0;
			Row[] temp;
			if (fieldIdx == -1) { // set list of struct as an object
				for (int i = 0; offset < childCount; i++) {
					temp = new Row[(int) lengthVector[i]];
					System.arraycopy(nestedFields, offset, temp, 0, temp.length);
					offset = offset + temp.length;
					rows[i] = temp;
				}
			}
			else { // set list of struct as a field of Row
				for (int i = 0; offset < childCount; i++) {
					temp = new Row[(int) lengthVector[i]];
					System.arraycopy(nestedFields, offset, temp, 0, temp.length);
					offset = offset + temp.length;
					((Row) rows[i]).setField(fieldIdx, temp);
				}
			}
		}
	}

	private static void readNonNullListColumn(Object[] rows, int fieldIdx, ListColumnVector list, TypeDescription schema, long[] lengthVector, int childCount) {

		TypeDescription fieldType = schema.getChildren().get(0);
		if (lengthVector == null) {
			long[] lengthVectorNested = list.lengths;
			readField(rows, fieldIdx, fieldType, list.child, lengthVectorNested, list.childCount);
		}
		else { // list in a list

			Object[] nestedList = new Object[childCount];

			// length vector for nested list
			long[] lengthVectorNested = list.lengths;

			// read nested list
			readField(nestedList, -1, fieldType, list.child, lengthVectorNested, list.childCount);

			// get type of nestedList
			Class<?> classType = nestedList[0].getClass();

			// fill outer list with nested list
			int offset = 0;
			int length;
			if (fieldIdx == -1) { // set list of list as an object
				for (int i = 0; offset < childCount; i++) {
					length = (int) lengthVector[i];
					Object temp = Array.newInstance(classType, length);
					System.arraycopy(nestedList, offset, temp, 0, length);
					offset = offset + length;
					rows[i] = temp;

				}
			} else { // set list of list as an field on Row
				for (int i = 0; offset < childCount; i++) {
					length = (int) lengthVector[i];
					Object temp = Array.newInstance(classType, length);
					System.arraycopy(nestedList, offset, temp, 0, length);
					offset = offset + length;
					((Row) rows[i]).setField(fieldIdx, temp);
				}
			}
		}

	}

	private static void readNonNullMapColumn(Object[] rows, int fieldIdx, MapColumnVector map, TypeDescription schema, long[] lengthVector, int childCount) {

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

		// check if map is directly in a list or not, e.g, array<map<k,v>>
		if (lengthVector == null) {
			long[] lengthVectorMap = map.lengths;
			int offset = 0;
			if (fieldIdx == -1) {
				for (int i = 0; i < childCount; i++) {
					rows[i] = readHashMap(keyRows, valueRows, offset, lengthVectorMap[i]);
					offset += lengthVectorMap[i];
				}
			} else {
				for (int i = 0; i < childCount; i++) {
					((Row) rows[i]).setField(fieldIdx, readHashMap(keyRows, valueRows, offset, lengthVectorMap[i]));
					offset += lengthVectorMap[i];
				}
			}
		} else { // list of map

			long[] lengthVectorMap = map.lengths;
			int mapOffset = 0; // offset of map element
			int offset = 0; // offset of map
			HashMap[] temp;
			if (fieldIdx == -1) { // set map list as an object
				for (int i = 0; offset < childCount; i++) {
					temp = new HashMap[(int) lengthVector[i]];
					for (int j = 0; j < temp.length; j++) {
						temp[j] = readHashMap(keyRows, valueRows, mapOffset, lengthVectorMap[offset]);
						mapOffset += lengthVectorMap[offset];
						offset++;
					}
					rows[i] = temp;
				}
			} else { // set map list as a field of Row
				for (int i = 0; offset < childCount; i++) {
					temp = new HashMap[(int) lengthVector[i]];
					for (int j = 0; j < temp.length; j++) {
						temp[j] = readHashMap(keyRows, valueRows, mapOffset, lengthVectorMap[offset]);
						mapOffset += lengthVectorMap[offset];
						offset++;
					}
					((Row) rows[i]).setField(fieldIdx, temp);
				}
			}
		}
	}

	private static void fillColumnWithRepeatingValue(Object[] rows, int fieldIdx, Object repeatingValue, int childCount) {

		if (fieldIdx == -1) { // set as an object
			for (int i = 0; i < childCount; i++) {
				rows[i] = repeatingValue;
			}
		} else { // set as a field of Row
			for (int i = 0; i < childCount; i++) {
				((Row) rows[i]).setField(fieldIdx, repeatingValue);
			}
		}
	}

	private static void fillListWithRepeatingNull(Object[] rows, int fieldIdx, Class<?> classType, long[] lengthVector, int childCount) {

		int length;
		if (fieldIdx == -1) {
			for (int i = 0; i < childCount; i++) {
				length = (int) lengthVector[i];
				Object temp = Array.newInstance(classType, length);
				rows[i] = temp;
			}
		} else {
			for (int i = 0; i < childCount; i++) {
				length = (int) lengthVector[i];
				Object temp = Array.newInstance(classType, length);
				((Row) rows[i]).setField(fieldIdx, temp);
			}
		}
	}

	private static void readBooleanColumn(Object[] rows, int fieldIdx, LongColumnVector vector, long[] lengthVector, int childCount) {

		// check if data type(dt) is directly in list or not, e.g, array<dt>
		if (lengthVector == null) {
			if (vector.isRepeating) { // fill complete column with first value
				// Also column contains null value and it's repeating
				fillColumnWithRepeatingValue(rows, fieldIdx, null, childCount);
			} else {
				boolean[] isNullVector = vector.isNull;
				if (fieldIdx == -1) { // set as an object
					for (int i = 0; i < childCount; i++) {
						if (isNullVector[i]) {
							rows[i] = null;
							continue;
						}
						rows[i] = vector.vector[i] != 0;
					}
				} else { // set as a field of Row
					for (int i = 0; i < childCount; i++) {
						if (isNullVector[i]) {
							((Row) rows[i]).setField(fieldIdx, null);
							continue;
						}
						((Row) rows[i]).setField(fieldIdx, vector.vector[i] != 0);
					}
				}
			}
		} else { // in a list
			if (vector.isRepeating) { // // fill complete list with first value
				// Also column contains null value and it's repeating
				// so all values are null, but we need to set list with null values
				fillListWithRepeatingNull(rows, fieldIdx, boolean[].class, lengthVector, childCount);
			} else {
				// column contain null values
				int offset = 0;
				boolean[] temp;
				boolean[] isNullVector = vector.isNull;
				if (fieldIdx == -1) { // set list as an object
					for (int i = 0; offset < childCount; i++) {
						temp = new boolean[(int) lengthVector[i]];
						for (int j = 0; j < temp.length; j++) {
							if (isNullVector[offset]) {
								offset++;
								continue;
							}
							temp[j] = vector.vector[offset++] != 0;
						}
						rows[i] = temp;
					}
				} else { // set list as a field of Row
					for (int i = 0; offset < childCount; i++) {
						temp = new boolean[(int) lengthVector[i]];
						for (int j = 0; j < temp.length; j++) {
							if (isNullVector[offset]) {
								offset++;
								continue;
							}
							temp[j] = vector.vector[offset++] != 0;
						}
						((Row) rows[i]).setField(fieldIdx, temp);
					}
				}
			}
		}
	}

	private static void readByteColumn(Object[] rows, int fieldIdx, LongColumnVector vector, long[] lengthVector, int childCount) {

		// check if data type(dt) is directly in list or not, e.g, array<dt>
		if (lengthVector == null) {
			if (vector.isRepeating) { // fill complete column with first value
				// Also column contains null value and it's repeating
				fillColumnWithRepeatingValue(rows, fieldIdx, null, childCount);
			} else {
				boolean[] isNullVector = vector.isNull;
				if (fieldIdx == -1) { // set as an object
					for (int i = 0; i < childCount; i++) {
						if (isNullVector[i]) {
							rows[i] = null;
							continue;
						}
						rows[i] = (byte) vector.vector[i];
					}
				} else { // set as a field of Row
					for (int i = 0; i < childCount; i++) {
						if (isNullVector[i]) {
							((Row) rows[i]).setField(fieldIdx, null);
							continue;
						}
						((Row) rows[i]).setField(fieldIdx, (byte) vector.vector[i]);
					}
				}
			}
		} else { // in a list
			if (vector.isRepeating) { // // fill complete list with first value
				// Also column contains null value and it's repeating
				// so all values are null, but we need to set list with null values
				fillListWithRepeatingNull(rows, fieldIdx, byte[].class, lengthVector, childCount);
			} else {
				// column contain null values
				int offset = 0;
				byte[] temp;
				boolean[] isNullVector = vector.isNull;
				if (fieldIdx == -1) { // set list as an object
					for (int i = 0; offset < childCount; i++) {
						temp = new byte[(int) lengthVector[i]];
						for (int j = 0; j < temp.length; j++) {
							if (isNullVector[offset]) {
								offset++;
								continue;
							}
							temp[j] = (byte) vector.vector[offset++];
						}
						rows[i] = temp;
					}
				} else { // set list as a field of Row
					for (int i = 0; offset < childCount; i++) {
						temp = new byte[(int) lengthVector[i]];
						for (int j = 0; j < temp.length; j++) {
							if (isNullVector[offset]) {
								offset++;
								continue;
							}
							temp[j] = (byte) vector.vector[offset++];
						}
						((Row) rows[i]).setField(fieldIdx, temp);
					}
				}
			}
		}
	}

	private static void readShortColumn(Object[] rows, int fieldIdx, LongColumnVector vector, long[] lengthVector, int childCount) {

		// check if data type(dt) is directly in list or not, e.g, array<dt>
		if (lengthVector == null) {
			if (vector.isRepeating) { // fill complete column with first value
				// Also column contains null value and it's repeating
				fillColumnWithRepeatingValue(rows, fieldIdx, null, childCount);
			} else {
				boolean[] isNullVector = vector.isNull;
				if (fieldIdx == -1) { // set as an object
					for (int i = 0; i < childCount; i++) {
						if (isNullVector[i]) {
							rows[i] = null;
							continue;
						}
						rows[i] = (short) vector.vector[i];
					}
				} else { // set as field of Row
					for (int i = 0; i < childCount; i++) {
						if (isNullVector[i]) {
							((Row) rows[i]).setField(fieldIdx, null);
							continue;
						}
						((Row) rows[i]).setField(fieldIdx, (short) vector.vector[i]);
					}
				}
			}
		} else { // in a list
			if (vector.isRepeating) { // // fill complete list with first value
				// Also column contains null value and it's repeating
				// so all values are null, but we need to set list with null values
				fillListWithRepeatingNull(rows, fieldIdx, short[].class, lengthVector, childCount);
			} else {
				// column contain null values
				int offset = 0;
				short[] temp;
				boolean[] isNullVector = vector.isNull;
				if (fieldIdx == -1) { // set list as an object
					for (int i = 0; offset < childCount; i++) {
						temp = new short[(int) lengthVector[i]];
						for (int j = 0; j < temp.length; j++) {
							if (isNullVector[offset]) {
								offset++;
								continue;
							}
							temp[j] = (short) vector.vector[offset++];
						}
						rows[i] = temp;
					}
				} else { // set list as a field of Row
					for (int i = 0; offset < childCount; i++) {
						temp = new short[(int) lengthVector[i]];
						for (int j = 0; j < temp.length; j++) {
							if (isNullVector[offset]) {
								offset++;
								continue;
							}
							temp[j] = (short) vector.vector[offset++];
						}
						((Row) rows[i]).setField(fieldIdx, temp);
					}
				}
			}
		}
	}

	private static void readIntColumn(Object[] rows, int fieldIdx, LongColumnVector vector, long[] lengthVector, int childCount) {

		// check if data type(dt) is directly in list or not, e.g, array<dt>
		if (lengthVector == null) {
			if (vector.isRepeating) { // fill complete column with first value
				// Also column contains null value and it's repeating
				fillColumnWithRepeatingValue(rows, fieldIdx, null, childCount);
			} else {
				boolean[] isNullVector = vector.isNull;
				if (fieldIdx == -1) { // set as an object
					for (int i = 0; i < childCount; i++) {
						if (isNullVector[i]) {
							rows[i] = null;
							continue;
						}
						rows[i] = (int) vector.vector[i];
					}
				} else { // set as a field of Row
					for (int i = 0; i < childCount; i++) {
						if (isNullVector[i]) {
							((Row) rows[i]).setField(fieldIdx, null);
							continue;
						}
						((Row) rows[i]).setField(fieldIdx, (int) vector.vector[i]);
					}
				}
			}
		} else { // in a list
			if (vector.isRepeating) { // // fill complete list with first value
				// Also column contains null value and it's repeating
				// so all values are null, but we need to set list with null values
				fillListWithRepeatingNull(rows, fieldIdx, int[].class, lengthVector, childCount);
			} else {
				// column contain null values
				int offset = 0;
				int[] temp;
				boolean[] isNullVector = vector.isNull;
				if (fieldIdx == -1) { // set list as an object
					for (int i = 0; offset < childCount; i++) {
						temp = new int[(int) lengthVector[i]];
						for (int j = 0; j < temp.length; j++) {
							if (isNullVector[offset]) {
								offset++;
								continue;
							}
							temp[j] = (int) vector.vector[offset++];
						}
						rows[i] = temp;
					}
				} else { // set list as a field of Row
					for (int i = 0; offset < childCount; i++) {
						temp = new int[(int) lengthVector[i]];
						for (int j = 0; j < temp.length; j++) {
							if (isNullVector[offset]) {
								offset++;
								continue;
							}
							temp[j] = (int) vector.vector[offset++];
						}
						((Row) rows[i]).setField(fieldIdx, temp);
					}
				}
			}
		}
	}

	private static void readLongColumn(Object[] rows, int fieldIdx, LongColumnVector vector, long[] lengthVector, int childCount) {

		// check if data type(dt) is directly in list or not, e.g, array<dt>
		if (lengthVector == null) {
			if (vector.isRepeating) { // fill complete column with first value
				// Also column contains null value and it's repeating
				fillColumnWithRepeatingValue(rows, fieldIdx, null, childCount);
			} else {
				boolean[] isNullVector = vector.isNull;
				if (fieldIdx == -1) { // set as an object
					for (int i = 0; i < childCount; i++) {
						if (isNullVector[i]) {
							rows[i] = null;
							continue;
						}
						rows[i] = vector.vector[i];
					}
				} else { // set as a field of Row
					for (int i = 0; i < childCount; i++) {
						if (isNullVector[i]) {
							((Row) rows[i]).setField(fieldIdx, null);
							continue;
						}
						((Row) rows[i]).setField(fieldIdx, vector.vector[i]);
					}
				}
			}
		} else { // in a list
			if (vector.isRepeating) { // // fill complete list with first value
				// Also column contains null value and it's repeating
				// so all values are null, but we need to set list with null values
				fillListWithRepeatingNull(rows, fieldIdx, long[].class, lengthVector, childCount);
			} else {
				// column contain null values
				int offset = 0;
				long[] temp;
				boolean[] isNullVector = vector.isNull;
				if (fieldIdx == -1) { // set list as an object
					for (int i = 0; offset < childCount; i++) {
						temp = new long[(int) lengthVector[i]];
						for (int j = 0; j < temp.length; j++) {
							if (isNullVector[offset]) {
								offset++;
								continue;
							}
							temp[j] = vector.vector[offset++];
						}
						rows[i] = temp;
					}
				} else { // set list as a field of Row
					for (int i = 0; offset < childCount; i++) {
						temp = new long[(int) lengthVector[i]];
						for (int j = 0; j < temp.length; j++) {
							if (isNullVector[offset]) {
								offset++;
								continue;
							}
							temp[j] = vector.vector[offset++];
						}
						((Row) rows[i]).setField(fieldIdx, temp);
					}
				}
			}
		}
	}

	private static void readFloatColumn(Object[] rows, int fieldIdx, DoubleColumnVector vector, long[] lengthVector, int childCount) {

		// check if data type(dt) is directly in list or not, e.g, array<dt>
		if (lengthVector == null) {
			if (vector.isRepeating) { // fill complete column with first value
				// Also column contains null value and it's repeating
				fillColumnWithRepeatingValue(rows, fieldIdx, null, childCount);
			} else {
				boolean[] isNullVector = vector.isNull;
				if (fieldIdx == -1) { // set as an object
					for (int i = 0; i < childCount; i++) {
						if (isNullVector[i]) {
							rows[i] = null;
							continue;
						}
						rows[i] = (float) vector.vector[i];
					}
				} else { // set as a field of Row
					for (int i = 0; i < childCount; i++) {
						if (isNullVector[i]) {
							((Row) rows[i]).setField(fieldIdx, null);
							continue;
						}
						((Row) rows[i]).setField(fieldIdx, (float) vector.vector[i]);
					}
				}
			}
		} else { // in a list
			if (vector.isRepeating) { // // fill complete list with first value
				// Also column contains null value and it's repeating
				// so all values are null, but we need to set list with null values
				fillListWithRepeatingNull(rows, fieldIdx, float[].class, lengthVector, childCount);
			} else {
				// column contain null values
				int offset = 0;
				float[] temp;
				boolean[] isNullVector = vector.isNull;
				if (fieldIdx == -1) { // set list as an object
					for (int i = 0; offset < childCount; i++) {
						temp = new float[(int) lengthVector[i]];
						for (int j = 0; j < temp.length; j++) {
							if (isNullVector[offset]) {
								offset++;
								continue;
							}
							temp[j] = (float) vector.vector[offset++];
						}
						rows[i] = temp;
					}
				} else { // set list as a field of Row
					for (int i = 0; i < childCount; i++) {
						temp = new float[(int) lengthVector[i]];
						for (int j = 0; j < temp.length; j++) {
							if (isNullVector[offset]) {
								offset++;
								continue;
							}
							temp[j] = (float) vector.vector[offset++];
						}
						((Row) rows[i]).setField(fieldIdx, temp);
					}
				}
			}
		}
	}

	private static void readDoubleColumn(Object[] rows, int fieldIdx, DoubleColumnVector vector, long[] lengthVector, int childCount) {

		// check if data type(dt) is directly in list or not, e.g, array<dt>
		if (lengthVector == null) {
			if (vector.isRepeating) { // fill complete column with first value
				// Also column contains null value and it's repeating
				fillColumnWithRepeatingValue(rows, fieldIdx, null, childCount);
			} else {
				boolean[] isNullVector = vector.isNull;
				if (fieldIdx == -1) { // set as an object
					for (int i = 0; i < childCount; i++) {
						if (isNullVector[i]) {
							rows[i] = null;
							continue;
						}
						rows[i] = vector.vector[i];
					}
				} else { // set as field of Row
					for (int i = 0; i < childCount; i++) {
						if (isNullVector[i]) {
							((Row) rows[i]).setField(fieldIdx, null);
							continue;
						}
						((Row) rows[i]).setField(fieldIdx, vector.vector[i]);
					}
				}
			}
		} else { // in a list
			if (vector.isRepeating) { // // fill complete list with first value
				// Also column contains null value and it's repeating
				// so all values are null, but we need to set list with null values
				fillListWithRepeatingNull(rows, fieldIdx, double[].class, lengthVector, childCount);
			} else {
				// column contain null values
				int offset = 0;
				double[] temp;
				boolean[] isNullVector = vector.isNull;
				if (fieldIdx == -1) { // set list as an object
					for (int i = 0; offset < childCount; i++) {
						temp = new double[(int) lengthVector[i]];
						for (int j = 0; j < temp.length; j++) {
							if (isNullVector[offset]) {
								offset++;
								continue;
							}
							temp[j] = vector.vector[offset++];
						}
						rows[i] = temp;
					}
				} else { // set list as a field of Row
					for (int i = 0; offset < childCount; i++) {
						temp = new double[(int) lengthVector[i]];
						for (int j = 0; j < temp.length; j++) {
							if (isNullVector[offset]) {
								offset++;
								continue;
							}
							temp[j] = vector.vector[offset++];
						}
						((Row) rows[i]).setField(fieldIdx, temp);
					}
				}
			}
		}
	}

	private static void readStringColumn(Object[] rows, int fieldIdx, BytesColumnVector bytes, long[] lengthVector, int childCount) {

		// check if string is directly in a list or not, e.g, array<string>
		if (lengthVector == null) {
			if (bytes.isRepeating) { // fill complete column with first value
				// Also column contains null value and it's repeating
				fillColumnWithRepeatingValue(rows, fieldIdx, null, childCount);
			} else {
				boolean[] isNullVector = bytes.isNull;
				if (fieldIdx == -1) { // set as an object
					for (int i = 0; i < childCount; i++) {
						if (isNullVector[i]) {
							rows[i] = null;
							continue;
						}
						rows[i] = new String(bytes.vector[i], bytes.start[i], bytes.length[i]);
					}
				} else { // set as a field of Row
					for (int i = 0; i < childCount; i++) {
						if (isNullVector[i]) {
							((Row) rows[i]).setField(fieldIdx, null);
							continue;
						}
						((Row) rows[i]).setField(fieldIdx, new String(bytes.vector[i], bytes.start[i], bytes.length[i]));
					}
				}
			}
		} else { // in a list
			if (bytes.isRepeating) { // fill list with first value
				// Also column contains null value and it's repeating
				// so all values are null, but we need to set list with null values
				fillListWithRepeatingNull(rows, fieldIdx, String[].class, lengthVector, childCount);
			} else {
				int offset = 0;
				String[] temp;
				boolean[] isNullVector = bytes.isNull;
				if (fieldIdx == -1) { // set list as an object
					for (int i = 0; offset < childCount; i++) {
						temp = new String[(int) lengthVector[i]];
						for (int j = 0; j < temp.length; j++) {
							if (isNullVector[offset]) {
								offset++;
								temp[j] = null;
								continue; // skip null value
							}
							temp[j] = new String(bytes.vector[offset], bytes.start[offset], bytes.length[offset]);
							offset++;
						}
						rows[i] = temp;
					}
				} else { // set list as a field
					for (int i = 0; offset < childCount; i++) {
						temp = new String[(int) lengthVector[i]];
						for (int j = 0; j < temp.length; j++) {
							if (isNullVector[offset]) {
								offset++;
								temp[j] = null;
								continue; // skip null value
							}
							temp[j] = new String(bytes.vector[offset], bytes.start[offset], bytes.length[offset]);
							offset++;
						}
						((Row) rows[i]).setField(fieldIdx, temp);
					}
				}
			}
		}

	}

	private static void readDateColumn(Object[] rows, int fieldIdx, LongColumnVector vector, long[] lengthVector, int childCount) {

		// check if date is directly in a list or not, e.g, array<date>
		if (lengthVector == null) {
			if (vector.isRepeating) { // fill complete column with first value
				// Also column contains null value and it's repeating
				fillColumnWithRepeatingValue(rows, fieldIdx, null, childCount);
			} else {
				boolean[] isNullVector = vector.isNull;
				if (fieldIdx == -1) { // set as an object
					for (int i = 0; i < childCount; i++) {
						if (isNullVector[i]) {
							rows[i] = null;
							continue;
						}
						rows[i] = readDate(vector.vector[i]);
					}
				} else { // set as a field of Row
					for (int i = 0; i < childCount; i++) {
						if (isNullVector[i]) {
							((Row) rows[i]).setField(fieldIdx, null);
							continue;
						}
						((Row) rows[i]).setField(fieldIdx, readDate(vector.vector[i]));
					}
				}
			}
		} else {
			if (vector.isRepeating) { // fill complete list with first value
				// Also column contains null value and it's repeating
				// so all values are null, but we need to set list with null values
				fillListWithRepeatingNull(rows, fieldIdx, Date[].class, lengthVector, childCount);
			} else {
				int offset = 0;
				Date[] temp;
				boolean[] isNullVector = vector.isNull;
				if (fieldIdx == -1) { // set list as an object
					for (int i = 0; offset < childCount; i++) {
						temp = new Date[(int) lengthVector[i]];
						for (int j = 0; j < temp.length; j++) {
							if (isNullVector[offset]) {
								offset++;
								temp[j] = null;
								continue;
							}
							temp[j] = readDate(vector.vector[offset++]);
						}
						rows[i] = temp;
					}
				} else { // set list as a field of Row
					for (int i = 0; offset < childCount; i++) {
						temp = new Date[(int) lengthVector[i]];
						for (int j = 0; j < temp.length; j++) {
							if (isNullVector[offset]) {
								offset++;
								temp[j] = null;
								continue;
							}
							temp[j] = readDate(vector.vector[offset++]);
						}
						((Row) rows[i]).setField(fieldIdx, temp);
					}
				}
			}
		}

	}

	private static void readTimestampColumn(Object[] rows, int fieldIdx, TimestampColumnVector vector, long[] lengthVector, int childCount) {

		// check if timestamp is directly in a list or not, e.g, array<timestamp>
		if (lengthVector == null) {
			if (vector.isRepeating) { // fill complete column with first value
				// Also column contains null value and it's repeating
				fillColumnWithRepeatingValue(rows, fieldIdx, null, childCount);
			} else {
				boolean[] isNullVector = vector.isNull;
				if (fieldIdx == -1) { // set as an object
					for (int i = 0; i < childCount; i++) {
						if (isNullVector[i]) {
							rows[i] = null;
							continue;
						}
						Timestamp ts = new Timestamp(vector.time[i]);
						ts.setNanos(vector.nanos[i]);
						rows[i] = ts;
					}
				} else { // set as a field of Row
					for (int i = 0; i < childCount; i++) {
						if (isNullVector[i]) {
							((Row) rows[i]).setField(fieldIdx, null);
							continue;
						}
						Timestamp ts = new Timestamp(vector.time[i]);
						ts.setNanos(vector.nanos[i]);
						((Row) rows[i]).setField(fieldIdx, ts);
					}
				}
			}
		}
		else {
			if (vector.isRepeating) { // fill complete list with first value
				// Also column contains null value and it's repeating
				// so all values are null, but we need to set list with null values
				fillListWithRepeatingNull(rows, fieldIdx, Timestamp[].class, lengthVector, childCount);
			} else {
				int offset = 0;
				Timestamp[] temp;
				boolean[] isNullVector = vector.isNull;
				if (fieldIdx == -1) { // set list as an object
					for (int i = 0; offset < childCount; i++) {
						temp = new Timestamp[(int) lengthVector[i]];
						for (int j = 0; j < temp.length; j++) {
							if (isNullVector[offset]) {
								offset++;
								temp[j] = null;
								continue;
							}
							temp[j] = new Timestamp(vector.time[offset]);
							temp[j].setNanos(vector.nanos[offset]);
							offset++;
						}
						rows[i] = temp;
					}
				} else { // set list as a field of Row
					for (int i = 0; offset < childCount; i++) {
						temp = new Timestamp[(int) lengthVector[i]];
						for (int j = 0; j < temp.length; j++) {
							if (isNullVector[offset]) {
								offset++;
								temp[j] = null;
								continue;
							}
							temp[j] = new Timestamp(vector.time[offset]);
							temp[j].setNanos(vector.nanos[offset]);
							offset++;
						}
						((Row) rows[i]).setField(fieldIdx, temp);
					}
				}
			}
		}
	}

	private static void readBinaryColumn(Object[] rows, int fieldIdx, BytesColumnVector bytes, long[] lengthVector, int childCount) {

		// check if string is directly in a list or not, e.g, array<string>
		if (lengthVector == null) {
			if (bytes.isRepeating) { // fill complete column with first value
				// Also column contains null value and it's repeating
				fillColumnWithRepeatingValue(rows, fieldIdx, null, childCount);
			} else {
				boolean[] isNullVectorIndex = bytes.isNull;
				if (fieldIdx == -1) { // set as an object
					for (int i = 0; i < childCount; i++) {
						if (isNullVectorIndex[i]) {
							rows[i] = null;
							continue;
						}
						rows[i] = readBinary(bytes.vector[i], bytes.start[i], bytes.length[i]);
					}
				} else { // set as a field of Row
					for (int i = 0; i < childCount; i++) {
						if (isNullVectorIndex[i]) {
							((Row) rows[i]).setField(fieldIdx, null);
							continue;
						}
						((Row) rows[i]).setField(fieldIdx, readBinary(bytes.vector[i], bytes.start[i], bytes.length[i]));
					}
				}
			}
		} else {
			if (bytes.isRepeating) { // fill complete list with first value
				// Also column contains null value and it's repeating
				// so all values are null, but we need to set list with null values
				fillListWithRepeatingNull(rows, fieldIdx, byte[][].class, lengthVector, childCount);
			} else {
				int offset = 0;
				byte[][] temp;
				boolean[] isNullVector = bytes.isNull;
				if (fieldIdx == -1) { // set list as an object
					for (int i = 0; offset < childCount; i++) {
						temp = new byte[(int) lengthVector[i]][];
						for (int j = 0; j < temp.length; j++) {
							if (isNullVector[offset]) {
								offset++;
								temp[j] = null;
								continue;
							}
							temp[j] = readBinary(bytes.vector[offset], bytes.start[offset], bytes.length[offset]);
							offset++;
						}
						rows[i] = temp;
					}
				} else { // set list as a field
					for (int i = 0; offset < childCount; i++) {
						temp = new byte[(int) lengthVector[i]][];
						for (int j = 0; j < temp.length; j++) {
							if (isNullVector[offset]) {
								offset++;
								temp[j] = null;
								continue;
							}
							temp[j] = readBinary(bytes.vector[offset], bytes.start[offset], bytes.length[offset]);
							offset++;
						}
						((Row) rows[i]).setField(fieldIdx, temp);
					}
				}
			}
		}
	}

	private static void readDecimalColumn(Object[] rows, int fieldIdx, DecimalColumnVector vector, long[] lengthVector, int childCount) {

		// check if decimal is directly in a list or not, e.g, array<decimal>
		if (lengthVector == null) {
			if (vector.isRepeating) { // fill complete column with first value
				// Also column contains null value and it's repeating
				fillColumnWithRepeatingValue(rows, fieldIdx, null, childCount);
			} else {
				boolean[] isNullVector = vector.isNull;
				if (fieldIdx == -1) { // set as an object
					for (int i = 0; i < childCount; i++) {
						if (isNullVector[i]) {
							rows[i] = null;
							continue;
						}
						rows[i] = readBigDecimal(vector.vector[i]);
					}
				} else { // set as a field of Row
					for (int i = 0; i < childCount; i++) {
						if (isNullVector[i]) {
							((Row) rows[i]).setField(fieldIdx, null);
							continue;
						}
						((Row) rows[i]).setField(fieldIdx, readBigDecimal(vector.vector[i]));
					}
				}
			}
		} else {
			if (vector.isRepeating) { // fill complete list with first value
				// Also column contains null value and it's repeating
				// so all values are null, but we need to set list with null values
				fillListWithRepeatingNull(rows, fieldIdx, BigDecimal[].class, lengthVector, childCount);
			} else {
				int offset = 0;
				BigDecimal[] temp;
				boolean[] isNullVector = vector.isNull;
				if (fieldIdx == -1) { // set list as an object
					for (int i = 0; offset < childCount; i++) {
						temp = new BigDecimal[(int) lengthVector[i]];
						for (int j = 0; j < temp.length; j++) {
							if (isNullVector[offset]) {
								offset++;
								temp[j] = null;
								continue;
							}
							temp[j] = readBigDecimal(vector.vector[offset++]);
						}
						rows[i] = temp;
					}
				} else { // set list as a field of Row
					for (int i = 0; offset < childCount; i++) {
						temp = new BigDecimal[(int) lengthVector[i]];
						for (int j = 0; j < temp.length; j++) {
							if (isNullVector[offset]) {
								offset++;
								temp[j] = null;
								continue;
							}
							temp[j] = readBigDecimal(vector.vector[offset++]);
						}
						((Row) rows[i]).setField(fieldIdx, temp);
					}
				}
			}
		}
	}

	private static void readStructColumn(Object[] rows, int fieldIdx, StructColumnVector struct, TypeDescription schema, long[] lengthVector, int childCount) {

		List<TypeDescription> childrenTypes = schema.getChildren();

		int numChildren = childrenTypes.size();
		Row[] nestedFields = new Row[childCount];
		for (int i = 0; i < childCount; i++) {
			nestedFields[i] = new Row(numChildren);
		}
		for (int i = 0; i < numChildren; i++) {
			readField(nestedFields, i, childrenTypes.get(i), struct.fields[i], null, childCount);
		}

		boolean[] isNullVector = struct.isNull;

		// check if struct is directly in a list or not, e.g, array<struct<dt>>
		if (lengthVector == null) {
			if (fieldIdx == -1) { // set struct as an object
				for (int i = 0; i < childCount; i++) {
					if (isNullVector[i]) {
						rows[i] = null;
						continue;
					}
					rows[i] = nestedFields[i];
				}
			} else { // set struct as a field of Row
				for (int i = 0; i < childCount; i++) {
					if (isNullVector[i]) {
						((Row) rows[i]).setField(fieldIdx, null);
						continue;
					}
					((Row) rows[i]).setField(fieldIdx, nestedFields[i]);
				}
			}
		} else { // struct in a list
			int offset = 0;
			Row[] temp;
			if (fieldIdx == -1) { // set list of struct as an object
				for (int i = 0; offset < childCount; i++) {
					temp = new Row[(int) lengthVector[i]];
					for (int j = 0; j < temp.length; j++) {
						if (isNullVector[offset]) {
							temp[j] = null;
							continue;
						}
						temp[j] = nestedFields[offset++];
					}
					rows[i] = temp;
				}
			}
			else { // set list of struct as a field of Row
				for (int i = 0; offset < childCount; i++) {
					temp = new Row[(int) lengthVector[i]];
					for (int j = 0; j < temp.length; j++) {
						if (isNullVector[offset]) {
							temp[j] = null;
							continue;
						}
						temp[j] = nestedFields[offset++];
					}
					((Row) rows[i]).setField(fieldIdx, temp);
				}
			}
		}
	}

	private static void readListColumn(Object[] rows, int fieldIdx, ListColumnVector list, TypeDescription schema, long[] lengthVector, int childCount) {

		TypeDescription fieldType = schema.getChildren().get(0);
		if (lengthVector == null) {
			long[] lengthVectorNested = list.lengths;
			readField(rows, fieldIdx, fieldType, list.child, lengthVectorNested, list.childCount);
		}
		else { // list in a list

			Object[] nestedList = new Object[childCount];

			// length vector for nested list
			long[] lengthVectorNested = list.lengths;

			// read nested list
			readField(nestedList, -1, fieldType, list.child, lengthVectorNested, list.childCount);

			// get type of nestedList
			Class<?> classType = nestedList[0].getClass();

			// fill outer list with nested list
			int offset = 0;
			int length;
			if (fieldIdx == -1) { // set list of list as an object
				for (int i = 0; offset < childCount; i++) {
					length = (int) lengthVector[i];
					Object temp = Array.newInstance(classType, length);
					System.arraycopy(nestedList, offset, temp, 0, length);
					offset = offset + length;
					rows[i] = temp;

				}
			} else { // set list of list as an field on Row
				for (int i = 0; offset < childCount; i++) {
					length = (int) lengthVector[i];
					Object temp = Array.newInstance(classType, length);
					System.arraycopy(nestedList, offset, temp, 0, length);
					offset = offset + length;
					((Row) rows[i]).setField(fieldIdx, temp);
				}
			}
		}
	}

	private static void readMapColumn(Object[] rows, int fieldIdx, MapColumnVector map, TypeDescription schema, long[] lengthVector, int childCount) {

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

		// check if map is directly in a list or not, e.g, array<map<k,v>>
		if (lengthVector == null) {
			long[] lengthVectorMap = map.lengths;
			int offset = 0;
			if (fieldIdx == -1) { // set map as an object
				for (int i = 0; i < childCount; i++) {
					if (isNullVector[i]) {
						rows[i] = null;
						continue;
					}
					rows[i] = readHashMap(keyRows, valueRows, offset, lengthVectorMap[i]);
					offset += lengthVectorMap[i];
				}
			} else { // set map as a field of Row
				for (int i = 0; i < childCount; i++) {
					if (isNullVector[i]) {
						((Row) rows[i]).setField(fieldIdx, null);
						continue;
					}
					((Row) rows[i]).setField(fieldIdx, readHashMap(keyRows, valueRows, offset, lengthVectorMap[i]));
					offset += lengthVectorMap[i];
				}
			}
		} else { // list of map
			long[] lengthVectorMap = map.lengths;
			int mapOffset = 0; // offset of map element
			int offset = 0; // offset of map
			HashMap[] temp;
			if (fieldIdx == -1) { // set map list as an object
				for (int i = 0; offset < childCount; i++) {
					temp = new HashMap[(int) lengthVector[i]];
					for (int j = 0; j < temp.length; j++) {
						if (isNullVector[offset]) {
							temp[j] = null;
							continue;
						}
						temp[j] = readHashMap(keyRows, valueRows, mapOffset, lengthVectorMap[offset]);
						mapOffset += lengthVectorMap[offset];
						offset++;
					}
					rows[i] = temp;
				}
			} else { // set map list as a field of Row
				for (int i = 0; offset < childCount; i++) {
					temp = new HashMap[(int) lengthVector[i]];
					for (int j = 0; j < temp.length; j++) {
						if (isNullVector[offset]) {
							temp[j] = null;
							continue;
						}
						temp[j] = readHashMap(keyRows, valueRows, mapOffset, lengthVectorMap[offset]);
						mapOffset += lengthVectorMap[offset];
						offset++;
					}
					((Row) rows[i]).setField(fieldIdx, temp);
				}
			}
		}
	}

	private static BigDecimal readBigDecimal(HiveDecimalWritable hiveDecimalWritable) {
		HiveDecimal hiveDecimal = hiveDecimalWritable.getHiveDecimal();
		return hiveDecimal.bigDecimalValue();
	}

	private static byte[] readBinary(byte[] src, int srcPos, int length) {
		byte[] result = new byte[length];
		System.arraycopy(src, srcPos, result, 0, length);
		return result;
	}

	private static Timestamp readTimeStamp(long time, int nanos) {
		Timestamp ts = new Timestamp(time);
		ts.setNanos(nanos);
		return ts;
	}

	private static Date readDate(long days) {
		// day to milliseconds
		return new Date(days * 24 * 60 * 60 * 1000);
	}

	private static HashMap readHashMap(Object[] keyRows, Object[] valueRows, int offset, long length) {

		HashMap<Object, Object> resultMap = new HashMap<>();
		for (int j = 0; j < length; j++) {
			resultMap.put(keyRows[offset], valueRows[offset]);
			offset++;
		}
		return resultMap;
	}

}
