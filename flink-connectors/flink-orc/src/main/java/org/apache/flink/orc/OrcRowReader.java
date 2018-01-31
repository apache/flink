/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.orc;

import org.apache.flink.api.common.io.InputFormat;
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
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;

import java.io.IOException;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

/**
 * Maps Orc vectors to rows. Adapted from org.apache.orc:orc-mapreduce.
 */
class OrcRowReader {
	private static final long MILLIS_PER_DAY = 86400000L;
	private static final TimeZone LOCAL_TZ = TimeZone.getDefault();
	private final TypeDescription schema;
	private final int[] selectedFields;
	private final RecordReader batchReader;
	private final VectorizedRowBatch batch;
	private final Map<TypeDescription, Class<?>> javaTypesCache;
	private int rowInBatch;

	OrcRowReader(TypeDescription schema, int[] selectedFields, RecordReader batchReader) {
		this.schema = schema;
		this.selectedFields = selectedFields;
		this.batchReader = batchReader;
		this.batch = schema.createRowBatch();
		this.javaTypesCache = new HashMap<>();
		this.rowInBatch = 0;
	}

	private Boolean nextBoolean(ColumnVector vector, int row) {
		if (vector.isRepeating) {
			row = 0;
		}
		if (vector.noNulls || !vector.isNull[row]) {
			return ((LongColumnVector) vector).vector[row] != 0;
		} else {
			return null;
		}
	}

	private Byte nextByte(ColumnVector vector, int row) {
		if (vector.isRepeating) {
			row = 0;
		}
		if (vector.noNulls || !vector.isNull[row]) {
			return (byte) ((LongColumnVector) vector).vector[row];
		} else {
			return null;
		}
	}

	private Short nextShort(ColumnVector vector, int row) {
		if (vector.isRepeating) {
			row = 0;
		}
		if (vector.noNulls || !vector.isNull[row]) {
			return (short) ((LongColumnVector) vector).vector[row];
		} else {
			return null;
		}
	}

	private Integer nextInt(ColumnVector vector, int row) {
		if (vector.isRepeating) {
			row = 0;
		}
		if (vector.noNulls || !vector.isNull[row]) {
			return (int) ((LongColumnVector) vector).vector[row];
		} else {
			return null;
		}
	}

	private Long nextLong(ColumnVector vector, int row) {
		if (vector.isRepeating) {
			row = 0;
		}
		if (vector.noNulls || !vector.isNull[row]) {
			return ((LongColumnVector) vector).vector[row];
		} else {
			return null;
		}
	}

	private Float nextFloat(ColumnVector vector, int row) {
		if (vector.isRepeating) {
			row = 0;
		}
		if (vector.noNulls || !vector.isNull[row]) {
			return (float) ((DoubleColumnVector) vector).vector[row];
		} else {
			return null;
		}
	}

	private Double nextDouble(ColumnVector vector, int row) {
		if (vector.isRepeating) {
			row = 0;
		}
		if (vector.noNulls || !vector.isNull[row]) {
			return ((DoubleColumnVector) vector).vector[row];
		} else {
			return null;
		}
	}

	private String nextString(ColumnVector vector, int row) {
		if (vector.isRepeating) {
			row = 0;
		}
		if (vector.noNulls || !vector.isNull[row]) {
			BytesColumnVector bytes = (BytesColumnVector) vector;
			return new String(bytes.vector[row], bytes.start[row], bytes.length[row]);
		} else {
			return null;
		}
	}

	private byte[] nextBinary(ColumnVector vector, int row) {
		if (vector.isRepeating) {
			row = 0;
		}
		if (vector.noNulls || !vector.isNull[row]) {
			BytesColumnVector bytes = (BytesColumnVector) vector;
			int length = bytes.length[row];
			byte[] result = new byte[length];
			System.arraycopy(bytes.vector[row], bytes.start[row], result, 0, length);
			return result;
		} else {
			return null;
		}
	}

	private BigDecimal nextDecimal(ColumnVector vector, int row, Object previous) {
		if (vector.isRepeating) {
			row = 0;
		}
		if (vector.noNulls || !vector.isNull[row]) {
			HiveDecimalWritable writable = new HiveDecimalWritable();
			writable.set(((DecimalColumnVector) vector).vector[row]);
			HiveDecimal hiveDecimal = writable.getHiveDecimal();
			BigDecimal result = hiveDecimal.bigDecimalValue();
			return result;
		} else {
			return null;
		}
	}

	private java.sql.Date nextDate(ColumnVector vector, int row, Object previous) {
		if (vector.isRepeating) {
			row = 0;
		}
		if (vector.noNulls || !vector.isNull[row]) {
			java.sql.Date result;
			if (previous == null || previous.getClass() != java.sql.Date.class) {
				result = new java.sql.Date(0);
			} else {
				result = (java.sql.Date) previous;
			}
			int l = (int) ((LongColumnVector) vector).vector[row];
			long t = l * MILLIS_PER_DAY;
			result.setTime(t - LOCAL_TZ.getOffset(t));
			return result;
		} else {
			return null;
		}
	}

	private java.sql.Timestamp nextTimestamp(ColumnVector vector, int row, Object previous) {
		if (vector.isRepeating) {
			row = 0;
		}
		if (vector.noNulls || !vector.isNull[row]) {
			java.sql.Timestamp result;
			if (previous == null || previous.getClass() != java.sql.Timestamp.class) {
				result = new java.sql.Timestamp(0);
			} else {
				result = (java.sql.Timestamp) previous;
			}
			TimestampColumnVector tcv = (TimestampColumnVector) vector;
			result.setTime(tcv.time[row]);
			result.setNanos(tcv.nanos[row]);
			return result;
		} else {
			return null;
		}
	}

	private Row nextStruct(ColumnVector vector, int row, TypeDescription schema, Object previous) {
		if (vector.isRepeating) {
			row = 0;
		}
		if (vector.noNulls || !vector.isNull[row]) {
			List<TypeDescription> childrenTypes = schema.getChildren();
			int numChildren = childrenTypes.size();
			// TODO: Implement reuse
			Row result = new Row(numChildren);
			StructColumnVector struct = (StructColumnVector) vector;
			for (int f = 0; f < numChildren; ++f) {
				result.setField(f, nextValue(struct.fields[f], row,
					childrenTypes.get(f), null));
			}
			return result;
		} else {
			return null;
		}
	}

	private Object[] nextList(ColumnVector vector, int row, TypeDescription schema, Object previous) {
		if (vector.isRepeating) {
			row = 0;
		}
		if (vector.noNulls || !vector.isNull[row]) {
			List<TypeDescription> childrenTypes = schema.getChildren();
			TypeDescription valueType = childrenTypes.get(0);
			final int oldLength;
			final Object[] previousArray;
			if (previous == null || previous.getClass() != Object[].class) {
				previousArray = null;
				oldLength = 0;
			} else {
				previousArray = (Object[]) previous;
				oldLength = previousArray.length;
			}
			ListColumnVector list = (ListColumnVector) vector;
			int length = (int) list.lengths[row];
			int offset = (int) list.offsets[row];
			Object[] result = newArray(valueType, length);
			int idx = 0;
			for (; idx < length && idx < oldLength; idx++) {
				result[idx] = nextValue(list.child, offset + idx, valueType, previousArray[idx]);
			}
			if (oldLength < length) {
				for (; idx < length; idx++) {
					result[idx] = nextValue(list.child, offset + idx, valueType, null);
				}
			}
			return result;
		} else {
			return null;
		}
	}

	private Object[] newArray(TypeDescription valueType, int length) {
		Class<?> valueJavaType = lookupJavaType(valueType);
		return (Object[]) Array.newInstance(valueJavaType, length);
	}

	private HashMap nextMap(ColumnVector vector, int row, TypeDescription schema, Object previous) {
		if (vector.isRepeating) {
			row = 0;
		}
		if (vector.noNulls || !vector.isNull[row]) {
			MapColumnVector map = (MapColumnVector) vector;
			int length = (int) map.lengths[row];
			int offset = (int) map.offsets[row];
			List<TypeDescription> childrenTypes = schema.getChildren();
			TypeDescription keyType = childrenTypes.get(0);
			TypeDescription valueType = childrenTypes.get(1);
			HashMap<Object, Object> result = new HashMap<>();
			for (int e = 0; e < length; ++e) {
				result.put(nextValue(map.keys, e + offset, keyType, null),
					nextValue(map.values, e + offset, valueType, null));
			}
			return result;
		} else {
			return null;
		}
	}

	private Object nextValue(ColumnVector vector, int row, TypeDescription schema, Object previous) {
		switch (schema.getCategory()) {
			case BOOLEAN:
				return nextBoolean(vector, row);
			case BYTE:
				return nextByte(vector, row);
			case SHORT:
				return nextShort(vector, row);
			case INT:
				return nextInt(vector, row);
			case LONG:
				return nextLong(vector, row);
			case FLOAT:
				return nextFloat(vector, row);
			case DOUBLE:
				return nextDouble(vector, row);
			case STRING:
			case CHAR:
			case VARCHAR:
				return nextString(vector, row);
			case BINARY:
				return nextBinary(vector, row);
			case DECIMAL:
				return nextDecimal(vector, row, previous);
			case DATE:
				return nextDate(vector, row, previous);
			case TIMESTAMP:
				return nextTimestamp(vector, row, previous);
			case STRUCT:
				return nextStruct(vector, row, schema, previous);
			case UNION:
				throw new UnsupportedOperationException("UNION type not supported yet");
			case LIST:
				return nextList(vector, row, schema, previous);
			case MAP:
				return nextMap(vector, row, schema, previous);
			default:
				throw new IllegalArgumentException("Unknown type " + schema);
		}
	}

	private Class<?> lookupJavaType(TypeDescription schema) {
		Class<?> result = javaTypesCache.get(schema);
		if (result == null) {
			result = javaType(schema);
			javaTypesCache.put(schema, result);
		}
		return result;
	}

	private Class<?> javaType(TypeDescription schema) {
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
			case STRING:
			case CHAR:
			case VARCHAR:
				return String.class;
			case BINARY:
				return byte[].class;
			case DECIMAL:
				return BigDecimal.class;
			case DATE:
				return java.sql.Date.class;
			case TIMESTAMP:
				return java.sql.Timestamp.class;
			case STRUCT:
				return Row.class;
			case UNION:
				throw new UnsupportedOperationException("UNION type not supported yet");
			case LIST:
				List<TypeDescription> children = schema.getChildren();
				TypeDescription valueType = children.get(0);
				Class<?> valueJavaType = javaType(valueType);
				return Array.newInstance(valueJavaType, 0).getClass();
			case MAP:
				return HashMap.class;
			default:
				throw new IllegalArgumentException("Unknown type " + schema);
		}
	}

	private boolean ensureBatch() throws IOException {
		if (rowInBatch >= batch.size) {
			rowInBatch = 0;
			return batchReader.nextBatch(batch);
		}
		return true;
	}

	/**
	 * Check if end of {@link RecordReader} is reached.
	 *
	 * @return True if end was reached, false otherwise
	 * @throws IOException If next batch was attempted to be read and failed
	 * @see InputFormat#reachedEnd()
	 */
	boolean reachedEnd() throws IOException {
		return !ensureBatch();
	}

	/**
	 * Maps next record in current batch to {@link Row}.
	 *
	 * @param reuse Optional row object to be reused
	 * @return Next row
	 * @throws IOException If batch reading was attempted but failed
	 * @see InputFormat#nextRecord(Object)
	 */
	Row nextRecord(Row reuse) throws IOException {
		if (!ensureBatch()) {
			throw new IllegalStateException("End of input");
		}
		final Row result;
		if (schema.getCategory() == TypeDescription.Category.STRUCT) {
			result = reuse == null ? new Row(selectedFields.length) : reuse;
			List<TypeDescription> children = schema.getChildren();
			int numberOfChildren = selectedFields.length;
			for (int i = 0; i < numberOfChildren; ++i) {
				int orcFieldIdx = selectedFields[i];
				result.setField(i, nextValue(batch.cols[orcFieldIdx], rowInBatch,
					children.get(orcFieldIdx), result.getField(i)));
			}
		} else {
			throw new IllegalStateException("Root must be STRUCT type");
		}
		rowInBatch += 1;
		return result;
	}

	/**
	 * Closes {@link RecordReader}.
	 *
	 * @see RecordReader#close()
	 * @see InputFormat#close()
	 */
	void close() throws IOException {
		this.batchReader.close();
	}
}
