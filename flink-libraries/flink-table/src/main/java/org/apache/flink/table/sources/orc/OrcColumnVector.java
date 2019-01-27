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

package org.apache.flink.table.sources.orc;

import org.apache.flink.table.api.types.BooleanType;
import org.apache.flink.table.api.types.ByteArrayType;
import org.apache.flink.table.api.types.ByteType;
import org.apache.flink.table.api.types.DataType;
import org.apache.flink.table.api.types.DateType;
import org.apache.flink.table.api.types.DecimalType;
import org.apache.flink.table.api.types.DoubleType;
import org.apache.flink.table.api.types.FloatType;
import org.apache.flink.table.api.types.IntType;
import org.apache.flink.table.api.types.LongType;
import org.apache.flink.table.api.types.ShortType;
import org.apache.flink.table.api.types.StringType;
import org.apache.flink.table.api.types.TimestampType;
import org.apache.flink.table.dataformat.Decimal;
import org.apache.flink.table.dataformat.vector.TypeGetVector;
import org.apache.flink.table.dataformat.vector.VectorizedColumnBatch;

import org.apache.orc.storage.ql.exec.vector.BytesColumnVector;
import org.apache.orc.storage.ql.exec.vector.ColumnVector;
import org.apache.orc.storage.ql.exec.vector.DecimalColumnVector;
import org.apache.orc.storage.ql.exec.vector.DoubleColumnVector;
import org.apache.orc.storage.ql.exec.vector.LongColumnVector;
import org.apache.orc.storage.ql.exec.vector.TimestampColumnVector;
import org.apache.orc.storage.serde2.io.HiveDecimalWritable;

import java.util.Arrays;

/**
 *  A column vector class wrapping hive's ColumnVector.
 *  This column vector is used to adapt hive's ColumnVector to Flink's ColumnVector.
 */
public class OrcColumnVector extends TypeGetVector {
	private DataType fieldType;
	private ColumnVector baseData;
	private LongColumnVector longData;
	private DoubleColumnVector doubleData;
	private BytesColumnVector bytesData;
	private DecimalColumnVector decimalData;
	private TimestampColumnVector timestampData;

	public OrcColumnVector(DataType fieldType, ColumnVector vector) {
		super(vector.isNull.length);
		this.fieldType = fieldType;
		baseData = vector;

		if (vector instanceof LongColumnVector) {
			longData = (LongColumnVector) vector;
		} else if (vector instanceof DoubleColumnVector) {
			doubleData = (DoubleColumnVector) vector;
		} else if (vector instanceof BytesColumnVector) {
			bytesData = (BytesColumnVector) vector;
		} else if (vector instanceof DecimalColumnVector) {
			decimalData = (DecimalColumnVector) vector;
		} else if (vector instanceof TimestampColumnVector) {
			timestampData = (TimestampColumnVector) vector;
		} else {
			throw new UnsupportedOperationException();
		}
	}

	@Override
	public Object get(int index) {
		if (baseData.isNull[index]) {
			return null;
		}

		if (baseData.isRepeating) {
			index = 0;
		}

		if (fieldType instanceof BooleanType) {
			return longData.vector[index] == 1;
		} else if (fieldType instanceof ByteType) {
			return (byte) longData.vector[index];
		} else if (fieldType instanceof ShortType) {
			return (short) longData.vector[index];
		} else if (fieldType instanceof IntType) {
			return (int) longData.vector[index];
		} else if (fieldType instanceof LongType) {
			return longData.vector[index];
		} else if (fieldType instanceof FloatType) {
			return (float) doubleData.vector[index];
		} else if (fieldType instanceof DoubleType) {
			return doubleData.vector[index];
		} else if (fieldType instanceof StringType ||
				fieldType instanceof ByteArrayType) {
			byte[][] data = bytesData.vector;
			int[] start = bytesData.start;
			int[] length = bytesData.length;
			return Arrays.copyOfRange(data[index], start[index], start[index] + length[index]);
		} else if (fieldType instanceof DecimalType) {
			HiveDecimalWritable[] data = decimalData.vector;
			int precision = ((DecimalType) fieldType).precision();
			int scala = ((DecimalType) fieldType).scale();
			Decimal decimal = Decimal.fromBigDecimal(data[index].getHiveDecimal().bigDecimalValue(), precision, scala);
			if (Decimal.is32BitDecimal(precision)) {
				return (int) (decimal.toUnscaledLong());
			} else if (Decimal.is64BitDecimal(precision)) {
				return decimal.toUnscaledLong();
			} else {
				return decimal.toUnscaledBytes();
			}
		} else if (fieldType instanceof DateType) {
			return (int) longData.vector[index];
		} else if (fieldType instanceof TimestampType) {
			return (timestampData.time[index] + timestampData.nanos[index] / 1000000);
		} else {
			throw new UnsupportedOperationException("Unsupported Data Type: " + fieldType);
		}
	}

	@Override
	public boolean getBoolean(int rowId) {
		if (baseData.isRepeating) {
			rowId = 0;
		}
		return longData.vector[rowId] == 1;
	}

	@Override
	public byte getByte(int rowId) {
		if (baseData.isRepeating) {
			rowId = 0;
		}
		return (byte) longData.vector[rowId];
	}

	@Override
	public short getShort(int rowId) {
		if (baseData.isRepeating) {
			rowId = 0;
		}
		return (short) longData.vector[rowId];
	}

	@Override
	public int getInt(int rowId) {
		if (baseData.isRepeating) {
			rowId = 0;
		}
		return (int) longData.vector[rowId];
	}

	@Override
	public long getLong(int rowId) {
		if (baseData.isRepeating) {
			rowId = 0;
		}

		if (fieldType instanceof TimestampType) {
			return (timestampData.time[rowId] + timestampData.nanos[rowId] / 1000000);
		} else {
			return longData.vector[rowId];
		}
	}

	@Override
	public float getFloat(int rowId) {
		if (baseData.isRepeating) {
			rowId = 0;
		}
		return (float) doubleData.vector[rowId];
	}

	@Override
	public double getDouble(int rowId) {
		if (baseData.isRepeating) {
			rowId = 0;
		}
		return doubleData.vector[rowId];
	}

	@Override
	public VectorizedColumnBatch.ByteArray getByteArray(int rowId) {
		if (baseData.isRepeating) {
			rowId = 0;
		}
		byte[][] data = bytesData.vector;
		int[] start = bytesData.start;
		int[] length = bytesData.length;
		return new VectorizedColumnBatch.ByteArray(data[rowId], start[rowId], length[rowId]);
	}

	@Override
	public Decimal getDecimal(int rowId, int precision, int scala) {
		if (baseData.isRepeating) {
			rowId = 0;
		}

		HiveDecimalWritable[] data = decimalData.vector;
		return Decimal.fromBigDecimal(data[rowId].getHiveDecimal().bigDecimalValue(), precision, scala);
	}

	@Override
	public void setElement(
			int outElementNum,
			int inputElementNum,
			org.apache.flink.table.dataformat.vector.ColumnVector inputVector) {
		throw new UnsupportedOperationException();
	}

	public void setNullInfo(ColumnVector vector) {
		System.arraycopy(vector.isNull, 0, isNull, 0, vector.isNull.length);
		noNulls = vector.noNulls;
	}
}
