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

package org.apache.flink.formats.parquet.vector;

import org.apache.flink.table.dataformat.Decimal;
import org.apache.flink.table.dataformat.vector.BytesColumnVector;
import org.apache.flink.table.dataformat.vector.ColumnVector;
import org.apache.flink.table.dataformat.vector.DecimalColumnVector;
import org.apache.flink.table.dataformat.vector.IntColumnVector;
import org.apache.flink.table.dataformat.vector.LongColumnVector;

/**
 * Parquet write decimal as int32 and int64 and binary, this class wrap the real vector to
 * provide {@link DecimalColumnVector} interface.
 */
public class ParquetDecimalVector implements DecimalColumnVector {

	private final ColumnVector vector;

	ParquetDecimalVector(ColumnVector vector) {
		this.vector = vector;
	}

	@Override
	public Decimal getDecimal(int i, int precision, int scale) {
		if (Decimal.is32BitDecimal(precision)) {
			return Decimal.fromUnscaledLong(
					precision, scale, ((IntColumnVector) vector).getInt(i));
		} else if (Decimal.is64BitDecimal(precision)) {
			return Decimal.fromUnscaledLong(
					precision, scale, ((LongColumnVector) vector).getLong(i));
		} else {
			return Decimal.fromUnscaledBytes(
					precision, scale, ((BytesColumnVector) vector).getBytes(i).getBytes());
		}
	}

	@Override
	public boolean isNullAt(int i) {
		return vector.isNullAt(i);
	}
}
