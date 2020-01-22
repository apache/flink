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

import org.apache.flink.core.fs.Path;
import org.apache.flink.table.dataformat.Decimal;
import org.apache.flink.table.dataformat.SqlTimestamp;
import org.apache.flink.table.dataformat.vector.ColumnVector;
import org.apache.flink.table.dataformat.vector.VectorizedColumnBatch;
import org.apache.flink.table.dataformat.vector.heap.HeapBooleanVector;
import org.apache.flink.table.dataformat.vector.heap.HeapByteVector;
import org.apache.flink.table.dataformat.vector.heap.HeapBytesVector;
import org.apache.flink.table.dataformat.vector.heap.HeapDoubleVector;
import org.apache.flink.table.dataformat.vector.heap.HeapFloatVector;
import org.apache.flink.table.dataformat.vector.heap.HeapIntVector;
import org.apache.flink.table.dataformat.vector.heap.HeapLongVector;
import org.apache.flink.table.dataformat.vector.heap.HeapShortVector;
import org.apache.flink.table.dataformat.vector.heap.HeapTimestampVector;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.util.Preconditions;

import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.table.runtime.functions.SqlDateTimeUtils.dateToInternal;

/**
 * Util for generating {@link ParquetColumnarRowSplitReader}.
 */
public class ParquetSplitReaderUtil {

	/**
	 * Util for generating partitioned {@link ParquetColumnarRowSplitReader}.
	 */
	public static ParquetColumnarRowSplitReader genPartColumnarRowReader(
			boolean utcTimestamp,
			Configuration conf,
			String[] fullFieldNames,
			DataType[] fullFieldTypes,
			Map<String, Object> partitionSpec,
			int[] selectedFields,
			int batchSize,
			Path path,
			long splitStart,
			long splitLength) throws IOException {
		List<String> nonPartNames = Arrays.stream(fullFieldNames)
				.filter(n -> !partitionSpec.containsKey(n))
				.collect(Collectors.toList());

		List<String> selNonPartNames = Arrays.stream(selectedFields)
				.mapToObj(i -> fullFieldNames[i])
				.filter(nonPartNames::contains).collect(Collectors.toList());

		int[] selOrcFields = selNonPartNames.stream()
				.mapToInt(nonPartNames::indexOf)
				.toArray();

		ParquetColumnarRowSplitReader.ColumnBatchGenerator gen = readVectors -> {
			// create and initialize the row batch
			ColumnVector[] vectors = new ColumnVector[selectedFields.length];
			for (int i = 0; i < vectors.length; i++) {
				String name = fullFieldNames[selectedFields[i]];
				LogicalType type = fullFieldTypes[selectedFields[i]].getLogicalType();
				vectors[i] = partitionSpec.containsKey(name) ?
						createVectorFromConstant(type, partitionSpec.get(name), batchSize) :
						readVectors[i];
			}
			return new VectorizedColumnBatch(vectors);
		};

		return new ParquetColumnarRowSplitReader(
				utcTimestamp,
				conf,
				Arrays.stream(selOrcFields)
						.mapToObj(i -> fullFieldTypes[i].getLogicalType())
						.toArray(LogicalType[]::new),
				selNonPartNames.toArray(new String[0]),
				gen,
				batchSize,
				new org.apache.hadoop.fs.Path(path.toUri()),
				splitStart,
				splitLength);
	}

	private static ColumnVector createVectorFromConstant(
			LogicalType type,
			Object value,
			int batchSize) {
		switch (type.getTypeRoot()) {
			case CHAR:
			case VARCHAR:
			case BINARY:
			case VARBINARY:
				HeapBytesVector bsv = new HeapBytesVector(batchSize);
				if (value == null) {
					bsv.fillWithNulls();
				} else {
					bsv.fill(value instanceof byte[] ?
							(byte[]) value :
							value.toString().getBytes(StandardCharsets.UTF_8));
				}
				return bsv;
			case BOOLEAN:
				HeapBooleanVector bv = new HeapBooleanVector(batchSize);
				if (value == null) {
					bv.fillWithNulls();
				} else {
					bv.fill((boolean) value);
				}
				return bv;
			case TINYINT:
				HeapByteVector byteVector = new HeapByteVector(batchSize);
				if (value == null) {
					byteVector.fillWithNulls();
				} else {
					byteVector.fill(((Number) value).byteValue());
				}
				return byteVector;
			case SMALLINT:
				HeapShortVector sv = new HeapShortVector(batchSize);
				if (value == null) {
					sv.fillWithNulls();
				} else {
					sv.fill(((Number) value).shortValue());
				}
				return sv;
			case INTEGER:
				HeapIntVector iv = new HeapIntVector(batchSize);
				if (value == null) {
					iv.fillWithNulls();
				} else {
					iv.fill(((Number) value).intValue());
				}
				return iv;
			case BIGINT:
				HeapLongVector lv = new HeapLongVector(batchSize);
				if (value == null) {
					lv.fillWithNulls();
				} else {
					lv.fill(((Number) value).longValue());
				}
				return lv;
			case DECIMAL:
				DecimalType decimalType = (DecimalType) type;
				int precision = decimalType.getPrecision();
				int scale = decimalType.getScale();
				Decimal decimal = value == null ? null : Preconditions.checkNotNull(
						Decimal.fromBigDecimal((BigDecimal) value, precision, scale));
				ColumnVector internalVector;
				if (Decimal.is32BitDecimal(precision)) {
					internalVector = createVectorFromConstant(
							new IntType(),
							decimal == null ? null : (int) decimal.toUnscaledLong(),
							batchSize);
				} else if (Decimal.is64BitDecimal(precision)) {
					internalVector = createVectorFromConstant(
							new BigIntType(),
							decimal == null ? null : decimal.toUnscaledLong(),
							batchSize);
				} else {
					internalVector = createVectorFromConstant(
							new VarBinaryType(),
							decimal == null ? null : decimal.toUnscaledBytes(),
							batchSize);
				}
				return new ParquetDecimalVector(internalVector);
			case FLOAT:
				HeapFloatVector fv = new HeapFloatVector(batchSize);
				if (value == null) {
					fv.fillWithNulls();
				} else {
					fv.fill(((Number) value).floatValue());
				}
				return fv;
			case DOUBLE:
				HeapDoubleVector dv = new HeapDoubleVector(batchSize);
				if (value == null) {
					dv.fillWithNulls();
				} else {
					dv.fill(((Number) value).doubleValue());
				}
				return dv;
			case DATE:
				if (value instanceof LocalDate) {
					value = Date.valueOf((LocalDate) value);
				}
				return createVectorFromConstant(
						new IntType(),
						value == null ? null : dateToInternal((Date) value),
						batchSize);
			case TIMESTAMP_WITHOUT_TIME_ZONE:
				HeapTimestampVector tv = new HeapTimestampVector(batchSize);
				if (value == null) {
					tv.fillWithNulls();
				} else {
					tv.fill(SqlTimestamp.fromLocalDateTime((LocalDateTime) value));
				}
				return tv;
			default:
				throw new UnsupportedOperationException("Unsupported type: " + type);
		}
	}

}
