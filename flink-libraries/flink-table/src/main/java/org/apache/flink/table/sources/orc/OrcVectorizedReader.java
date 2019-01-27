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
import org.apache.flink.table.api.types.InternalType;
import org.apache.flink.table.api.types.LongType;
import org.apache.flink.table.api.types.ShortType;
import org.apache.flink.table.api.types.StringType;
import org.apache.flink.table.api.types.TimestampType;
import org.apache.flink.table.dataformat.Decimal;
import org.apache.flink.table.dataformat.vector.BooleanColumnVector;
import org.apache.flink.table.dataformat.vector.ByteColumnVector;
import org.apache.flink.table.dataformat.vector.BytesColumnVector;
import org.apache.flink.table.dataformat.vector.ColumnVector;
import org.apache.flink.table.dataformat.vector.DateColumnVector;
import org.apache.flink.table.dataformat.vector.DoubleColumnVector;
import org.apache.flink.table.dataformat.vector.FloatColumnVector;
import org.apache.flink.table.dataformat.vector.IntegerColumnVector;
import org.apache.flink.table.dataformat.vector.LongColumnVector;
import org.apache.flink.table.dataformat.vector.ShortColumnVector;
import org.apache.flink.table.dataformat.vector.TimestampColumnVector;
import org.apache.flink.table.dataformat.vector.VectorizedColumnBatch;
import org.apache.flink.util.Preconditions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.orc.OrcConf;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcInputFormat;
import org.apache.orc.storage.common.type.HiveDecimal;
import org.apache.orc.storage.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.storage.serde2.io.HiveDecimalWritable;

import java.io.IOException;
import java.util.Arrays;

/**
 * This reader is used to read a {@link VectorizedColumnBatch} from input split, part of the code is referred
 * from Apache Spark and Hive.
 */
public class OrcVectorizedReader extends RecordReader<Void, Object> {

	/**
	 * Capacity of {@link VectorizedRowBatch}, should be consistent with {@link VectorizedColumnBatch}
	 * TODO: make this configurable.
	 */
	private static final int MAX_SIZE = 2048;

	// Vectorized ORC Row Batch
	private VectorizedRowBatch batch;

	private TypeDescription orcSchema;

	/**
	 * The column IDs of the physical ORC file schema which are required by this reader.
	 * -1 means this required column doesn't exist in the ORC file.
	 */
	private int[] requestedColumnIds;

	// Record reader from ORC row batch.
	private org.apache.orc.RecordReader recordReader;

	protected VectorizedColumnBatch columnarBatch;

	// The wrapped orc column Vectors. It should be null if 'copyToFlink' is true.
	protected ColumnVector[] orcVectorWrappers;

	protected InternalType[] fieldTypes;
	protected String[] fieldNames;
	protected String[] schemaFieldNames;

	// Whether or not to copy the ORC columnar batch to flink columnar batch.
	// TODO: make this configurable.
	private boolean copyToFlink = false;

	private boolean caseSensitive = true;

	public OrcVectorizedReader(InternalType[] fieldTypes, String[] fieldNames, String[] schemaFieldNames) {
		this(fieldTypes, fieldNames, schemaFieldNames, false, true);
	}

	public OrcVectorizedReader(
			InternalType[] fieldTypes,
			String[] fieldNames,
			String[] schemaFieldNames,
			boolean copyToFlink,
			boolean caseSensitive) {
		super();
		Preconditions.checkArgument(fieldTypes != null && fieldTypes.length > 0);
		Preconditions.checkArgument(fieldNames != null && fieldNames.length == fieldTypes.length);
		this.fieldTypes = fieldTypes;
		this.fieldNames = fieldNames;
		this.schemaFieldNames = schemaFieldNames;
		this.copyToFlink = copyToFlink;
		this.caseSensitive = caseSensitive;
	}

	@Override
	public void initialize(
			InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
		FileSplit fileSplit = (FileSplit) inputSplit;
		Configuration conf = taskAttemptContext.getConfiguration();
		Reader reader = OrcFile.createReader(
			fileSplit.getPath(),
			OrcFile.readerOptions(conf)
				.maxLength(OrcConf.MAX_FILE_LENGTH.getLong(conf))
				.filesystem(fileSplit.getPath().getFileSystem(conf)));
		Reader.Options options =
			OrcInputFormat.buildOptions(conf, reader, fileSplit.getStart(), fileSplit.getLength());
		recordReader = reader.rows(options);
		orcSchema = reader.getSchema();
		requestedColumnIds = OrcUtils.requestedColumnIds(caseSensitive, fieldNames, schemaFieldNames, reader);

		initBatch();
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		return nextBatch();
	}

	@Override
	public Void getCurrentKey() throws IOException, InterruptedException {
		return null;
	}

	@Override
	public Object getCurrentValue() throws IOException, InterruptedException {
		return columnarBatch;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return 0;
	}

	@Override
	public void close() throws IOException {
		if (recordReader != null) {
			recordReader.close();
			recordReader = null;
		}
	}

	void initBatch() {
		batch = orcSchema.createRowBatch(MAX_SIZE);
		assert (!batch.selectedInUse); // `selectedInUse` should be initialized with `false`.

		if (copyToFlink) {
			columnarBatch = VectorizedColumnBatch.allocate(fieldTypes);

			// Initialize missing columns
			for (int i = 0; i < requestedColumnIds.length; i++) {
				if (requestedColumnIds[i] == -1) {
					Arrays.fill(columnarBatch.columns[i].isNull, true);
					columnarBatch.columns[i].noNulls = false;
				}
			}
		} else {
			orcVectorWrappers = new ColumnVector[fieldTypes.length];
			for (int i = 0; i < fieldTypes.length; i++) {
				int columnId = requestedColumnIds[i];
				if (columnId == -1) {
					orcVectorWrappers[i] = new MissingColumnVector(MAX_SIZE);
				} else {
					orcVectorWrappers[i] = new OrcColumnVector(fieldTypes[i], batch.cols[columnId]);
				}
			}
			columnarBatch = new VectorizedColumnBatch(fieldTypes, MAX_SIZE, orcVectorWrappers);
		}
	}

	protected boolean nextBatch() throws IOException {
		recordReader.nextBatch(batch);
		int batchSize = batch.size;
		if (batchSize == 0) {
			return false;
		}

		columnarBatch.reset();
		columnarBatch.setNumRows(batchSize);

		if (!copyToFlink) {
			// wraps
			for (int i = 0; i < fieldTypes.length; i++) {
				int columnId = requestedColumnIds[i];
				if (columnId != -1) {
					((OrcColumnVector) orcVectorWrappers[i]).setNullInfo(batch.cols[columnId]);
				}
			}
			return true;
		}

		for (int i = 0; i < fieldTypes.length; i++) {
			if (requestedColumnIds[i] >= 0) {
				// field exists
				org.apache.orc.storage.ql.exec.vector.ColumnVector fromColumn = batch.cols[requestedColumnIds[i]];
				if (fromColumn.isRepeating) {
					putRepeatingValues(batchSize, fieldTypes[i], fromColumn, columnarBatch.columns[i]);
				} else if (fromColumn.noNulls) {
					putNotNullValues(batchSize, fieldTypes[i], fromColumn, columnarBatch.columns[i]);
				} else {
					putValues(batchSize, fieldTypes[i], fromColumn, columnarBatch.columns[i]);
				}
			}
		}
		return true;
	}

	private void putRepeatingValues(
			int batchSize,
			DataType fieldType,
			org.apache.orc.storage.ql.exec.vector.ColumnVector fromColumn,
			ColumnVector toColumn) {
		if (fromColumn.isNull[0]) {
			Arrays.fill(toColumn.isNull, true);
		} else {
			if (fieldType instanceof BooleanType) {
				Arrays.fill(
					((BooleanColumnVector) toColumn).vector,
					((org.apache.orc.storage.ql.exec.vector.LongColumnVector) fromColumn).vector[0] == 1);
			} else if (fieldType instanceof ByteType) {
				Arrays.fill(
					((ByteColumnVector) toColumn).vector,
					(byte) ((org.apache.orc.storage.ql.exec.vector.LongColumnVector) fromColumn).vector[0]);
			} else if (fieldType instanceof ShortType) {
				Arrays.fill(
					((ShortColumnVector) toColumn).vector,
					(short) ((org.apache.orc.storage.ql.exec.vector.LongColumnVector) fromColumn).vector[0]);
			} else if (fieldType instanceof IntType) {
				Arrays.fill(
					((IntegerColumnVector) toColumn).vector,
					(int) ((org.apache.orc.storage.ql.exec.vector.LongColumnVector) fromColumn).vector[0]);
			} else if (fieldType instanceof LongType) {
				Arrays.fill(
					((LongColumnVector) toColumn).vector,
					((org.apache.orc.storage.ql.exec.vector.LongColumnVector) fromColumn).vector[0]);
			} else if (fieldType instanceof FloatType) {
				Arrays.fill(
					((FloatColumnVector) toColumn).vector,
					(float) ((org.apache.orc.storage.ql.exec.vector.DoubleColumnVector) fromColumn).vector[0]);
			} else if (fieldType instanceof DoubleType) {
				Arrays.fill(
					((DoubleColumnVector) toColumn).vector,
					((org.apache.orc.storage.ql.exec.vector.DoubleColumnVector) fromColumn).vector[0]);
			} else if (fieldType instanceof StringType || fieldType instanceof ByteArrayType) {
				for (int i = 0; i < batchSize; i++) {
					byte[][] data = ((org.apache.orc.storage.ql.exec.vector.BytesColumnVector) fromColumn).vector;
					int[] start = ((org.apache.orc.storage.ql.exec.vector.BytesColumnVector) fromColumn).start;
					int[] length = ((org.apache.orc.storage.ql.exec.vector.BytesColumnVector) fromColumn).length;
					((BytesColumnVector) toColumn).setVal(i, data[0], start[0], length[0]);
				}
			} else if (fieldType instanceof DateType) {
				Arrays.fill(
					((DateColumnVector) toColumn).vector,
					(int) ((org.apache.orc.storage.ql.exec.vector.LongColumnVector) fromColumn).vector[0]);
			} else if (fieldType instanceof TimestampType) {
				long time = ((org.apache.orc.storage.ql.exec.vector.TimestampColumnVector) fromColumn).time[0];
				int nanos = ((org.apache.orc.storage.ql.exec.vector.TimestampColumnVector) fromColumn).nanos[0];
				Arrays.fill(
					((TimestampColumnVector) toColumn).vector,
					time + nanos / 1000000);
			} else if (fieldType instanceof DecimalType) {
				org.apache.orc.storage.ql.exec.vector.DecimalColumnVector data =
					(org.apache.orc.storage.ql.exec.vector.DecimalColumnVector) fromColumn;

				int precision = ((DecimalType) fieldType).precision();
				int scale = ((DecimalType) fieldType).scale();
				putDecimalWritables(
					toColumn,
					batchSize,
					precision,
					scale,
					data.vector[0]);
			} else {
				throw new UnsupportedOperationException("Unsupported Data Type: " + fieldType);
			}
		}
	}

	private void putNotNullValues(
			int batchSize,
			DataType fieldType,
			org.apache.orc.storage.ql.exec.vector.ColumnVector fromColumn,
			ColumnVector toColumn) {
		if (fieldType instanceof BooleanType) {
			long[] data = ((org.apache.orc.storage.ql.exec.vector.LongColumnVector) fromColumn).vector;
			for (int i = 0; i < batchSize; i++) {
				((BooleanColumnVector) toColumn).vector[i] = (data[i] == 1);
			}
		} else if (fieldType instanceof ByteType) {
			long[] data = ((org.apache.orc.storage.ql.exec.vector.LongColumnVector) fromColumn).vector;
			for (int i = 0; i < batchSize; i++) {
				((ByteColumnVector) toColumn).vector[i] = (byte) data[i];
			}
		} else if (fieldType instanceof ShortType) {
			long[] data = ((org.apache.orc.storage.ql.exec.vector.LongColumnVector) fromColumn).vector;
			for (int i = 0; i < batchSize; i++) {
				((ShortColumnVector) toColumn).vector[i] = (short) data[i];
			}
		} else if (fieldType instanceof IntType) {
			long[] data = ((org.apache.orc.storage.ql.exec.vector.LongColumnVector) fromColumn).vector;
			for (int i = 0; i < batchSize; i++) {
				((IntegerColumnVector) toColumn).vector[i] = (int) data[i];
			}
		} else if (fieldType instanceof LongType) {
			long[] data = ((org.apache.orc.storage.ql.exec.vector.LongColumnVector) fromColumn).vector;
			System.arraycopy(data, 0, ((LongColumnVector) toColumn).vector, 0, batchSize);
		} else if (fieldType instanceof FloatType) {
			double[] data = ((org.apache.orc.storage.ql.exec.vector.DoubleColumnVector) fromColumn).vector;
			for (int i = 0; i < batchSize; i++) {
				((FloatColumnVector) toColumn).vector[i] = (float) data[i];
			}
		} else if (fieldType instanceof DoubleType) {
			double[] data = ((org.apache.orc.storage.ql.exec.vector.DoubleColumnVector) fromColumn).vector;
			System.arraycopy(data, 0, ((DoubleColumnVector) toColumn).vector, 0, batchSize);
		} else if (fieldType instanceof StringType || fieldType instanceof ByteArrayType) {
			byte[][] data = ((org.apache.orc.storage.ql.exec.vector.BytesColumnVector) fromColumn).vector;
			int[] start = ((org.apache.orc.storage.ql.exec.vector.BytesColumnVector) fromColumn).start;
			int[] length = ((org.apache.orc.storage.ql.exec.vector.BytesColumnVector) fromColumn).length;
			for (int i = 0; i < batchSize; i++) {
				((BytesColumnVector) toColumn).setVal(i, data[i], start[i], length[i]);
			}
		} else if (fieldType instanceof DateType) {
			long[] data = ((org.apache.orc.storage.ql.exec.vector.LongColumnVector) fromColumn).vector;
			for (int i = 0; i < batchSize; i++) {
				((DateColumnVector) toColumn).vector[i] = (int) data[i];
			}
		} else if (fieldType instanceof TimestampType) {
			long[] time = ((org.apache.orc.storage.ql.exec.vector.TimestampColumnVector) fromColumn).time;
			int[] nanos = ((org.apache.orc.storage.ql.exec.vector.TimestampColumnVector) fromColumn).nanos;
			for (int i = 0; i < batchSize; i++) {
				((TimestampColumnVector) toColumn).vector[i] = time[i] + nanos[i] / 1000000;
			}
		} else if (fieldType instanceof DecimalType) {
			org.apache.orc.storage.ql.exec.vector.DecimalColumnVector data =
				(org.apache.orc.storage.ql.exec.vector.DecimalColumnVector) fromColumn;

			int precision = ((DecimalType) fieldType).precision();
			int scale = ((DecimalType) fieldType).scale();
			for (int i = 0; i < batchSize; i++) {
				putDecimalWritable(
					toColumn,
					i,
					precision,
					scale,
					data.vector[i]);
			}
		} else {
			throw new UnsupportedOperationException("Unsupported Data Type: " + fieldType);
		}
	}

	private void putValues(
			int batchSize,
			DataType fieldType,
			org.apache.orc.storage.ql.exec.vector.ColumnVector fromColumn,
			ColumnVector toColumn) {

		System.arraycopy(fromColumn.isNull, 0, toColumn.isNull, 0, batchSize);
		toColumn.noNulls = fromColumn.noNulls;

		if (fieldType instanceof BooleanType) {
			long[] data = ((org.apache.orc.storage.ql.exec.vector.LongColumnVector) fromColumn).vector;
			for (int i = 0; i < batchSize; i++) {
				if (!fromColumn.isNull[i]) {
					((BooleanColumnVector) toColumn).vector[i] = (data[i] == 1);
				}
			}
		} else if (fieldType instanceof ByteType) {
			long[] data = ((org.apache.orc.storage.ql.exec.vector.LongColumnVector) fromColumn).vector;
			for (int i = 0; i < batchSize; i++) {
				if (!fromColumn.isNull[i]) {
					((ByteColumnVector) toColumn).vector[i] = (byte) data[i];
				}
			}
		} else if (fieldType instanceof ShortType) {
			long[] data = ((org.apache.orc.storage.ql.exec.vector.LongColumnVector) fromColumn).vector;
			for (int i = 0; i < batchSize; i++) {
				if (!fromColumn.isNull[i]) {
					((ShortColumnVector) toColumn).vector[i] = (short) data[i];
				}
			}
		} else if (fieldType instanceof IntType) {
			long[] data = ((org.apache.orc.storage.ql.exec.vector.LongColumnVector) fromColumn).vector;
			for (int i = 0; i < batchSize; i++) {
				if (!fromColumn.isNull[i]) {
					((IntegerColumnVector) toColumn).vector[i] = (int) data[i];
				}
			}
		} else if (fieldType instanceof LongType) {
			long[] data = ((org.apache.orc.storage.ql.exec.vector.LongColumnVector) fromColumn).vector;
			System.arraycopy(data, 0, ((LongColumnVector) toColumn).vector, 0, batchSize);
		} else if (fieldType instanceof FloatType) {
			double[] data = ((org.apache.orc.storage.ql.exec.vector.DoubleColumnVector) fromColumn).vector;
			for (int i = 0; i < batchSize; i++) {
				if (!fromColumn.isNull[i]) {
					((FloatColumnVector) toColumn).vector[i] = (float) data[i];
				}
			}
		} else if (fieldType instanceof DoubleType) {
			double[] data = ((org.apache.orc.storage.ql.exec.vector.DoubleColumnVector) fromColumn).vector;
			System.arraycopy(data, 0, ((DoubleColumnVector) toColumn).vector, 0, batchSize);
		} else if (fieldType instanceof StringType || fieldType instanceof ByteArrayType) {
			byte[][] data = ((org.apache.orc.storage.ql.exec.vector.BytesColumnVector) fromColumn).vector;
			int[] start = ((org.apache.orc.storage.ql.exec.vector.BytesColumnVector) fromColumn).start;
			int[] length = ((org.apache.orc.storage.ql.exec.vector.BytesColumnVector) fromColumn).length;
			for (int i = 0; i < batchSize; i++) {
				// TODO: bug？
				// if isNull(idx), should we set start[idx] and lenght[idx]?
				if (!fromColumn.isNull[i]) {
					((BytesColumnVector) toColumn).setVal(i, data[i], start[i], length[i]);
				}
			}
		} else if (fieldType instanceof DateType) {
			long[] data = ((org.apache.orc.storage.ql.exec.vector.LongColumnVector) fromColumn).vector;
			for (int i = 0; i < batchSize; i++) {
				if (!fromColumn.isNull[i]) {
					((DateColumnVector) toColumn).vector[i] = (int) data[i];
				}
			}
		} else if (fieldType instanceof TimestampType) {
			long[] time = ((org.apache.orc.storage.ql.exec.vector.TimestampColumnVector) fromColumn).time;
			int[] nanos = ((org.apache.orc.storage.ql.exec.vector.TimestampColumnVector) fromColumn).nanos;
			for (int i = 0; i < batchSize; i++) {
				if (!fromColumn.isNull[i]) {
					((TimestampColumnVector) toColumn).vector[i] = time[i] + nanos[i] / 1000000;
				}
			}
		} else if (fieldType instanceof DecimalType) {
			org.apache.orc.storage.ql.exec.vector.DecimalColumnVector data =
				(org.apache.orc.storage.ql.exec.vector.DecimalColumnVector) fromColumn;

			int precision = ((DecimalType) fieldType).precision();
			int scale = ((DecimalType) fieldType).scale();
			for (int i = 0; i < batchSize; i++) {
				if (!fromColumn.isNull[i]) {
					putDecimalWritable(
						toColumn,
						i,
						precision,
						scale,
						data.vector[i]);
				}
			}
		} else {
			throw new UnsupportedOperationException("Unsupported Data Type: " + fieldType);
		}
	}

	private void putDecimalWritables(
			ColumnVector toColumn,
			int batchSize,
			int precision,
			int scale,
			HiveDecimalWritable decimalWritable) {
		HiveDecimal decimal = decimalWritable.getHiveDecimal();
		Decimal value = Decimal.fromBigDecimal(decimal.bigDecimalValue(), precision, scale);
		if (Decimal.is32BitDecimal(precision)) {
			Arrays.fill(
				((IntegerColumnVector) toColumn).vector,
				(int) value.toUnscaledLong());
		} else if (Decimal.is64BitDecimal(precision)) {
			Arrays.fill(
				((LongColumnVector) toColumn).vector,
				value.toUnscaledLong());
		} else {
			byte[] bytes = value.toUnscaledBytes();
			for (int i = 0; i < batchSize; i++) {
				((BytesColumnVector) toColumn).setVal(i, bytes);
			}
		}
	}

	private void putDecimalWritable(
		ColumnVector toColumn,
		int index,
		int precision,
		int scale,
		HiveDecimalWritable decimalWritable) {
		HiveDecimal decimal = decimalWritable.getHiveDecimal();
		Decimal value = Decimal.fromBigDecimal(decimal.bigDecimalValue(), precision, scale);
		if (Decimal.is32BitDecimal(precision)) {
			((IntegerColumnVector) toColumn).vector[index] = (int) value.toUnscaledLong();
		} else if (Decimal.is64BitDecimal(precision)) {
			((LongColumnVector) toColumn).vector[index] = value.toUnscaledLong();
		} else {
			byte[] bytes = value.toUnscaledBytes();
			((BytesColumnVector) toColumn).setVal(index, bytes);
		}
	}
}
