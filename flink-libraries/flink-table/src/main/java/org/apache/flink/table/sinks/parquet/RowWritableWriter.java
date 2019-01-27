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

package org.apache.flink.table.sinks.parquet;

import org.apache.flink.table.api.types.DataTypes;
import org.apache.flink.table.api.types.DecimalType;
import org.apache.flink.table.api.types.InternalType;
import org.apache.flink.table.api.types.RowType;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.Decimal;
import org.apache.flink.table.sources.parquet.ParquetSchemaConverter;
import org.apache.flink.util.Preconditions;

import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * RowWritableWriter sends a record to the Parquet API with the expected schema in order
 * to be written to a file.
 *
 * <p>This class is only used through RowWritableWriteSupport class.
 */
public class RowWritableWriter {
	private static final Logger LOG = LoggerFactory.getLogger(RowWritableWriter.class);
	protected final RecordConsumer recordConsumer;
	private final GroupType schema;

	/* This writer will be created when writing the first row in order to get
	information about how to inspect the record data.  */
	private RowWriter messageWriter;

	private InternalType[] fieldTypes;

	public RowWritableWriter(final InternalType[] fieldTypes, final RecordConsumer recordConsumer,
								final GroupType schema) {
		this.recordConsumer = recordConsumer;
		this.schema = schema;
		this.fieldTypes = fieldTypes;
	}

	/**
	 * It writes a record to Parquet.
	 *
	 * @param record Contains the record that is going to be written.
	 */
	public void write(final BaseRow record) {
		if (record != null) {
			if (messageWriter == null) {
				try {
					messageWriter = createMessageWriter(new RowType(fieldTypes), schema);
				} catch (RuntimeException e) {
					String errorMessage = "Parquet record is malformed: " + e.getMessage();
					LOG.error(errorMessage, e);
					throw new RuntimeException(errorMessage, e);
				}
			}
			messageWriter.write(record);
		}
	}

	private MessageRowWriter createMessageWriter(RowType rowTypeInfo, GroupType schema) {
		return new MessageRowWriter(rowTypeInfo, schema);
	}

	private FieldWriter createWriter(InternalType t, Type type) {
		if (type.isPrimitive()) {
			if (DataTypes.INT.equals(t)) {
				return new IntegerWriter();
			} else if (DataTypes.SHORT.equals(t)) {
				return new ShortWriter();
			} else if (DataTypes.BOOLEAN.equals(t)) {
				return new BooleanWriter();
			} else if (DataTypes.DOUBLE.equals(t)) {
				return new DoubleWriter();
			} else if (DataTypes.FLOAT.equals(t)) {
				return new FloatWriter();
			} else if (DataTypes.LONG.equals(t)) {
				return new LongWriter();
			} else if (DataTypes.STRING.equals(t)) {
				return new StringWriter();
			} else if (DataTypes.BYTE.equals(t)) {
				return new ByteWriter();
			} else if (DataTypes.TIMESTAMP.equals(t)) {
				return new TimestampWriter();
			} else if (DataTypes.DATE.equals(t)) {
				return new DateWriter();
			} else if (DataTypes.TIME.equals(t)) {
				return new TimeWriter();
			} else if (t instanceof DecimalType) {
				DecimalType decimalTypeInfo = (DecimalType) t;
				return makeDecimalWriter(decimalTypeInfo.precision(), decimalTypeInfo.scale());
			} else {
				throw new IllegalArgumentException("Unsupported  data type: " + t);
			}
		} else {
			throw new IllegalArgumentException("Unsupported  data type: " + t);
		}
	}

	private interface RowWriter {
		void write(BaseRow row);
	}

	private interface FieldWriter {
		void write(BaseRow row, int ordinal);
	}

	private class GroupRowWriter implements RowWriter {
		private RowType rowTypeInfo;
		private FieldWriter[] writers;
		private String[] fieldNames;

		public GroupRowWriter(RowType rowTypeInfo, GroupType groupType) {
			this.rowTypeInfo = rowTypeInfo;
			this.writers = new FieldWriter[rowTypeInfo.getFieldTypes().length];
			this.fieldNames = rowTypeInfo.getFieldNames();

			for (int i = 0; i < this.rowTypeInfo.getFieldTypes().length; i++) {
				writers[i] = createWriter(rowTypeInfo.getInternalTypeAt(i), groupType.getType(i));
			}
		}

		@Override
		public void write(BaseRow row) {
			for (int i = 0; i < writers.length; i++) {
				if (!row.isNullAt(i)) {
					String fieldName = fieldNames[i];
					FieldWriter writer = writers[i];

					recordConsumer.startField(fieldName, i);
					writer.write(row, i);
					recordConsumer.endField(fieldName, i);
				}
			}
		}
	}

	private class MessageRowWriter extends GroupRowWriter implements RowWriter {
		public MessageRowWriter(RowType rowTypeInfo, GroupType groupType) {
			super(rowTypeInfo, groupType);
		}

		@Override
		public void write(BaseRow row) {
			recordConsumer.startMessage();
			if (row != null) {
				super.write(row);
			}
			recordConsumer.endMessage();
		}
	}

	private class BooleanWriter implements FieldWriter {
		@Override
		public void write(BaseRow row, int ordinal) {
			recordConsumer.addBoolean(row.getBoolean(ordinal));
		}
	}

	private class ByteWriter implements FieldWriter {
		@Override
		public void write(BaseRow row, int ordinal) {
			recordConsumer.addInteger(row.getByte(ordinal));
		}
	}

	private class ShortWriter implements FieldWriter {
		@Override
		public void write(BaseRow row, int ordinal) {
			recordConsumer.addInteger(row.getShort(ordinal));
		}
	}

	private class LongWriter implements FieldWriter {
		@Override
		public void write(BaseRow row, int ordinal) {
			recordConsumer.addLong(row.getLong(ordinal));
		}
	}

	private class FloatWriter implements FieldWriter {
		@Override
		public void write(BaseRow row, int ordinal) {
			recordConsumer.addFloat(row.getFloat(ordinal));
		}
	}

	private class DoubleWriter implements FieldWriter {

		@Override
		public void write(BaseRow row, int ordinal) {
			recordConsumer.addDouble(row.getDouble(ordinal));
		}
	}

	private class StringWriter implements FieldWriter {
		@Override
		public void write(BaseRow row, int ordinal) {
			recordConsumer.addBinary(
				Binary.fromReusedByteArray(row.getBinaryString(ordinal).getBytes()));
		}
	}

	private class IntegerWriter implements FieldWriter {
		@Override
		public void write(BaseRow row, int ordinal) {
			recordConsumer.addInteger(row.getInt(ordinal));
		}
	}

	private class TimestampWriter implements FieldWriter {
		@Override
		public void write(BaseRow row, int ordinal) {
			recordConsumer.addLong(row.getLong(ordinal));
		}
	}

	private class TimeWriter implements FieldWriter {
		@Override
		public void write(BaseRow row, int ordinal) {
			recordConsumer.addInteger(row.getInt(ordinal));
		}
	}

	private class DateWriter implements FieldWriter {
		@Override
		public void write(BaseRow row, int ordinal) {
			recordConsumer.addInteger(row.getInt(ordinal));
		}
	}

	private FieldWriter makeDecimalWriter(int precision, int scale) {
		Preconditions.checkArgument(
			precision <= DecimalType.MAX_PRECISION,
			"Decimal precision %s exceeds max precision %s",
			precision, DecimalType.MAX_PRECISION);

		class Int32Writer implements FieldWriter {
			@Override
			public void write(BaseRow row, int ordinal) {
				long unscaledLong = row.getDecimal(ordinal, precision, scale).toUnscaledLong();
				recordConsumer.addInteger((int) unscaledLong);
			}
		}

		class Int64Writer implements FieldWriter {
			@Override
			public void write(BaseRow row, int ordinal) {
				long unscaledLong = row.getDecimal(ordinal, precision, scale).toUnscaledLong();
				recordConsumer.addLong(unscaledLong);
			}
		}

		class UnscaledBytesWriter implements FieldWriter {
			private final byte[] decimalBuffer;

			private UnscaledBytesWriter() {
				int maxNumBytes = ParquetSchemaConverter.computeMinBytesForPrecision(DecimalType.MAX_PRECISION);
				this.decimalBuffer = new byte[maxNumBytes];
			}

			@Override
			public void write(BaseRow row, int ordinal) {
				byte[] bytes = row.getDecimal(ordinal, precision, scale).toUnscaledBytes();
				int numBytes = ParquetSchemaConverter.computeMinBytesForPrecision(precision);
				byte[] fixedLengthBytes;
				if (bytes.length == numBytes) {
					// If the length of the underlying byte array of the unscaled `BigInteger` happens to be
					// `numBytes`, just reuse it, so that we don't bother copying it to `decimalBuffer`.
					fixedLengthBytes = bytes;
				} else {
					// Otherwise, the length must be less than `numBytes`.  In this case we copy contents of
					// the underlying bytes with padding sign bytes to `decimalBuffer` to form the result
					// fixed-length byte array.
					byte signByte = bytes[0] < 0 ? (byte) -1 : (byte) 0;
					Arrays.fill(decimalBuffer, 0, numBytes - bytes.length, signByte);
					System.arraycopy(bytes, 0, decimalBuffer, numBytes - bytes.length, bytes.length);
					fixedLengthBytes = decimalBuffer;
				}
				recordConsumer.addBinary(Binary.fromReusedByteArray(fixedLengthBytes, 0, numBytes));
			}
		}

		// 1 <= precision <= 9, writes as INT32
		if (Decimal.is32BitDecimal(precision)) {
			return new Int32Writer();
		}

		// 10 <= precision <= 18, writes as INT64
		if (Decimal.is64BitDecimal(precision)) {
			return new Int64Writer();
		}

		// 19 <= precision <= 38, writes as FIXED_LEN_BYTE_ARRAY
		return new UnscaledBytesWriter();
	}

}
