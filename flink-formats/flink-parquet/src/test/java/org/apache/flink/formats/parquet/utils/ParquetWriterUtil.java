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

package org.apache.flink.formats.parquet.utils;

import org.apache.flink.core.fs.Path;
import org.apache.flink.types.Row;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import static org.apache.flink.formats.parquet.vector.reader.TimestampColumnReader.JULIAN_EPOCH_OFFSET_DAYS;
import static org.apache.flink.formats.parquet.vector.reader.TimestampColumnReader.MILLIS_IN_DAY;
import static org.apache.flink.formats.parquet.vector.reader.TimestampColumnReader.NANOS_PER_SECOND;

/**
 * Parquet writer util to write Row to file.
 */
public class ParquetWriterUtil {

	public static Path createTempParquetFile(File folder, MessageType schema, List<Row> records, int rowGroupSize) throws IOException {
		Path path = new Path(folder.getPath(), UUID.randomUUID().toString());
		WriteSupport<Row> support = new WriteSupport<Row>() {
			private RecordConsumer consumer;

			@Override
			public WriteContext init(Configuration configuration) {
				return new WriteContext(schema, new HashMap<>());
			}

			@Override
			public void prepareForWrite(RecordConsumer consumer) {
				this.consumer = consumer;
			}

			@Override
			public void write(Row row) {
				consumer.startMessage();
				for (int i = 0; i < row.getArity(); i++) {
					PrimitiveType type = schema.getColumns().get(i).getPrimitiveType();
					Object field = row.getField(i);
					if (field != null) {
						consumer.startField("f" + i, i);
						switch (type.getPrimitiveTypeName()) {
							case INT64:
								consumer.addLong(((Number) field).longValue());
								break;
							case INT32:
								consumer.addInteger(((Number) field).intValue());
								break;
							case BOOLEAN:
								consumer.addBoolean((Boolean) field);
								break;
							case BINARY:
								if (field instanceof String) {
									field = ((String) field).getBytes(StandardCharsets.UTF_8);
								} else if (field instanceof BigDecimal) {
									field = ((BigDecimal) field).unscaledValue().toByteArray();
								}
								consumer.addBinary(Binary.fromConstantByteArray((byte[]) field));
								break;
							case FLOAT:
								consumer.addFloat(((Number) field).floatValue());
								break;
							case DOUBLE:
								consumer.addDouble(((Number) field).doubleValue());
								break;
							case INT96:
								consumer.addBinary(timestampToInt96((LocalDateTime) field));
								break;
							case FIXED_LEN_BYTE_ARRAY:
								byte[] bytes = ((BigDecimal) field).unscaledValue().toByteArray();
								byte signByte = (byte) (bytes[0] < 0 ? -1 : 0);
								int numBytes = 16;
								byte[] newBytes = new byte[numBytes];
								Arrays.fill(newBytes, 0, numBytes - bytes.length, signByte);
								System.arraycopy(bytes, 0, newBytes, numBytes - bytes.length, bytes.length);
								consumer.addBinary(Binary.fromConstantByteArray(newBytes));
								break;
						}
						consumer.endField("f" + i, i);
					}
				}
				consumer.endMessage();
			}
		};
		ParquetWriter<Row> writer = new ParquetWriterBuilder(
				new org.apache.hadoop.fs.Path(path.getPath()), support)
				.withRowGroupSize(rowGroupSize)
				.build();

		for (Row record : records) {
			writer.write(record);
		}

		writer.close();
		return path;
	}

	private static class ParquetWriterBuilder extends ParquetWriter.Builder<Row, ParquetWriterBuilder> {

		private final WriteSupport<Row> support;

		private ParquetWriterBuilder(org.apache.hadoop.fs.Path path, WriteSupport<Row> support) {
			super(path);
			this.support = support;
		}

		@Override
		protected ParquetWriterBuilder self() {
			return this;
		}

		@Override
		protected WriteSupport<Row> getWriteSupport(Configuration conf) {
			return support;
		}
	}

	private static Binary timestampToInt96(LocalDateTime time) {
		Timestamp timestamp = Timestamp.valueOf(time);
		long mills = timestamp.getTime();
		int julianDay = (int) ((mills / MILLIS_IN_DAY) + JULIAN_EPOCH_OFFSET_DAYS);
		long nanosOfDay = ((mills % MILLIS_IN_DAY) / 1000) * NANOS_PER_SECOND + timestamp.getNanos();

		ByteBuffer buf = ByteBuffer.allocate(12);
		buf.order(ByteOrder.LITTLE_ENDIAN);
		buf.putLong(nanosOfDay);
		buf.putInt(julianDay);
		buf.flip();
		return Binary.fromConstantByteBuffer(buf);
	}
}
