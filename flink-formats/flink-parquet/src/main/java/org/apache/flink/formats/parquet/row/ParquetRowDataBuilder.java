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

package org.apache.flink.formats.parquet.row;

import org.apache.flink.formats.parquet.ParquetBuilder;
import org.apache.flink.formats.parquet.ParquetWriterFactory;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.types.logical.RowType;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.HashMap;

import static org.apache.flink.formats.parquet.utils.ParquetSchemaConverter.convertToParquetMessageType;
import static org.apache.parquet.hadoop.ParquetOutputFormat.MAX_PADDING_BYTES;
import static org.apache.parquet.hadoop.ParquetOutputFormat.getBlockSize;
import static org.apache.parquet.hadoop.ParquetOutputFormat.getDictionaryPageSize;
import static org.apache.parquet.hadoop.ParquetOutputFormat.getEnableDictionary;
import static org.apache.parquet.hadoop.ParquetOutputFormat.getPageSize;
import static org.apache.parquet.hadoop.ParquetOutputFormat.getValidation;
import static org.apache.parquet.hadoop.ParquetOutputFormat.getWriterVersion;
import static org.apache.parquet.hadoop.codec.CodecConfig.getParquetCompressionCodec;

/**
 * {@link BaseRow} of {@link ParquetWriter.Builder}.
 */
public class ParquetRowDataBuilder extends ParquetWriter.Builder<BaseRow, ParquetRowDataBuilder> {

	private final RowType rowType;
	private final boolean utcTimestamp;
	private final boolean isLegacyFormat;

	public ParquetRowDataBuilder(
			OutputFile path,
			RowType rowType,
			boolean utcTimestamp,
			boolean isLegacyFormat) {
		super(path);
		this.rowType = rowType;
		this.utcTimestamp = utcTimestamp;
		this.isLegacyFormat = isLegacyFormat;
	}

	@Override
	protected ParquetRowDataBuilder self() {
		return this;
	}

	@Override
	protected WriteSupport<BaseRow> getWriteSupport(Configuration conf) {
		return new ParquetWriteSupport();
	}

	private class ParquetWriteSupport extends WriteSupport<BaseRow> {

		private MessageType schema = convertToParquetMessageType(
				"flink_schema", rowType, isLegacyFormat);
		private ParquetRowDataWriter writer;

		@Override
		public WriteContext init(Configuration configuration) {
			return new WriteContext(schema, new HashMap<>());
		}

		@Override
		public void prepareForWrite(RecordConsumer recordConsumer) {
			this.writer = new ParquetRowDataWriter(
					recordConsumer,
					rowType,
					schema,
					utcTimestamp,
					isLegacyFormat);
		}

		@Override
		public void write(BaseRow record) {
			try {
				this.writer.write(record);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
	}

	public static ParquetWriterFactory<BaseRow> createWriterFactory(
			RowType rowType,
			Configuration conf,
			boolean utcTimestamp,
			boolean isLegacyFormat) {
		return new ParquetWriterFactory<>(
				new FlinkParquetBuilder(rowType, conf, utcTimestamp, isLegacyFormat));
	}

	/**
	 * Flink Row {@link ParquetBuilder}.
	 */
	public static class FlinkParquetBuilder implements ParquetBuilder<BaseRow> {

		private final RowType rowType;
		private final SerializableConfiguration configuration;
		private final boolean utcTimestamp;
		private final boolean isLegacyFormat;

		public FlinkParquetBuilder(
				RowType rowType,
				Configuration conf,
				boolean utcTimestamp,
				boolean isLegacyFormat) {
			this.rowType = rowType;
			this.configuration = new SerializableConfiguration(conf);
			this.utcTimestamp = utcTimestamp;
			this.isLegacyFormat = isLegacyFormat;
		}

		@Override
		public ParquetWriter<BaseRow> createWriter(OutputFile out) throws IOException {
			Configuration conf = configuration.conf();
			return new ParquetRowDataBuilder(out, rowType, utcTimestamp, isLegacyFormat)
					.withCompressionCodec(getParquetCompressionCodec(conf))
					.withRowGroupSize(getBlockSize(conf))
					.withPageSize(getPageSize(conf))
					.withDictionaryPageSize(getDictionaryPageSize(conf))
					.withMaxPaddingSize(conf.getInt(
							MAX_PADDING_BYTES, ParquetWriter.MAX_PADDING_SIZE_DEFAULT))
					.withDictionaryEncoding(getEnableDictionary(conf))
					.withValidation(getValidation(conf))
					.withWriterVersion(getWriterVersion(conf))
					.withConf(conf).build();
		}
	}

	/**
	 * Wrap {@link Configuration} to a serializable class.
	 */
	public static class SerializableConfiguration implements Serializable {

		private static final long serialVersionUID = 1L;

		private Configuration conf;

		public SerializableConfiguration(Configuration conf) {
			this.conf = conf;
		}

		public Configuration conf() {
			return conf;
		}

		private void writeObject(ObjectOutputStream out) throws IOException {
			conf.write(out);
		}

		private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
			if (conf == null) {
				conf = new Configuration();
			}
			conf.readFields(in);
		}
	}
}
