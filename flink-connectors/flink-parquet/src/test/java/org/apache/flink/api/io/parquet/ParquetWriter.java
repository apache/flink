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

package org.apache.flink.api.io.parquet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;

/**
 * A helper class to write data to Parquet File.
 */
public class ParquetWriter {

	private org.apache.parquet.hadoop.ParquetWriter<Group> realWriter;
	private SimpleGroupFactory factory;

	public ParquetWriter(
			MessageType parquetSchema,
			String path,
			ParquetFileWriter.Mode writeMode,
			CompressionCodecName compressionCodecName,
			int rowGroupSize,
			int pageSize,
			int dictionaryPageSize,
			int maxPaddingSize,
			boolean enableDictionary,
			boolean validating,
			ParquetProperties.WriterVersion writerVersion,
			Configuration conf
	) throws IOException {

		GroupWriteSupport.setSchema(parquetSchema, conf);

		realWriter = new Builder(new Path(path))
				.withWriteMode(writeMode)
				.withCompressionCodec(compressionCodecName)
				.withRowGroupSize(rowGroupSize)
				.withPageSize(pageSize)
				.withDictionaryPageSize(dictionaryPageSize)
				.withMaxPaddingSize(maxPaddingSize)
				.withDictionaryEncoding(enableDictionary)
				.withValidation(validating)
				.withWriterVersion(writerVersion)
				.withConf(conf)
				.build();

		factory = new SimpleGroupFactory(parquetSchema);
	}

	public ParquetWriter(
			MessageType parquetSchema,
			String path,
			Configuration conf
	) throws IOException {
		this(parquetSchema,
				path,
				ParquetFileWriter.Mode.OVERWRITE,
				CompressionCodecName.UNCOMPRESSED,
				2048, // row group size
				1024, // page size
				512, // dictionary page size
				0, // max padding size
				true,
				false,
				ParquetProperties.WriterVersion.PARQUET_2_0,
				conf);
	}

	public Group newGroup() {
		return factory.newGroup();
	}

	public void write(Group group) throws IOException {
		realWriter.write(group);
	}

	public void close() throws IOException {
		realWriter.close();
	}

	/** */
	private static class Builder extends org.apache.parquet.hadoop.ParquetWriter.Builder<Group, Builder> {

		private Builder(Path file) {
			super(file);
		}

		@Override
		protected Builder self() {
			return this;
		}

		@Override
		protected WriteSupport<Group> getWriteSupport(Configuration conf) {
			return new GroupWriteSupport();
		}
	}

}
