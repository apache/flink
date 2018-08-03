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

package org.apache.flink.formats.parquet;

import org.apache.flink.api.common.io.CheckpointableInputFormat;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.utils.ParquetRecordReader;
import org.apache.flink.formats.parquet.utils.ParquetSchemaConverter;
import org.apache.flink.formats.parquet.utils.RowReadSupport;
import org.apache.flink.metrics.Counter;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * The base InputFormat class to read from Parquet files.
 * For specific return types the {@link #convert(Row)} method need to be implemented.
 *
 * <P>Using {@link ParquetRecordReader} to Read files instead of {@link org.apache.flink.core.fs.FSDataInputStream},
 * we override {@link #open(FileInputSplit)} and {@link #close()} to change the behaviors.
 */
public abstract class ParquetInputFormat<E> extends FileInputFormat<E> implements
	CheckpointableInputFormat<FileInputSplit, Tuple2<Long, Long>> {

	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(ParquetInputFormat.class);

	private transient Counter recordConsumed;

	protected RowTypeInfo readType;

	protected final TypeInformation[] fieldTypes;

	protected final String[] fieldNames;

	protected transient ParquetRecordReader<Row> parquetRecordReader;

	protected transient long recordsReadSinceLastSync;

	protected long lastSyncedBlock = -1L;

	protected ParquetInputFormat(
		Path path, TypeInformation[] fieldTypes, String[] fieldNames) {
		super(path);
		this.readType = new RowTypeInfo(fieldTypes, fieldNames);
		this.fieldTypes = readType.getFieldTypes();
		this.fieldNames = readType.getFieldNames();
		this.unsplittable = true;
	}

	@Override
	public Tuple2<Long, Long> getCurrentState() {
		return new Tuple2<>(this.lastSyncedBlock, this.recordsReadSinceLastSync);
	}

	@Override
	public void open(FileInputSplit split) throws IOException {
		org.apache.hadoop.conf.Configuration configuration = new org.apache.hadoop.conf.Configuration();
		InputFile inputFile =
			HadoopInputFile.fromPath(new org.apache.hadoop.fs.Path(split.getPath().toUri()), configuration);
		ParquetReadOptions options = ParquetReadOptions.builder().build();
		ParquetFileReader fileReader = new ParquetFileReader(inputFile, options);
		MessageType schema = fileReader.getFileMetaData().getSchema();
		checkSchema(schema);
		MessageType readSchema = ParquetSchemaConverter.toParquetType(readType);
		this.parquetRecordReader = new ParquetRecordReader<>(new RowReadSupport(), readSchema);
		this.parquetRecordReader.initialize(fileReader, configuration);
		if (this.recordConsumed == null) {
			this.recordConsumed = getRuntimeContext().getMetricGroup().counter("parquet-record-consumed");
		}
		LOG.debug(String.format("Open ParquetRowInputFormat with FileInputSplit [%s]", split.getPath().toString()));
	}

	@Override
	public void reopen(FileInputSplit split, Tuple2<Long, Long> state) throws IOException {
		Preconditions.checkNotNull(split, "reopen() cannot be called on a null split.");
		Preconditions.checkNotNull(state, "reopen() cannot be called with a null initial state.");

		try {
			this.open(split);
		} finally {
			if (getCurrentState().f0 != -1) {
				lastSyncedBlock = state.f0;
				recordsReadSinceLastSync = state.f1;
			}
		}

		if (lastSyncedBlock != -1) {
			// open and read util the record we were before
			// the checkpoint and discard the values
			parquetRecordReader.seek(lastSyncedBlock);
			for (int i = 0; i < recordsReadSinceLastSync; i++) {
				// skip the record already processed
				parquetRecordReader.nextRecord();
			}
		}
	}

	@Override
	public void close() throws IOException {
		if (parquetRecordReader != null) {
			parquetRecordReader.close();
		}
	}

	@Override
	public boolean reachedEnd() throws IOException {
		return parquetRecordReader.reachEnd();
	}

	@Override
	public E nextRecord(E e) throws IOException {
		if (reachedEnd()) {
			return null;
		}

		// Means start to create a new block
		if (parquetRecordReader.getCurrentBlock() != lastSyncedBlock) {
			lastSyncedBlock = parquetRecordReader.getCurrentBlock();
			recordsReadSinceLastSync = 0;
		}

		if (parquetRecordReader.hasNextRecord()) {
			recordsReadSinceLastSync++;
			recordConsumed.inc();
			return convert(parquetRecordReader.nextRecord());
		}

		return null;
	}

	protected abstract E convert(Row row);

	private void checkSchema(MessageType schema) {
		RowTypeInfo rootTypeInfo = (RowTypeInfo) ParquetSchemaConverter.fromParquetType(schema);

		for (int i = 0; i < fieldNames.length; ++i) {
			String readFieldName = fieldNames[i];
			TypeInformation<?> readFieldType = fieldTypes[i];
			if (rootTypeInfo.getFieldIndex(readFieldName) < 0) {
				throw new IllegalArgumentException(readFieldName + " can not be found in parquet schema");
			}

			if (!readFieldType.equals(rootTypeInfo.getTypeAt(readFieldName))) {
				throw new IllegalArgumentException(readFieldName + " can not be converted to " + readFieldType);
			}
		}
	}
}
