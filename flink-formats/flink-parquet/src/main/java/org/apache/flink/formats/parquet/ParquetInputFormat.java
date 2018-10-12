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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.utils.ParquetRecordReader;
import org.apache.flink.formats.parquet.utils.ParquetSchemaConverter;
import org.apache.flink.formats.parquet.utils.RowReadSupport;
import org.apache.flink.metrics.Counter;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The base InputFormat class to read from Parquet files.
 * For specific return types the {@link #convert(Row)} method need to be implemented.
 *
 * <P>Using {@link ParquetRecordReader} to read files instead of {@link org.apache.flink.core.fs.FSDataInputStream},
 * we override {@link #open(FileInputSplit)} and {@link #close()} to change the behaviors.
 *
 * @param <E> The type of record to read.
 */
public abstract class ParquetInputFormat<E>
	extends FileInputFormat<E>
	implements CheckpointableInputFormat<FileInputSplit, Tuple2<Long, Long>> {

	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(ParquetInputFormat.class);

	private transient Counter recordConsumed;

	private transient MessageType expectedFileSchema;

	private TypeInformation[] fieldTypes;

	private String[] fieldNames;

	private boolean skipThisSplit = false;

	private transient ParquetRecordReader<Row> parquetRecordReader;


	/**
	 * Read parquet files with given parquet file schema.
	 *
	 * @param path The path of the file to read.
	 * @param messageType schema of parquet file
	 */

	protected ParquetInputFormat(Path path, MessageType messageType) {
		super(path);
		this.expectedFileSchema = checkNotNull(messageType, "messageType");
		RowTypeInfo rowTypeInfo = (RowTypeInfo) ParquetSchemaConverter.fromParquetType(expectedFileSchema);
		this.fieldTypes = rowTypeInfo.getFieldTypes();
		this.fieldNames = rowTypeInfo.getFieldNames();
		// read whole parquet file as one file split
		this.unsplittable = true;
	}

	@Override
	public void configure(Configuration parameters) {
		super.configure(parameters);
		parquetRecordReader.setSkipCorruptedRecord(this.skipCorruptedRecord);
	}

	public void selectFields(String[] fieldNames) {
		Preconditions.checkNotNull("Select field array can't be NULL");
		this.fieldNames = fieldNames;
		RowTypeInfo rowTypeInfo = (RowTypeInfo) ParquetSchemaConverter.fromParquetType(expectedFileSchema);
		TypeInformation[] selectFieldTypes = new TypeInformation[fieldNames.length];
		for (int i = 0; i < fieldNames.length; i++) {
			selectFieldTypes[i] = rowTypeInfo.getTypeAt(fieldNames[i]);
		}
		this.fieldTypes = selectFieldTypes;
	}

	@Override
	public Tuple2<Long, Long> getCurrentState() {
		return new Tuple2<>(this.parquetRecordReader.getCurrentBlock(),
			this.parquetRecordReader.getRecordInCurrentBlock());
	}

	@Override
	public void open(FileInputSplit split) throws IOException {
		org.apache.hadoop.conf.Configuration configuration = new org.apache.hadoop.conf.Configuration();
		InputFile inputFile =
			HadoopInputFile.fromPath(new org.apache.hadoop.fs.Path(split.getPath().toUri()), configuration);
		ParquetReadOptions options = ParquetReadOptions.builder().build();
		ParquetFileReader fileReader = new ParquetFileReader(inputFile, options);
		MessageType schema = fileReader.getFileMetaData().getSchema();
		this.skipThisSplit = false;
		MessageType readSchema = getReadSchema(schema);
		this.parquetRecordReader = new ParquetRecordReader<>(new RowReadSupport(), readSchema, FilterCompat.NOOP);
		this.parquetRecordReader.initialize(fileReader, configuration);
		if (this.recordConsumed == null) {
			this.recordConsumed = getRuntimeContext().getMetricGroup().counter("parquet-records-consumed");
		}

		LOG.debug(String.format("Open ParquetInputFormat with FileInputSplit [%s]", split.getPath().toString()));
		if (skipThisSplit) {
			LOG.warn(String.format(
				"Escaped the file split [%s] due to mismatch of file schema to expected result schema",
				split.getPath().toString()));
		}
	}

	@Override
	public void reopen(FileInputSplit split, Tuple2<Long, Long> state) throws IOException {
		Preconditions.checkNotNull(split, "reopen() cannot be called on a null split.");
		Preconditions.checkNotNull(state, "reopen() cannot be called with a null initial state.");

		try {
			this.open(split);
		} finally {
			if (state.f0 != -1) {
				// open and read util the record we were before
				// the checkpoint and discard the values
				parquetRecordReader.seek(state.f0, state.f1);
			}
		}
	}

	/**
	 * Get field names of read result.
	 *
	 * @return field names array
	 */
	protected String[] getFieldNames() {
		return fieldNames;
	}

	/**
	 * Get field types of read result.
	 *
	 * @return field types array
	 */
	protected TypeInformation[] getFieldTypes() {
		return fieldTypes;
	}

	@Override
	public void close() throws IOException {
		if (parquetRecordReader != null) {
			parquetRecordReader.close();
		}
	}

	@Override
	public boolean reachedEnd() throws IOException {
		if (skipThisSplit) {
			return true;
		}
		return parquetRecordReader.reachEnd();
	}

	@Override
	public E nextRecord(E e) throws IOException {
		if (reachedEnd()) {
			return null;
		}

		if (parquetRecordReader.hasNextRecord()) {
			recordConsumed.inc();
			return convert(parquetRecordReader.nextRecord());
		}

		LOG.warn(String.format("Try to read next record in the end of a split. This should not happen!"));
		return null;
	}

	/**
	 * This ParquetInputFormat read parquet record as Row by default. Sub classes of it can extend this method
	 * to further convert row to other types, such as POJO, Map or Tuple.
	 *
	 * @param row row read from parquet file
	 * @return E target result type
	 */
	protected abstract E convert(Row row);

	private MessageType getReadSchema(MessageType schema) {
		RowTypeInfo rootTypeInfo = (RowTypeInfo) ParquetSchemaConverter.fromParquetType(schema);
		List<Type> types = new ArrayList<>();
		for (int i = 0; i < fieldNames.length; ++i) {
			String readFieldName = fieldNames[i];
			TypeInformation<?> readFieldType = fieldTypes[i];
			if (rootTypeInfo.getFieldIndex(readFieldName) < 0) {
				if (!skipWrongSchemaFileSplit) {
					throw new IllegalArgumentException(readFieldName + " can not be found in parquet schema");
				} else {
					this.skipThisSplit = true;
					return schema;
				}
			}

			if (!readFieldType.equals(rootTypeInfo.getTypeAt(readFieldName))) {
				if (!skipWrongSchemaFileSplit) {
					throw new IllegalArgumentException(readFieldName + " can not be converted to " + readFieldType);
				} else {
					this.skipThisSplit = true;
					return schema;
				}
			}
			types.add(schema.getType(readFieldName));
		}

		return new MessageType(schema.getName(), types);
	}
}
