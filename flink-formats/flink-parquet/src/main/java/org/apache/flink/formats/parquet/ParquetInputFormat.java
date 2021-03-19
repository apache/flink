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

import org.apache.flink.annotation.VisibleForTesting;
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
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The base InputFormat class to read from Parquet files. For specific return types the {@link
 * #convert(Row)} method need to be implemented.
 *
 * <p>Using {@link ParquetRecordReader} to read files instead of {@link
 * org.apache.flink.core.fs.FSDataInputStream}, we override {@link #open(FileInputSplit)} and {@link
 * #close()} to change the behaviors.
 *
 * @param <E> The type of record to read.
 */
public abstract class ParquetInputFormat<E> extends FileInputFormat<E>
        implements CheckpointableInputFormat<FileInputSplit, Tuple2<Long, Long>> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(ParquetInputFormat.class);

    /** The flag to specify whether to skip file splits with wrong schema. */
    private boolean skipWrongSchemaFileSplit = false;

    /** The flag to specify whether to skip corrupted record. */
    private boolean skipCorruptedRecord = false;

    /** The flag to track that the current split should be skipped. */
    private boolean skipThisSplit = false;

    @Nullable
    /**
     * Fields to read types. They can be null if the user did not provide a schema, in that case the
     * types are extracted from the file itself
     */
    private TypeInformation[] fieldTypes;

    @Nullable
    /**
     * Fields to read. They can be null if the user provided neither the expected schema nor the
     * projected fields
     */
    private String[] fieldNames;

    private FilterPredicate filterPredicate;

    private transient Counter recordConsumed;

    @Nullable
    /**
     * User provided schema. It can be null if the user did not provide a schema, then the schema
     * will be determined out of the file itself
     */
    private transient MessageType expectedFileSchema;

    private transient ParquetRecordReader<Row> parquetRecordReader;

    /**
     * Read parquet files with given parquet file schema.
     *
     * @param path The path of the file to read.
     * @param messageType schema of parquet file
     */
    protected ParquetInputFormat(Path path, MessageType messageType) {
        super(path);
        this.expectedFileSchema = messageType;
        if (expectedFileSchema != null) {
            RowTypeInfo rowTypeInfo =
                    (RowTypeInfo) ParquetSchemaConverter.fromParquetType(expectedFileSchema);
            this.fieldTypes = rowTypeInfo.getFieldTypes();
            this.fieldNames = rowTypeInfo.getFieldNames();
        }
        // read whole parquet file as one file split
        this.unsplittable = true;
    }

    @Override
    public void configure(Configuration parameters) {
        super.configure(parameters);

        if (!this.skipWrongSchemaFileSplit) {
            this.skipWrongSchemaFileSplit =
                    parameters.getBoolean(PARQUET_SKIP_WRONG_SCHEMA_SPLITS, false);
        }

        if (this.skipCorruptedRecord) {
            this.skipCorruptedRecord = parameters.getBoolean(PARQUET_SKIP_CORRUPTED_RECORD, false);
        }
    }

    /**
     * Configures the fields to be read and returned by the ParquetInputFormat. Selected fields must
     * be present in the configured schema.
     *
     * @param fieldNames Names of all selected fields.
     */
    public void selectFields(String[] fieldNames) {
        checkNotNull(fieldNames, "fieldNames");
        this.fieldNames = fieldNames;
        if (expectedFileSchema != null) {
            this.fieldTypes = getFieldTypesFromSchema(fieldNames, expectedFileSchema);
        }
    }

    private TypeInformation[] getFieldTypesFromSchema(String[] fieldNames, MessageType schema) {
        RowTypeInfo rowTypeInfo = (RowTypeInfo) ParquetSchemaConverter.fromParquetType(schema);
        TypeInformation[] selectFieldTypes = new TypeInformation[fieldNames.length];
        for (int i = 0; i < fieldNames.length; i++) {
            try {
                selectFieldTypes[i] = rowTypeInfo.getTypeAt(fieldNames[i]);
            } catch (IndexOutOfBoundsException e) {
                throw new IllegalArgumentException(
                        String.format(
                                "Fail to access Field %s , "
                                        + "which is not contained in the file schema",
                                fieldNames[i]),
                        e);
            }
        }
        return selectFieldTypes;
    }

    public void setFilterPredicate(FilterPredicate filterPredicate) {
        this.filterPredicate = filterPredicate;
    }

    @Override
    public Tuple2<Long, Long> getCurrentState() {
        return parquetRecordReader.getCurrentReadPosition();
    }

    @Override
    public void open(FileInputSplit split) throws IOException {
        // reset the flag when open a new split
        this.skipThisSplit = false;
        org.apache.hadoop.conf.Configuration configuration =
                new org.apache.hadoop.conf.Configuration();
        InputFile inputFile =
                HadoopInputFile.fromPath(
                        new org.apache.hadoop.fs.Path(split.getPath().toUri()), configuration);
        ParquetReadOptions options = ParquetReadOptions.builder().build();
        ParquetFileReader fileReader = new ParquetFileReader(inputFile, options);
        MessageType fileSchema = fileReader.getFileMetaData().getSchema();
        if (expectedFileSchema == null) { // user did not provide a read schema, use the file schema
            if (fieldNames == null) { // user did not provide projected fields, use all the fields
                RowTypeInfo rowTypeInfo =
                        (RowTypeInfo) ParquetSchemaConverter.fromParquetType(fileSchema);
                fieldNames = rowTypeInfo.getFieldNames();
                fieldTypes = rowTypeInfo.getFieldTypes();
            } else { // user provided projected fields, get the corresponding types from file schema
                fieldTypes = getFieldTypesFromSchema(fieldNames, fileSchema);
            }
        }
        MessageType readSchema = getReadSchema(fileSchema, split.getPath());
        if (skipThisSplit) {
            LOG.warn(
                    String.format(
                            "Escaped the file split [%s] due to mismatch of file schema to expected result schema",
                            split.getPath().toString()));
        } else {
            this.parquetRecordReader =
                    new ParquetRecordReader<>(
                            new RowReadSupport(),
                            readSchema,
                            filterPredicate == null
                                    ? FilterCompat.NOOP
                                    : FilterCompat.get(filterPredicate));
            this.parquetRecordReader.initialize(fileReader, configuration);
            this.parquetRecordReader.setSkipCorruptedRecord(this.skipCorruptedRecord);

            if (this.recordConsumed == null) {
                this.recordConsumed =
                        getRuntimeContext().getMetricGroup().counter("parquet-records-consumed");
            }

            LOG.debug(
                    String.format(
                            "Open ParquetInputFormat with FileInputSplit [%s]",
                            split.getPath().toString()));
        }
    }

    @Override
    public void reopen(FileInputSplit split, Tuple2<Long, Long> state) throws IOException {
        Preconditions.checkNotNull(split, "reopen() cannot be called on a null split.");
        Preconditions.checkNotNull(state, "reopen() cannot be called with a null initial state.");
        this.open(split);
        // seek to the read position in the split that we were at when the checkpoint was taken.
        parquetRecordReader.seek(state.f0, state.f1);
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

    @VisibleForTesting
    protected FilterPredicate getPredicate() {
        return this.filterPredicate;
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

        recordConsumed.inc();
        return convert(parquetRecordReader.nextRecord());
    }

    /**
     * This ParquetInputFormat read parquet record as Row by default. Sub classes of it can extend
     * this method to further convert row to other types, such as POJO, Map or Tuple.
     *
     * @param row row read from parquet file
     * @return E target result type
     */
    protected abstract E convert(Row row);

    /**
     * Generates and returns the read schema based on the projected fields for a given file.
     *
     * @param fileSchema The schema of the given file.
     * @param filePath The path of the given file.
     * @return The read schema based on the given file's schema and the projected fields.
     */
    private MessageType getReadSchema(MessageType fileSchema, Path filePath) {
        RowTypeInfo fileTypeInfo = (RowTypeInfo) ParquetSchemaConverter.fromParquetType(fileSchema);
        List<Type> types = new ArrayList<>();
        for (int i = 0; i < fieldNames.length; ++i) {
            String readFieldName = fieldNames[i];
            TypeInformation<?> readFieldType = fieldTypes[i];
            if (fileTypeInfo.getFieldIndex(readFieldName) < 0) {
                if (!skipWrongSchemaFileSplit) {
                    throw new IllegalArgumentException(
                            "Field "
                                    + readFieldName
                                    + " cannot be found in schema of "
                                    + " Parquet file: "
                                    + filePath
                                    + ".");
                } else {
                    this.skipThisSplit = true;
                    return fileSchema;
                }
            }

            if (!readFieldType.equals(fileTypeInfo.getTypeAt(readFieldName))) {
                if (!skipWrongSchemaFileSplit) {
                    throw new IllegalArgumentException(
                            "Expecting type "
                                    + readFieldType
                                    + " for field "
                                    + readFieldName
                                    + " but found type "
                                    + fileTypeInfo.getTypeAt(readFieldName)
                                    + " in Parquet file: "
                                    + filePath
                                    + ".");
                } else {
                    this.skipThisSplit = true;
                    return fileSchema;
                }
            }
            types.add(fileSchema.getType(readFieldName));
        }

        return new MessageType(fileSchema.getName(), types);
    }

    /** The config parameter which defines whether to skip file split with wrong schema. */
    public static final String PARQUET_SKIP_WRONG_SCHEMA_SPLITS = "skip.splits.wrong.schema";

    /** The config parameter which defines whether to skip corrupted record. */
    public static final String PARQUET_SKIP_CORRUPTED_RECORD = "skip.corrupted.record";
}
