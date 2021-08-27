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

package org.apache.flink.formats.avro;

import org.apache.flink.api.common.io.CheckpointableInputFormat;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.avro.utils.FSDataInputStreamWrapper;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.reflect.ReflectDatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Provides a {@link FileInputFormat} for Avro records.
 *
 * @param <E> the type of the result Avro record. If you specify {@link GenericRecord} then the
 *     result will be returned as a {@link GenericRecord}, so you do not have to know the schema
 *     ahead of time.
 */
public class AvroInputFormat<E> extends FileInputFormat<E>
        implements ResultTypeQueryable<E>,
                CheckpointableInputFormat<FileInputSplit, Tuple2<Long, Long>> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(AvroInputFormat.class);

    private final Class<E> avroValueType;

    private boolean reuseAvroValue = true;

    private transient DataFileReader<E> dataFileReader;

    private transient long end;

    private transient long recordsReadSinceLastSync;

    private long lastSync = -1L;

    public AvroInputFormat(Path filePath, Class<E> type) {
        super(filePath);
        this.avroValueType = type;
    }

    /**
     * Sets the flag whether to reuse the Avro value instance for all records. By default, the input
     * format reuses the Avro value.
     *
     * @param reuseAvroValue True, if the input format should reuse the Avro value instance, false
     *     otherwise.
     */
    public void setReuseAvroValue(boolean reuseAvroValue) {
        this.reuseAvroValue = reuseAvroValue;
    }

    /** If set, the InputFormat will only read entire files. */
    public void setUnsplittable(boolean unsplittable) {
        this.unsplittable = unsplittable;
    }

    // --------------------------------------------------------------------------------------------
    // Typing
    // --------------------------------------------------------------------------------------------

    @Override
    public TypeInformation<E> getProducedType() {
        return TypeExtractor.getForClass(this.avroValueType);
    }

    // --------------------------------------------------------------------------------------------
    // Input Format Methods
    // --------------------------------------------------------------------------------------------

    @Override
    public void open(FileInputSplit split) throws IOException {
        super.open(split);
        dataFileReader = initReader(split);
        dataFileReader.sync(split.getStart());
        lastSync = dataFileReader.previousSync();
    }

    private DataFileReader<E> initReader(FileInputSplit split) throws IOException {
        DatumReader<E> datumReader;

        if (org.apache.avro.generic.GenericRecord.class == avroValueType) {
            datumReader = new GenericDatumReader<E>();
        } else {
            datumReader =
                    org.apache.avro.specific.SpecificRecordBase.class.isAssignableFrom(
                                    avroValueType)
                            ? new SpecificDatumReader<E>(avroValueType)
                            : new ReflectDatumReader<E>(avroValueType);
        }
        if (LOG.isInfoEnabled()) {
            LOG.info("Opening split {}", split);
        }

        SeekableInput in =
                new FSDataInputStreamWrapper(
                        stream,
                        split.getPath().getFileSystem().getFileStatus(split.getPath()).getLen());
        DataFileReader<E> dataFileReader =
                (DataFileReader) DataFileReader.openReader(in, datumReader);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Loaded SCHEMA: {}", dataFileReader.getSchema());
        }

        end = split.getStart() + split.getLength();
        recordsReadSinceLastSync = 0;
        return dataFileReader;
    }

    @Override
    public boolean reachedEnd() throws IOException {
        return !dataFileReader.hasNext() || dataFileReader.pastSync(end);
    }

    public long getRecordsReadFromBlock() {
        return this.recordsReadSinceLastSync;
    }

    @Override
    public E nextRecord(E reuseValue) throws IOException {
        if (reachedEnd()) {
            return null;
        }

        // if we start a new block, then register the event, and
        // restart the counter.
        if (dataFileReader.previousSync() != lastSync) {
            lastSync = dataFileReader.previousSync();
            recordsReadSinceLastSync = 0;
        }
        recordsReadSinceLastSync++;

        if (reuseAvroValue) {
            return dataFileReader.next(reuseValue);
        } else {
            if (GenericRecord.class == avroValueType) {
                return dataFileReader.next();
            } else {
                return dataFileReader.next(
                        InstantiationUtil.instantiate(avroValueType, Object.class));
            }
        }
    }

    @Override
    public boolean supportsMultiPaths() {
        return true;
    }

    // --------------------------------------------------------------------------------------------
    //  Checkpointing
    // --------------------------------------------------------------------------------------------

    @Override
    public Tuple2<Long, Long> getCurrentState() throws IOException {
        return new Tuple2<>(this.lastSync, this.recordsReadSinceLastSync);
    }

    @Override
    public void reopen(FileInputSplit split, Tuple2<Long, Long> state) throws IOException {
        Preconditions.checkNotNull(split, "reopen() cannot be called on a null split.");
        Preconditions.checkNotNull(state, "reopen() cannot be called with a null initial state.");

        try {
            this.open(split);
        } finally {
            if (state.f0 != -1) {
                lastSync = state.f0;
                recordsReadSinceLastSync = state.f1;
            }
        }

        if (lastSync != -1) {
            // open and read until the record we were before
            // the checkpoint and discard the values
            dataFileReader.seek(lastSync);
            for (int i = 0; i < recordsReadSinceLastSync; i++) {
                dataFileReader.next(null);
            }
        }
    }
}
