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

package org.apache.flink.formats.parquet.avro;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.reader.StreamFormat;
import org.apache.flink.connector.file.src.util.CheckpointedPosition;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.formats.parquet.ParquetInputFile;
import org.apache.flink.util.function.SerializableSupplier;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.io.InputFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * A reader format that reads individual Avro records from a Parquet stream. Please refer to the
 * factory class {@link AvroParquetReaders} for how to create new {@link AvroParquetRecordFormat}.
 * This class leverages {@link ParquetReader} underneath. The parquet files need to be in a certain
 * way compatible with the provided Avro schema and Avro data models, i.e. {@link GenericData},
 * {@link org.apache.avro.specific.SpecificData}, or {@link org.apache.avro.reflect.ReflectData}.
 */
class AvroParquetRecordFormat<E> implements StreamFormat<E> {

    private static final long serialVersionUID = 1L;

    static final Logger LOG = LoggerFactory.getLogger(AvroParquetRecordFormat.class);

    private final TypeInformation<E> type;

    private final SerializableSupplier<GenericData> dataModelSupplier;

    AvroParquetRecordFormat(
            TypeInformation<E> type, SerializableSupplier<GenericData> dataModelSupplier) {
        this.type = type;
        this.dataModelSupplier = dataModelSupplier;
    }

    /**
     * Creates a new reader to read avro {@link GenericRecord} from Parquet input stream.
     *
     * <p>Several wrapper classes haven be created to Flink abstraction become compatible with the
     * parquet abstraction. Please refer to the inner classes {@link AvroParquetRecordReader},
     * {@link ParquetInputFile}, {@code FSDataInputStreamAdapter} for details.
     */
    @Override
    public Reader<E> createReader(
            Configuration config, FSDataInputStream stream, long fileLen, long splitEnd)
            throws IOException {

        // current version does not support splitting.
        checkNotSplit(fileLen, splitEnd);

        return new AvroParquetRecordReader<E>(
                AvroParquetReader.<E>builder(new ParquetInputFile(stream, fileLen))
                        .withDataModel(getDataModel())
                        .build());
    }

    /**
     * Restores the reader from a checkpointed position. It is in fact identical since only {@link
     * CheckpointedPosition#NO_OFFSET} as the {@code restoredOffset} is support.
     */
    @Override
    public Reader<E> restoreReader(
            Configuration config,
            FSDataInputStream stream,
            long restoredOffset,
            long fileLen,
            long splitEnd)
            throws IOException {

        // current version does not support splitting.
        checkNotSplit(fileLen, splitEnd);

        checkArgument(
                restoredOffset == CheckpointedPosition.NO_OFFSET,
                "The restoredOffset should always be NO_OFFSET");

        return createReader(config, stream, fileLen, splitEnd);
    }

    @VisibleForTesting
    GenericData getDataModel() {
        return dataModelSupplier.get();
    }

    /** Current version does not support splitting. */
    @Override
    public boolean isSplittable() {
        return false;
    }

    /**
     * Gets the type produced by this format. This type will be the type produced by the file source
     * as a whole.
     */
    @Override
    public TypeInformation<E> getProducedType() {
        return type;
    }

    private static void checkNotSplit(long fileLen, long splitEnd) {
        if (splitEnd != fileLen) {
            throw new IllegalArgumentException(
                    String.format(
                            "Current version of AvroParquetRecordFormat is not splittable, "
                                    + "but found split end (%d) different from file length (%d)",
                            splitEnd, fileLen));
        }
    }

    /**
     * {@link StreamFormat.Reader} implementation. Using {@link ParquetReader} internally to read
     * avro {@link GenericRecord} from parquet {@link InputFile}.
     */
    private static class AvroParquetRecordReader<E> implements StreamFormat.Reader<E> {

        private final ParquetReader<E> parquetReader;

        private long skipCount;
        private final boolean checkpointed;

        private AvroParquetRecordReader(ParquetReader<E> parquetReader) {
            this(parquetReader, 0, false);
        }

        private AvroParquetRecordReader(
                ParquetReader<E> parquetReader, long skipCount, boolean checkpointed) {
            this.parquetReader = parquetReader;
            this.skipCount = skipCount;
            this.checkpointed = checkpointed;
        }

        @Nullable
        @Override
        public E read() throws IOException {
            E record = parquetReader.read();
            incrementPosition();
            return record;
        }

        @Override
        public void close() throws IOException {
            parquetReader.close();
        }

        @Nullable
        @Override
        public CheckpointedPosition getCheckpointedPosition() {
            // Since ParquetReader does not expose the offset, always use
            // CheckpointedPosition.NO_OFFSET.
            return checkpointed
                    ? new CheckpointedPosition(CheckpointedPosition.NO_OFFSET, skipCount)
                    : null;
        }

        private void incrementPosition() {
            skipCount++;
        }
    }
}
