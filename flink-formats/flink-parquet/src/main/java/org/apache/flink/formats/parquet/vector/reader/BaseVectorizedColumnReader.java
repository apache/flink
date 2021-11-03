/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.formats.parquet.vector.reader;

import org.apache.flink.table.data.columnar.vector.writable.WritableColumnVector;
import org.apache.flink.table.types.logical.LogicalType;

import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.DataPageV2;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridDecoder;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import static org.apache.parquet.column.ValuesType.DEFINITION_LEVEL;
import static org.apache.parquet.column.ValuesType.REPETITION_LEVEL;
import static org.apache.parquet.column.ValuesType.VALUES;

/**
 * Abstract {@link ColumnReader}. part of the code is referred from Apache Hive and Apache Parquet.
 */
public abstract class BaseVectorizedColumnReader implements ColumnReader<WritableColumnVector> {

    private static final Logger LOG = LoggerFactory.getLogger(BaseVectorizedColumnReader.class);

    protected boolean isUtcTimestamp;

    /** Total number of values read. */
    protected long valuesRead;

    /**
     * value that indicates the end of the current page. That is, if valuesRead ==
     * endOfPageValueCount, we are at the end of the page.
     */
    protected long endOfPageValueCount;

    /** The dictionary, if this column has dictionary encoding. */
    protected final ParquetDataColumnReader dictionary;

    /** If true, the current page is dictionary encoded. */
    protected boolean isCurrentPageDictionaryEncoded;

    /** Maximum definition level for this column. */
    protected final int maxDefLevel;

    protected int definitionLevel;
    protected int repetitionLevel;

    /** Repetition/Definition/Value readers. */
    protected IntIterator repetitionLevelColumn;

    protected IntIterator definitionLevelColumn;
    protected ParquetDataColumnReader dataColumn;

    /** Total values in the current page. */
    protected int pageValueCount;

    protected final PageReader pageReader;
    protected final ColumnDescriptor descriptor;
    protected final Type type;
    protected final LogicalType logicalType;

    public BaseVectorizedColumnReader(
            ColumnDescriptor descriptor,
            PageReader pageReader,
            boolean isUtcTimestamp,
            Type parquetType,
            LogicalType logicalType)
            throws IOException {
        this.descriptor = descriptor;
        this.type = parquetType;
        this.pageReader = pageReader;
        this.maxDefLevel = descriptor.getMaxDefinitionLevel();
        this.isUtcTimestamp = isUtcTimestamp;
        this.logicalType = logicalType;

        DictionaryPage dictionaryPage = pageReader.readDictionaryPage();
        if (dictionaryPage != null) {
            try {
                this.dictionary =
                        ParquetDataColumnReaderFactory.getDataColumnReaderByTypeOnDictionary(
                                parquetType.asPrimitiveType(),
                                dictionaryPage
                                        .getEncoding()
                                        .initDictionary(descriptor, dictionaryPage),
                                isUtcTimestamp);
                this.isCurrentPageDictionaryEncoded = true;
            } catch (IOException e) {
                throw new IOException("could not decode the dictionary for " + descriptor, e);
            }
        } else {
            this.dictionary = null;
            this.isCurrentPageDictionaryEncoded = false;
        }
    }

    protected void readRepetitionAndDefinitionLevels() {
        repetitionLevel = repetitionLevelColumn.nextInt();
        definitionLevel = definitionLevelColumn.nextInt();
        valuesRead++;
    }

    protected void readPage() throws IOException {
        DataPage page = pageReader.readPage();

        if (page == null) {
            return;
        }

        page.accept(
                new DataPage.Visitor<Void>() {
                    @Override
                    public Void visit(DataPageV1 dataPageV1) {
                        readPageV1(dataPageV1);
                        return null;
                    }

                    @Override
                    public Void visit(DataPageV2 dataPageV2) {
                        readPageV2(dataPageV2);
                        return null;
                    }
                });
    }

    private void initDataReader(Encoding dataEncoding, ByteBufferInputStream in, int valueCount)
            throws IOException {
        this.pageValueCount = valueCount;
        this.endOfPageValueCount = valuesRead + pageValueCount;
        if (dataEncoding.usesDictionary()) {
            this.dataColumn = null;
            if (dictionary == null) {
                throw new IOException(
                        "could not read page in col "
                                + descriptor
                                + " as the dictionary was missing for encoding "
                                + dataEncoding);
            }
            dataColumn =
                    ParquetDataColumnReaderFactory.getDataColumnReaderByType(
                            type.asPrimitiveType(),
                            dataEncoding.getDictionaryBasedValuesReader(
                                    descriptor, VALUES, dictionary.getDictionary()),
                            isUtcTimestamp);
            this.isCurrentPageDictionaryEncoded = true;
        } else {
            dataColumn =
                    ParquetDataColumnReaderFactory.getDataColumnReaderByType(
                            type.asPrimitiveType(),
                            dataEncoding.getValuesReader(descriptor, VALUES),
                            isUtcTimestamp);
            this.isCurrentPageDictionaryEncoded = false;
        }

        try {
            dataColumn.initFromPage(pageValueCount, in);
        } catch (IOException e) {
            throw new IOException("could not read page in col " + descriptor, e);
        }
    }

    private void readPageV1(DataPageV1 page) {
        ValuesReader rlReader = page.getRlEncoding().getValuesReader(descriptor, REPETITION_LEVEL);
        ValuesReader dlReader = page.getDlEncoding().getValuesReader(descriptor, DEFINITION_LEVEL);
        this.repetitionLevelColumn = new ValuesReaderIntIterator(rlReader);
        this.definitionLevelColumn = new ValuesReaderIntIterator(dlReader);
        try {
            BytesInput bytes = page.getBytes();
            LOG.debug("page size " + bytes.size() + " bytes and " + pageValueCount + " records");
            ByteBufferInputStream in = bytes.toInputStream();
            LOG.debug("reading repetition levels at " + in.position());
            rlReader.initFromPage(pageValueCount, in);
            LOG.debug("reading definition levels at " + in.position());
            dlReader.initFromPage(pageValueCount, in);
            LOG.debug("reading data at " + in.position());
            initDataReader(page.getValueEncoding(), in, page.getValueCount());
        } catch (IOException e) {
            throw new ParquetDecodingException(
                    "could not read page " + page + " in col " + descriptor, e);
        }
    }

    private void readPageV2(DataPageV2 page) {
        this.pageValueCount = page.getValueCount();
        this.repetitionLevelColumn =
                newRLEIterator(descriptor.getMaxRepetitionLevel(), page.getRepetitionLevels());
        this.definitionLevelColumn =
                newRLEIterator(descriptor.getMaxDefinitionLevel(), page.getDefinitionLevels());
        try {
            LOG.debug(
                    "page data size "
                            + page.getData().size()
                            + " bytes and "
                            + pageValueCount
                            + " records");
            initDataReader(
                    page.getDataEncoding(), page.getData().toInputStream(), page.getValueCount());
        } catch (IOException e) {
            throw new ParquetDecodingException(
                    "could not read page " + page + " in col " + descriptor, e);
        }
    }

    private IntIterator newRLEIterator(int maxLevel, BytesInput bytes) {
        try {
            if (maxLevel == 0) {
                return new NullIntIterator();
            }
            return new RLEIntIterator(
                    new RunLengthBitPackingHybridDecoder(
                            BytesUtils.getWidthFromMaxInt(maxLevel),
                            new ByteArrayInputStream(bytes.toByteArray())));
        } catch (IOException e) {
            throw new ParquetDecodingException(
                    "could not read levels in page for col " + descriptor, e);
        }
    }

    /** Utility classes to abstract over different way to read ints with different encodings. */
    abstract static class IntIterator {
        abstract int nextInt();
    }

    /** read ints from {@link ValuesReader}. */
    protected static final class ValuesReaderIntIterator extends IntIterator {
        ValuesReader delegate;

        public ValuesReaderIntIterator(ValuesReader delegate) {
            this.delegate = delegate;
        }

        @Override
        int nextInt() {
            return delegate.readInteger();
        }
    }

    /** read ints from {@link RunLengthBitPackingHybridDecoder}. */
    protected static final class RLEIntIterator extends IntIterator {
        RunLengthBitPackingHybridDecoder delegate;

        public RLEIntIterator(RunLengthBitPackingHybridDecoder delegate) {
            this.delegate = delegate;
        }

        @Override
        int nextInt() {
            try {
                return delegate.readInt();
            } catch (IOException e) {
                throw new ParquetDecodingException(e);
            }
        }
    }

    /** return zero. */
    protected static final class NullIntIterator extends IntIterator {
        @Override
        int nextInt() {
            return 0;
        }
    }
}
