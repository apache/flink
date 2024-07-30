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

package org.apache.flink.formats.parquet.vector.reader;

import org.apache.flink.formats.parquet.vector.ParquetDecimalVector;
import org.apache.flink.formats.parquet.vector.position.LevelDelegation;
import org.apache.flink.runtime.util.IntArrayList;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.columnar.vector.heap.HeapBooleanVector;
import org.apache.flink.table.data.columnar.vector.heap.HeapByteVector;
import org.apache.flink.table.data.columnar.vector.heap.HeapBytesVector;
import org.apache.flink.table.data.columnar.vector.heap.HeapDoubleVector;
import org.apache.flink.table.data.columnar.vector.heap.HeapFloatVector;
import org.apache.flink.table.data.columnar.vector.heap.HeapIntVector;
import org.apache.flink.table.data.columnar.vector.heap.HeapLongVector;
import org.apache.flink.table.data.columnar.vector.heap.HeapShortVector;
import org.apache.flink.table.data.columnar.vector.heap.HeapTimestampVector;
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
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.parquet.column.ValuesType.DEFINITION_LEVEL;
import static org.apache.parquet.column.ValuesType.REPETITION_LEVEL;
import static org.apache.parquet.column.ValuesType.VALUES;

/** Reader to read nested primitive column. */
public class NestedPrimitiveColumnReader implements ColumnReader<WritableColumnVector> {
    private static final Logger LOG = LoggerFactory.getLogger(NestedPrimitiveColumnReader.class);

    private final IntArrayList repetitionLevelList = new IntArrayList(0);
    private final IntArrayList definitionLevelList = new IntArrayList(0);

    private final PageReader pageReader;
    private final ColumnDescriptor descriptor;
    private final Type type;
    private final LogicalType logicalType;
    /** The dictionary, if this column has dictionary encoding. */
    private final ParquetDataColumnReader dictionary;
    /** Maximum definition level for this column. */
    private final int maxDefLevel;

    private boolean isUtcTimestamp;

    /** Total number of values read. */
    private long valuesRead;

    /**
     * value that indicates the end of the current page. That is, if valuesRead ==
     * endOfPageValueCount, we are at the end of the page.
     */
    private long endOfPageValueCount;

    /** If true, the current page is dictionary encoded. */
    private boolean isCurrentPageDictionaryEncoded;

    private int definitionLevel;
    private int repetitionLevel;

    /** Repetition/Definition/Value readers. */
    private IntIterator repetitionLevelColumn;

    private IntIterator definitionLevelColumn;
    private ParquetDataColumnReader dataColumn;

    /** Total values in the current page. */
    private int pageValueCount;

    // flag to indicate if there is no data in parquet data page
    private boolean eof = false;

    private boolean isFirstRow = true;

    private Object lastValue;

    public NestedPrimitiveColumnReader(
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
                throw new IOException(
                        String.format("Could not decode the dictionary for %s", descriptor), e);
            }
        } else {
            this.dictionary = null;
            this.isCurrentPageDictionaryEncoded = false;
        }
    }

    // This won't call, will actually call readAndNewVector
    @Override
    public void readToVector(int readNumber, WritableColumnVector vector) throws IOException {
        throw new UnsupportedOperationException("This function should no be called.");
    }

    public WritableColumnVector readAndNewVector(int readNumber, WritableColumnVector vector)
            throws IOException {
        if (isFirstRow) {
            if (!readValue()) {
                return vector;
            }
            isFirstRow = false;
        }

        // index to set value.
        int index = 0;
        int valueIndex = 0;
        List<Object> valueList = new ArrayList<>();

        // repeated type need two loops to read data.
        while (!eof && index < readNumber) {
            do {
                valueList.add(lastValue);
                valueIndex++;
            } while (readValue() && (repetitionLevel != 0));
            index++;
        }

        return fillColumnVector(valueIndex, valueList);
    }

    public LevelDelegation getLevelDelegation() {
        int[] repetition = repetitionLevelList.toArray();
        int[] definition = definitionLevelList.toArray();
        repetitionLevelList.clear();
        definitionLevelList.clear();
        repetitionLevelList.add(repetitionLevel);
        definitionLevelList.add(definitionLevel);
        return new LevelDelegation(repetition, definition);
    }

    private boolean readValue() throws IOException {
        int left = readPageIfNeed();
        if (left > 0) {
            // get the values of repetition and definitionLevel
            readAndSaveRepetitionAndDefinitionLevels();
            // read the data if it isn't null
            if (definitionLevel == maxDefLevel) {
                if (isCurrentPageDictionaryEncoded) {
                    int dictionaryId = dataColumn.readValueDictionaryId();
                    lastValue = dictionaryDecodeValue(logicalType, dictionaryId);
                } else {
                    lastValue = readPrimitiveTypedRow(logicalType);
                }
            } else {
                lastValue = null;
            }
            return true;
        } else {
            eof = true;
            return false;
        }
    }

    private void readAndSaveRepetitionAndDefinitionLevels() {
        // get the values of repetition and definitionLevel
        repetitionLevel = repetitionLevelColumn.nextInt();
        definitionLevel = definitionLevelColumn.nextInt();
        valuesRead++;
        repetitionLevelList.add(repetitionLevel);
        definitionLevelList.add(definitionLevel);
    }

    private int readPageIfNeed() throws IOException {
        // Compute the number of values we want to read in this page.
        int leftInPage = (int) (endOfPageValueCount - valuesRead);
        if (leftInPage == 0) {
            // no data left in current page, load data from new page
            readPage();
            leftInPage = (int) (endOfPageValueCount - valuesRead);
        }
        return leftInPage;
    }

    private Object readPrimitiveTypedRow(LogicalType category) {
        switch (category.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
            case BINARY:
            case VARBINARY:
                return dataColumn.readBytes();
            case BOOLEAN:
                return dataColumn.readBoolean();
            case TIME_WITHOUT_TIME_ZONE:
            case DATE:
            case INTEGER:
                return dataColumn.readInteger();
            case TINYINT:
                return dataColumn.readTinyInt();
            case SMALLINT:
                return dataColumn.readSmallInt();
            case BIGINT:
                return dataColumn.readLong();
            case FLOAT:
                return dataColumn.readFloat();
            case DOUBLE:
                return dataColumn.readDouble();
            case DECIMAL:
                switch (descriptor.getPrimitiveType().getPrimitiveTypeName()) {
                    case INT32:
                        return dataColumn.readInteger();
                    case INT64:
                        return dataColumn.readLong();
                    case BINARY:
                    case FIXED_LEN_BYTE_ARRAY:
                        return dataColumn.readBytes();
                }
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return dataColumn.readTimestamp();
            default:
                throw new RuntimeException("Unsupported type in the list: " + type);
        }
    }

    private Object dictionaryDecodeValue(LogicalType category, Integer dictionaryValue) {
        if (dictionaryValue == null) {
            return null;
        }

        switch (category.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
            case BINARY:
            case VARBINARY:
                return dictionary.readBytes(dictionaryValue);
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
            case INTEGER:
                return dictionary.readInteger(dictionaryValue);
            case BOOLEAN:
                return dictionary.readBoolean(dictionaryValue) ? 1 : 0;
            case DOUBLE:
                return dictionary.readDouble(dictionaryValue);
            case FLOAT:
                return dictionary.readFloat(dictionaryValue);
            case TINYINT:
                return dictionary.readTinyInt(dictionaryValue);
            case SMALLINT:
                return dictionary.readSmallInt(dictionaryValue);
            case BIGINT:
                return dictionary.readLong(dictionaryValue);
            case DECIMAL:
                switch (descriptor.getPrimitiveType().getPrimitiveTypeName()) {
                    case INT32:
                        return dictionary.readInteger(dictionaryValue);
                    case INT64:
                        return dictionary.readLong(dictionaryValue);
                    case FIXED_LEN_BYTE_ARRAY:
                    case BINARY:
                        return dictionary.readBytes(dictionaryValue);
                }
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return dictionary.readTimestamp(dictionaryValue);
            default:
                throw new RuntimeException("Unsupported type in the list: " + type);
        }
    }

    private WritableColumnVector fillColumnVector(int total, List valueList) {
        switch (logicalType.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
            case BINARY:
            case VARBINARY:
                HeapBytesVector heapBytesVector = new HeapBytesVector(total);
                for (int i = 0; i < valueList.size(); i++) {
                    byte[] src = ((List<byte[]>) valueList).get(i);
                    if (src == null) {
                        heapBytesVector.setNullAt(i);
                    } else {
                        heapBytesVector.appendBytes(i, src, 0, src.length);
                    }
                }
                return heapBytesVector;
            case BOOLEAN:
                HeapBooleanVector heapBooleanVector = new HeapBooleanVector(total);
                for (int i = 0; i < valueList.size(); i++) {
                    if (valueList.get(i) == null) {
                        heapBooleanVector.setNullAt(i);
                    } else {
                        heapBooleanVector.vector[i] = ((List<Boolean>) valueList).get(i);
                    }
                }
                return heapBooleanVector;
            case TINYINT:
                HeapByteVector heapByteVector = new HeapByteVector(total);
                for (int i = 0; i < valueList.size(); i++) {
                    if (valueList.get(i) == null) {
                        heapByteVector.setNullAt(i);
                    } else {
                        heapByteVector.vector[i] =
                                (byte) ((List<Integer>) valueList).get(i).intValue();
                    }
                }
                return heapByteVector;
            case SMALLINT:
                HeapShortVector heapShortVector = new HeapShortVector(total);
                for (int i = 0; i < valueList.size(); i++) {
                    if (valueList.get(i) == null) {
                        heapShortVector.setNullAt(i);
                    } else {
                        heapShortVector.vector[i] =
                                (short) ((List<Integer>) valueList).get(i).intValue();
                    }
                }
                return heapShortVector;
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                HeapIntVector heapIntVector = new HeapIntVector(total);
                for (int i = 0; i < valueList.size(); i++) {
                    if (valueList.get(i) == null) {
                        heapIntVector.setNullAt(i);
                    } else {
                        heapIntVector.vector[i] = ((List<Integer>) valueList).get(i);
                    }
                }
                return heapIntVector;
            case FLOAT:
                HeapFloatVector heapFloatVector = new HeapFloatVector(total);
                for (int i = 0; i < valueList.size(); i++) {
                    if (valueList.get(i) == null) {
                        heapFloatVector.setNullAt(i);
                    } else {
                        heapFloatVector.vector[i] = ((List<Float>) valueList).get(i);
                    }
                }
                return heapFloatVector;
            case BIGINT:
                HeapLongVector heapLongVector = new HeapLongVector(total);
                for (int i = 0; i < valueList.size(); i++) {
                    if (valueList.get(i) == null) {
                        heapLongVector.setNullAt(i);
                    } else {
                        heapLongVector.vector[i] = ((List<Long>) valueList).get(i);
                    }
                }
                return heapLongVector;
            case DOUBLE:
                HeapDoubleVector heapDoubleVector = new HeapDoubleVector(total);
                for (int i = 0; i < valueList.size(); i++) {
                    if (valueList.get(i) == null) {
                        heapDoubleVector.setNullAt(i);
                    } else {
                        heapDoubleVector.vector[i] = ((List<Double>) valueList).get(i);
                    }
                }
                return heapDoubleVector;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                HeapTimestampVector heapTimestampVector = new HeapTimestampVector(total);
                for (int i = 0; i < valueList.size(); i++) {
                    if (valueList.get(i) == null) {
                        heapTimestampVector.setNullAt(i);
                    } else {
                        heapTimestampVector.setTimestamp(
                                i, ((List<TimestampData>) valueList).get(i));
                    }
                }
                return heapTimestampVector;
            case DECIMAL:
                PrimitiveType.PrimitiveTypeName primitiveTypeName =
                        descriptor.getPrimitiveType().getPrimitiveTypeName();
                switch (primitiveTypeName) {
                    case INT32:
                        HeapIntVector phiv = new HeapIntVector(total);
                        for (int i = 0; i < valueList.size(); i++) {
                            if (valueList.get(i) == null) {
                                phiv.setNullAt(i);
                            } else {
                                phiv.vector[i] = ((List<Integer>) valueList).get(i);
                            }
                        }
                        return new ParquetDecimalVector(phiv);
                    case INT64:
                        HeapLongVector phlv = new HeapLongVector(total);
                        for (int i = 0; i < valueList.size(); i++) {
                            if (valueList.get(i) == null) {
                                phlv.setNullAt(i);
                            } else {
                                phlv.vector[i] = ((List<Long>) valueList).get(i);
                            }
                        }
                        return new ParquetDecimalVector(phlv);
                    default:
                        HeapBytesVector phbv = getHeapBytesVector(total, valueList);
                        return new ParquetDecimalVector(phbv);
                }
            default:
                throw new RuntimeException("Unsupported type in the list: " + type);
        }
    }

    private static HeapBytesVector getHeapBytesVector(int total, List valueList) {
        HeapBytesVector phbv = new HeapBytesVector(total);
        for (int i = 0; i < valueList.size(); i++) {
            byte[] src = ((List<byte[]>) valueList).get(i);
            if (valueList.get(i) == null) {
                phbv.setNullAt(i);
            } else {
                phbv.appendBytes(i, src, 0, src.length);
            }
        }
        return phbv;
    }

    protected void readPage() {
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
                        String.format(
                                "Could not read page in col %s because the dictionary was missing for encoding %s.",
                                descriptor, dataEncoding));
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
            throw new IOException(String.format("Could not read page in col %s.", descriptor), e);
        }
    }

    private void readPageV1(DataPageV1 page) {
        ValuesReader rlReader = page.getRlEncoding().getValuesReader(descriptor, REPETITION_LEVEL);
        ValuesReader dlReader = page.getDlEncoding().getValuesReader(descriptor, DEFINITION_LEVEL);
        this.repetitionLevelColumn = new ValuesReaderIntIterator(rlReader);
        this.definitionLevelColumn = new ValuesReaderIntIterator(dlReader);
        try {
            BytesInput bytes = page.getBytes();
            LOG.debug("Page size {}  bytes and {} records.", bytes.size(), pageValueCount);
            ByteBufferInputStream in = bytes.toInputStream();
            LOG.debug("Reading repetition levels at {}.", in.position());
            rlReader.initFromPage(pageValueCount, in);
            LOG.debug("Reading definition levels at {}.", in.position());
            dlReader.initFromPage(pageValueCount, in);
            LOG.debug("Reading data at {}.", in.position());
            initDataReader(page.getValueEncoding(), in, page.getValueCount());
        } catch (IOException e) {
            throw new ParquetDecodingException(
                    String.format("Could not read page %s in col %s.", page, descriptor), e);
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
                    "Page data size {} bytes and {} records.",
                    page.getData().size(),
                    pageValueCount);
            initDataReader(
                    page.getDataEncoding(), page.getData().toInputStream(), page.getValueCount());
        } catch (IOException e) {
            throw new ParquetDecodingException(
                    String.format("Could not read page %s in col %s.", page, descriptor), e);
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
                    String.format("Could not read levels in page for col %s.", descriptor), e);
        }
    }

    /** Utility interface to abstract over different way to read ints with different encodings. */
    interface IntIterator {
        int nextInt();
    }

    /** Reading int from {@link ValuesReader}. */
    protected static final class ValuesReaderIntIterator implements IntIterator {
        ValuesReader delegate;

        public ValuesReaderIntIterator(ValuesReader delegate) {
            this.delegate = delegate;
        }

        @Override
        public int nextInt() {
            return delegate.readInteger();
        }
    }

    /** Reading int from {@link RunLengthBitPackingHybridDecoder}. */
    protected static final class RLEIntIterator implements IntIterator {
        RunLengthBitPackingHybridDecoder delegate;

        public RLEIntIterator(RunLengthBitPackingHybridDecoder delegate) {
            this.delegate = delegate;
        }

        @Override
        public int nextInt() {
            try {
                return delegate.readInt();
            } catch (IOException e) {
                throw new ParquetDecodingException(e);
            }
        }
    }

    /** Reading zero always. */
    protected static final class NullIntIterator implements IntIterator {
        @Override
        public int nextInt() {
            return 0;
        }
    }
}
