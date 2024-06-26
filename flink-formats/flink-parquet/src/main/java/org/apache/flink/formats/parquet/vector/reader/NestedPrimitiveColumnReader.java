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

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import shaded.parquet.it.unimi.dsi.fastutil.ints.IntArrayList;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** Reader to read nested primitive column. */
public class NestedPrimitiveColumnReader extends BaseVectorizedColumnReader {

    // flag to indicate if there is no data in parquet data page
    private boolean eof = false;

    private boolean isFirstRow = true;

    private Object lastValue;

    private final IntArrayList repetetionLevelList = new IntArrayList();
    private final IntArrayList definitionLevelList = new IntArrayList();

    public NestedPrimitiveColumnReader(
            ColumnDescriptor descriptor,
            PageReader pageReader,
            boolean isUtcTimestamp,
            Type parquetType,
            LogicalType logicalType)
            throws IOException {
        super(descriptor, pageReader, isUtcTimestamp, parquetType, logicalType);
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
        int[] repetition = repetetionLevelList.toIntArray();
        int[] definition = definitionLevelList.toIntArray();
        repetetionLevelList.clear();
        definitionLevelList.clear();
        repetetionLevelList.add(repetitionLevel);
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
        readRepetitionAndDefinitionLevels();
        repetetionLevelList.add(repetitionLevel);
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
}
