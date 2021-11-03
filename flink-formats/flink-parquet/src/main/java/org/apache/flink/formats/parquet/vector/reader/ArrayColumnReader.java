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

import org.apache.flink.formats.parquet.vector.ParquetDecimalVector;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.columnar.vector.VectorizedColumnBatch;
import org.apache.flink.table.data.columnar.vector.heap.HeapArrayVector;
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
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/** Array {@link ColumnReader}. */
public class ArrayColumnReader extends BaseVectorizedColumnReader {

    // The value read in last time
    private Object lastValue;

    // flag to indicate if there is no data in parquet data page
    private boolean eof = false;

    // flag to indicate if it's the first time to read parquet data page with this instance
    boolean isFirstRow = true;

    public ArrayColumnReader(
            ColumnDescriptor descriptor,
            PageReader pageReader,
            boolean isUtcTimestamp,
            Type type,
            LogicalType logicalType)
            throws IOException {
        super(descriptor, pageReader, isUtcTimestamp, type, logicalType);
    }

    @Override
    public void readToVector(int readNumber, WritableColumnVector vector) throws IOException {
        HeapArrayVector lcv = (HeapArrayVector) vector;
        // before readBatch, initial the size of offsets & lengths as the default value,
        // the actual size will be assigned in setChildrenInfo() after reading complete.
        lcv.offsets = new long[VectorizedColumnBatch.DEFAULT_SIZE];
        lcv.lengths = new long[VectorizedColumnBatch.DEFAULT_SIZE];
        // Because the length of ListColumnVector.child can't be known now,
        // the valueList will save all data for ListColumnVector temporary.
        List<Object> valueList = new ArrayList<>();

        LogicalType category = ((ArrayType) logicalType).getElementType();

        // read the first row in parquet data page, this will be only happened once for this
        // instance
        if (isFirstRow) {
            if (!fetchNextValue(category)) {
                return;
            }
            isFirstRow = false;
        }

        int index = collectDataFromParquetPage(readNumber, lcv, valueList, category);

        // Convert valueList to array for the ListColumnVector.child
        fillColumnVector(category, lcv, valueList, index);
    }

    /**
     * Reads a single value from parquet page, puts it into lastValue. Returns a boolean indicating
     * if there is more values to read (true).
     *
     * @param category
     * @return boolean
     * @throws IOException
     */
    private boolean fetchNextValue(LogicalType category) throws IOException {
        int left = readPageIfNeed();
        if (left > 0) {
            // get the values of repetition and definitionLevel
            readRepetitionAndDefinitionLevels();
            // read the data if it isn't null
            if (definitionLevel == maxDefLevel) {
                if (isCurrentPageDictionaryEncoded) {
                    lastValue = dataColumn.readValueDictionaryId();
                } else {
                    lastValue = readPrimitiveTypedRow(category);
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

    // Need to be in consistent with that VectorizedPrimitiveColumnReader#readBatchHelper
    // TODO Reduce the duplicated code
    private Object readPrimitiveTypedRow(LogicalType category) {
        switch (category.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
            case BINARY:
            case VARBINARY:
                return dataColumn.readString();
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
                        return dataColumn.readString();
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
                return dictionary.readString(dictionaryValue);
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
                        return dictionary.readString(dictionaryValue);
                }
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return dictionary.readTimestamp(dictionaryValue);
            default:
                throw new RuntimeException("Unsupported type in the list: " + type);
        }
    }

    /**
     * Collects data from a parquet page and returns the final row index where it stopped. The
     * returned index can be equal to or less than total.
     *
     * @param total maximum number of rows to collect
     * @param lcv column vector to do initial setup in data collection time
     * @param valueList collection of values that will be fed into the vector later
     * @param category
     * @return int
     * @throws IOException
     */
    private int collectDataFromParquetPage(
            int total, HeapArrayVector lcv, List<Object> valueList, LogicalType category)
            throws IOException {
        int index = 0;
        /*
         * Here is a nested loop for collecting all values from a parquet page.
         * A column of array type can be considered as a list of lists, so the two loops are as below:
         * 1. The outer loop iterates on rows (index is a row index, so points to a row in the batch), e.g.:
         * [0, 2, 3]    <- index: 0
         * [NULL, 3, 4] <- index: 1
         *
         * 2. The inner loop iterates on values within a row (sets all data from parquet data page
         * for an element in ListColumnVector), so fetchNextValue returns values one-by-one:
         * 0, 2, 3, NULL, 3, 4
         *
         * As described below, the repetition level (repetitionLevel != 0)
         * can be used to decide when we'll start to read values for the next list.
         */
        while (!eof && index < total) {
            // add element to ListColumnVector one by one
            lcv.offsets[index] = valueList.size();
            /*
             * Let's collect all values for a single list.
             * Repetition level = 0 means that a new list started there in the parquet page,
             * in that case, let's exit from the loop, and start to collect value for a new list.
             */
            do {
                /*
                 * Definition level = 0 when a NULL value was returned instead of a list
                 * (this is not the same as a NULL value in of a list).
                 */
                if (definitionLevel == 0) {
                    lcv.setNullAt(index);
                }
                valueList.add(
                        isCurrentPageDictionaryEncoded
                                ? dictionaryDecodeValue(category, (Integer) lastValue)
                                : lastValue);
            } while (fetchNextValue(category) && (repetitionLevel != 0));

            lcv.lengths[index] = valueList.size() - lcv.offsets[index];
            index++;
        }
        return index;
    }

    /**
     * The lengths & offsets will be initialized as default size (1024), it should be set to the
     * actual size according to the element number.
     */
    private void setChildrenInfo(HeapArrayVector lcv, int itemNum, int elementNum) {
        lcv.childCount = itemNum;
        long[] lcvLength = new long[elementNum];
        long[] lcvOffset = new long[elementNum];
        System.arraycopy(lcv.lengths, 0, lcvLength, 0, elementNum);
        System.arraycopy(lcv.offsets, 0, lcvOffset, 0, elementNum);
        lcv.lengths = lcvLength;
        lcv.offsets = lcvOffset;
    }

    private void fillColumnVector(
            LogicalType category, HeapArrayVector lcv, List valueList, int elementNum) {
        int total = valueList.size();
        setChildrenInfo(lcv, total, elementNum);
        switch (category.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
            case BINARY:
            case VARBINARY:
                lcv.child = new HeapBytesVector(total);
                ((HeapBytesVector) lcv.child).reset();
                for (int i = 0; i < valueList.size(); i++) {
                    byte[] src = ((List<byte[]>) valueList).get(i);
                    if (src == null) {
                        ((HeapBytesVector) lcv.child).setNullAt(i);
                    } else {
                        ((HeapBytesVector) lcv.child).appendBytes(i, src, 0, src.length);
                    }
                }
                break;
            case BOOLEAN:
                lcv.child = new HeapBooleanVector(total);
                ((HeapBooleanVector) lcv.child).reset();
                for (int i = 0; i < valueList.size(); i++) {
                    if (valueList.get(i) == null) {
                        ((HeapBooleanVector) lcv.child).setNullAt(i);
                    } else {
                        ((HeapBooleanVector) lcv.child).vector[i] =
                                ((List<Boolean>) valueList).get(i);
                    }
                }
                break;
            case TINYINT:
                lcv.child = new HeapByteVector(total);
                ((HeapByteVector) lcv.child).reset();
                for (int i = 0; i < valueList.size(); i++) {
                    if (valueList.get(i) == null) {
                        ((HeapByteVector) lcv.child).setNullAt(i);
                    } else {
                        ((HeapByteVector) lcv.child).vector[i] =
                                (byte) ((List<Integer>) valueList).get(i).intValue();
                    }
                }
                break;
            case SMALLINT:
                lcv.child = new HeapShortVector(total);
                ((HeapShortVector) lcv.child).reset();
                for (int i = 0; i < valueList.size(); i++) {
                    if (valueList.get(i) == null) {
                        ((HeapShortVector) lcv.child).setNullAt(i);
                    } else {
                        ((HeapShortVector) lcv.child).vector[i] =
                                (short) ((List<Integer>) valueList).get(i).intValue();
                    }
                }
                break;
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
                lcv.child = new HeapIntVector(total);
                ((HeapIntVector) lcv.child).reset();
                for (int i = 0; i < valueList.size(); i++) {
                    if (valueList.get(i) == null) {
                        ((HeapIntVector) lcv.child).setNullAt(i);
                    } else {
                        ((HeapIntVector) lcv.child).vector[i] = ((List<Integer>) valueList).get(i);
                    }
                }
                break;
            case FLOAT:
                lcv.child = new HeapFloatVector(total);
                ((HeapFloatVector) lcv.child).reset();
                for (int i = 0; i < valueList.size(); i++) {
                    if (valueList.get(i) == null) {
                        ((HeapFloatVector) lcv.child).setNullAt(i);
                    } else {
                        ((HeapFloatVector) lcv.child).vector[i] = ((List<Float>) valueList).get(i);
                    }
                }
                break;
            case BIGINT:
                lcv.child = new HeapLongVector(total);
                ((HeapLongVector) lcv.child).reset();
                for (int i = 0; i < valueList.size(); i++) {
                    if (valueList.get(i) == null) {
                        ((HeapLongVector) lcv.child).setNullAt(i);
                    } else {
                        ((HeapLongVector) lcv.child).vector[i] = ((List<Long>) valueList).get(i);
                    }
                }
                break;
            case DOUBLE:
                lcv.child = new HeapDoubleVector(total);
                ((HeapDoubleVector) lcv.child).reset();
                for (int i = 0; i < valueList.size(); i++) {
                    if (valueList.get(i) == null) {
                        ((HeapDoubleVector) lcv.child).setNullAt(i);
                    } else {
                        ((HeapDoubleVector) lcv.child).vector[i] =
                                ((List<Double>) valueList).get(i);
                    }
                }
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                lcv.child = new HeapTimestampVector(total);
                ((HeapTimestampVector) lcv.child).reset();
                for (int i = 0; i < valueList.size(); i++) {
                    if (valueList.get(i) == null) {
                        ((HeapTimestampVector) lcv.child).setNullAt(i);
                    } else {
                        ((HeapTimestampVector) lcv.child)
                                .setTimestamp(i, ((List<TimestampData>) valueList).get(i));
                    }
                }
                break;
            case DECIMAL:
                PrimitiveType.PrimitiveTypeName primitiveTypeName =
                        descriptor.getPrimitiveType().getPrimitiveTypeName();
                switch (primitiveTypeName) {
                    case INT32:
                        lcv.child = new ParquetDecimalVector(new HeapIntVector(total));
                        ((HeapIntVector) ((ParquetDecimalVector) lcv.child).vector).reset();
                        for (int i = 0; i < valueList.size(); i++) {
                            if (valueList.get(i) == null) {
                                ((HeapIntVector) ((ParquetDecimalVector) lcv.child).vector)
                                        .setNullAt(i);
                            } else {
                                ((HeapIntVector) ((ParquetDecimalVector) lcv.child).vector)
                                                .vector[i] =
                                        ((List<Integer>) valueList).get(i);
                            }
                        }
                        break;
                    case INT64:
                        lcv.child = new ParquetDecimalVector(new HeapLongVector(total));
                        ((HeapLongVector) ((ParquetDecimalVector) lcv.child).vector).reset();
                        for (int i = 0; i < valueList.size(); i++) {
                            if (valueList.get(i) == null) {
                                ((HeapLongVector) ((ParquetDecimalVector) lcv.child).vector)
                                        .setNullAt(i);
                            } else {
                                ((HeapLongVector) ((ParquetDecimalVector) lcv.child).vector)
                                                .vector[i] =
                                        ((List<Long>) valueList).get(i);
                            }
                        }
                        break;
                    default:
                        lcv.child = new ParquetDecimalVector(new HeapBytesVector(total));
                        ((HeapBytesVector) ((ParquetDecimalVector) lcv.child).vector).reset();
                        for (int i = 0; i < valueList.size(); i++) {
                            byte[] src = ((List<byte[]>) valueList).get(i);
                            if (valueList.get(i) == null) {
                                ((HeapBytesVector) ((ParquetDecimalVector) lcv.child).vector)
                                        .setNullAt(i);
                            } else {
                                ((HeapBytesVector) ((ParquetDecimalVector) lcv.child).vector)
                                        .appendBytes(i, src, 0, src.length);
                            }
                        }
                        break;
                }
                break;
            default:
                throw new RuntimeException("Unsupported type in the list: " + type);
        }
    }
}
