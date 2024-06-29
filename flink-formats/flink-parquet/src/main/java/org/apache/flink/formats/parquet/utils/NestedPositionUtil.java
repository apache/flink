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

package org.apache.flink.formats.parquet.utils;

import org.apache.flink.formats.parquet.vector.position.CollectionPosition;
import org.apache.flink.formats.parquet.vector.position.RowPosition;
import org.apache.flink.formats.parquet.vector.type.ParquetField;
import org.apache.flink.runtime.util.BooleanArrayList;
import org.apache.flink.runtime.util.LongArrayList;

import static java.lang.String.format;

/** Utils to calculate nested type position. */
public class NestedPositionUtil {

    /**
     * Calculate row offsets according to column's max repetition level, definition level, value's
     * repetition level and definition level. Each row has three situation:
     * <li>Row is not defined,because it's optional parent fields is null, this is decided by its
     *     parent's repetition level
     * <li>Row is null
     * <li>Row is defined and not empty.
     *
     * @param field field that contains the row column message include max repetition level and
     *     definition level.
     * @param fieldRepetitionLevels int array with each value's repetition level.
     * @param fieldDefinitionLevels int array with each value's definition level.
     * @return {@link RowPosition} contains collections row count and isNull array.
     */
    public static RowPosition calculateRowOffsets(
            ParquetField field, int[] fieldDefinitionLevels, int[] fieldRepetitionLevels) {
        int rowDefinitionLevel = field.getDefinitionLevel();
        int rowRepetitionLevel = field.getRepetitionLevel();
        int nullValuesCount = 0;
        BooleanArrayList nullRowFlags = new BooleanArrayList(0);
        for (int i = 0; i < fieldDefinitionLevels.length; i++) {
            if (fieldRepetitionLevels[i] > rowRepetitionLevel) {
                throw new IllegalStateException(
                        format(
                                "In parquet's row type field repetition level should not larger than row's repetition level. "
                                        + "Row repetition level is %s, row field repetition level is %s.",
                                rowRepetitionLevel, fieldRepetitionLevels[i]));
            }

            if (fieldDefinitionLevels[i] >= rowDefinitionLevel) {
                // current row is defined and not empty
                nullRowFlags.add(false);
            } else {
                // current row is null
                nullRowFlags.add(true);
                nullValuesCount++;
            }
        }
        if (nullValuesCount == 0) {
            return new RowPosition(null, fieldDefinitionLevels.length);
        }
        return new RowPosition(nullRowFlags.toArray(), nullRowFlags.size());
    }

    /**
     * Calculate the collection's offsets according to column's max repetition level, definition
     * level, value's repetition level and definition level. Each collection (Array or Map) has four
     * situation:
     * <li>Collection is not defined, because optional parent fields is null, this is decided by its
     *     parent's repetition level
     * <li>Collection is null
     * <li>Collection is defined but empty
     * <li>Collection is defined and not empty. In this case offset value is increased by the number
     *     of elements in that collection
     *
     * @param field field that contains array/map column message include max repetition level and
     *     definition level.
     * @param definitionLevels int array with each value's repetition level.
     * @param repetitionLevels int array with each value's definition level.
     * @return {@link CollectionPosition} contains collections offset array, length array and isNull
     *     array.
     */
    public static CollectionPosition calculateCollectionOffsets(
            ParquetField field, int[] definitionLevels, int[] repetitionLevels) {
        int collectionDefinitionLevel = field.getDefinitionLevel();
        int collectionRepetitionLevel = field.getRepetitionLevel() + 1;
        int offset = 0;
        int valueCount = 0;
        LongArrayList offsets = new LongArrayList(0);
        offsets.add(offset);
        BooleanArrayList emptyCollectionFlags = new BooleanArrayList(0);
        BooleanArrayList nullCollectionFlags = new BooleanArrayList(0);
        int nullValuesCount = 0;
        for (int i = 0;
                i < definitionLevels.length;
                i = getNextCollectionStartIndex(repetitionLevels, collectionRepetitionLevel, i)) {
            valueCount++;
            if (definitionLevels[i] >= collectionDefinitionLevel - 1) {
                boolean isNull =
                        isOptionalFieldValueNull(definitionLevels[i], collectionDefinitionLevel);
                nullCollectionFlags.add(isNull);
                nullValuesCount += isNull ? 1 : 0;
                // definitionLevels[i] > collectionDefinitionLevel  => Collection is defined and not
                // empty
                // definitionLevels[i] == collectionDefinitionLevel => Collection is defined but
                // empty
                if (definitionLevels[i] > collectionDefinitionLevel) {
                    emptyCollectionFlags.add(false);
                    offset += getCollectionSize(repetitionLevels, collectionRepetitionLevel, i + 1);
                } else if (definitionLevels[i] == collectionDefinitionLevel) {
                    offset++;
                    emptyCollectionFlags.add(true);
                } else {
                    offset++;
                    emptyCollectionFlags.add(false);
                }
                offsets.add(offset);
            } else {
                // when definitionLevels[i] < collectionDefinitionLevel - 1, it means the collection
                // is
                // not defined, but we need to regard it as null to avoid getting value wrong.
                nullCollectionFlags.add(true);
                nullValuesCount++;
                offsets.add(++offset);
                emptyCollectionFlags.add(false);
            }
        }
        long[] offsetsArray = offsets.toArray();
        long[] length = calculateLengthByOffsets(emptyCollectionFlags.toArray(), offsetsArray);
        if (nullValuesCount == 0) {
            return new CollectionPosition(null, offsetsArray, length, valueCount);
        }
        return new CollectionPosition(
                nullCollectionFlags.toArray(), offsetsArray, length, valueCount);
    }

    public static boolean isOptionalFieldValueNull(int definitionLevel, int maxDefinitionLevel) {
        return definitionLevel == maxDefinitionLevel - 1;
    }

    public static long[] calculateLengthByOffsets(
            boolean[] collectionIsEmpty, long[] arrayOffsets) {
        LongArrayList lengthList = new LongArrayList(arrayOffsets.length);
        for (int i = 0; i < arrayOffsets.length - 1; i++) {
            long offset = arrayOffsets[i];
            long length = arrayOffsets[i + 1] - offset;
            if (length < 0) {
                throw new IllegalArgumentException(
                        format(
                                "Offset is not monotonically ascending. offsets[%s]=%s, offsets[%s]=%s",
                                i, arrayOffsets[i], i + 1, arrayOffsets[i + 1]));
            }
            if (collectionIsEmpty[i]) {
                length = 0;
            }
            lengthList.add(length);
        }
        return lengthList.toArray();
    }

    private static int getNextCollectionStartIndex(
            int[] repetitionLevels, int maxRepetitionLevel, int elementIndex) {
        do {
            elementIndex++;
        } while (hasMoreElements(repetitionLevels, elementIndex)
                && isNotCollectionBeginningMarker(
                        repetitionLevels, maxRepetitionLevel, elementIndex));
        return elementIndex;
    }

    /** This method is only called for non-empty collections. */
    private static int getCollectionSize(
            int[] repetitionLevels, int maxRepetitionLevel, int nextIndex) {
        int size = 1;
        while (hasMoreElements(repetitionLevels, nextIndex)
                && isNotCollectionBeginningMarker(
                        repetitionLevels, maxRepetitionLevel, nextIndex)) {
            // Collection elements cannot only be primitive, but also can have nested structure
            // Counting only elements which belong to current collection, skipping inner elements of
            // nested collections/structs
            if (repetitionLevels[nextIndex] <= maxRepetitionLevel) {
                size++;
            }
            nextIndex++;
        }
        return size;
    }

    private static boolean isNotCollectionBeginningMarker(
            int[] repetitionLevels, int maxRepetitionLevel, int nextIndex) {
        return repetitionLevels[nextIndex] >= maxRepetitionLevel;
    }

    private static boolean hasMoreElements(int[] repetitionLevels, int nextIndex) {
        return nextIndex < repetitionLevels.length;
    }
}
