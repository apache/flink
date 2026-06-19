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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.formats.parquet.utils.NestedPositionUtil;
import org.apache.flink.formats.parquet.vector.position.CollectionPosition;
import org.apache.flink.formats.parquet.vector.position.LevelDelegation;
import org.apache.flink.formats.parquet.vector.position.RowPosition;
import org.apache.flink.formats.parquet.vector.type.ParquetField;
import org.apache.flink.formats.parquet.vector.type.ParquetGroupField;
import org.apache.flink.formats.parquet.vector.type.ParquetPrimitiveField;
import org.apache.flink.table.data.columnar.vector.ColumnVector;
import org.apache.flink.table.data.columnar.vector.heap.AbstractHeapVector;
import org.apache.flink.table.data.columnar.vector.heap.HeapArrayVector;
import org.apache.flink.table.data.columnar.vector.heap.HeapMapVector;
import org.apache.flink.table.data.columnar.vector.heap.HeapRowVector;
import org.apache.flink.table.data.columnar.vector.writable.WritableColumnVector;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReadStore;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This ColumnReader mainly used to read `Group` type in parquet such as `Map`, `Array`, `Row`. The
 * method about how to resolve nested struct mainly refer to : <a
 * href="https://github.com/julienledem/redelm/wiki/The-striping-and-assembly-algorithms-from-the-Dremel-paper">The
 * striping and assembly algorithms from the Dremel paper</a>.
 *
 * <p>Brief explanation of reading repetition and definition levels: Repetition level equal to 0
 * means that this is the beginning of a new row. Other value means that we should add data to the
 * current row.
 *
 * <p>For example, if we have the following data: repetition levels: 0,1,1,0,0,1,[0] (last 0 is
 * implicit, normally will be the end of the page) values: a,b,c,d,e,f will consist of the sets of:
 * (a, b, c), (d), (e, f). <br>
 *
 * <p>Definition levels contains 3 situations: level = maxDefLevel means value exist and is not null
 * level = maxDefLevel - 1 means value is null level < maxDefLevel - 1 means value doesn't exist For
 * non-nullable (REQUIRED) fields the (level = maxDefLevel - 1) condition means non-existing value
 * as well. <br>
 *
 * <p>Quick example (maxDefLevel is 2): Read 3 rows out of: repetition levels: 0,1,0,1,1,0,0,...
 * definition levels: 2,1,0,2,1,2,... values: a,b,c,d,e,f,... Resulting buffer: a,n, ,d,n,f that
 * result is (a,n),(d,n),(f) where n means null
 */
public class NestedColumnReader implements ColumnReader<WritableColumnVector> {

    private final Map<ColumnDescriptor, NestedPrimitiveColumnReader> columnReaders;
    private final boolean isUtcTimestamp;

    private final PageReadStore pages;

    private final ParquetField field;

    public NestedColumnReader(boolean isUtcTimestamp, PageReadStore pages, ParquetField field) {
        this.isUtcTimestamp = isUtcTimestamp;
        this.pages = pages;
        this.field = field;
        this.columnReaders = new HashMap<>();
    }

    @Override
    public void readToVector(int readNumber, WritableColumnVector vector) throws IOException {
        readData(field, readNumber, vector, false);
    }

    private Tuple2<LevelDelegation, WritableColumnVector> readData(
            ParquetField field, int readNumber, ColumnVector vector, boolean inside)
            throws IOException {
        if (field.getType() instanceof RowType) {
            return readRow((ParquetGroupField) field, readNumber, vector, inside);
        } else if (field.getType() instanceof MapType || field.getType() instanceof MultisetType) {
            return readMap((ParquetGroupField) field, readNumber, vector, inside);
        } else if (field.getType() instanceof ArrayType) {
            return readArray((ParquetGroupField) field, readNumber, vector, inside);
        } else {
            return readPrimitive((ParquetPrimitiveField) field, readNumber, vector);
        }
    }

    private Tuple2<LevelDelegation, WritableColumnVector> readRow(
            ParquetGroupField field, int readNumber, ColumnVector vector, boolean inside)
            throws IOException {
        HeapRowVector heapRowVector = (HeapRowVector) vector;
        LevelDelegation levelDelegation = null;
        List<ParquetField> children = field.getChildren();
        WritableColumnVector[] childrenVectors = heapRowVector.getFields();
        WritableColumnVector[] finalChildrenVectors =
                new WritableColumnVector[childrenVectors.length];
        for (int i = 0; i < children.size(); i++) {
            Tuple2<LevelDelegation, WritableColumnVector> tuple =
                    readData(children.get(i), readNumber, childrenVectors[i], true);
            levelDelegation = tuple.f0;
            finalChildrenVectors[i] = tuple.f1;
        }
        if (levelDelegation == null) {
            throw new FlinkRuntimeException(
                    String.format("Row field does not have any children: %s.", field));
        }

        RowPosition rowPosition =
                NestedPositionUtil.calculateRowOffsets(
                        field,
                        levelDelegation.getDefinitionLevel(),
                        levelDelegation.getRepetitionLevel());

        // If row was inside the structure, then we need to renew the vector to reset the
        // capacity.
        if (inside) {
            heapRowVector =
                    new HeapRowVector(rowPosition.getPositionsCount(), finalChildrenVectors);
        } else {
            heapRowVector.setFields(finalChildrenVectors);
        }

        if (rowPosition.getIsNull() != null) {
            setFieldNullFalg(rowPosition.getIsNull(), heapRowVector);
        }
        return Tuple2.of(levelDelegation, heapRowVector);
    }

    private Tuple2<LevelDelegation, WritableColumnVector> readMap(
            ParquetGroupField field, int readNumber, ColumnVector vector, boolean inside)
            throws IOException {
        HeapMapVector mapVector = (HeapMapVector) vector;
        mapVector.reset();
        List<ParquetField> children = field.getChildren();
        Preconditions.checkArgument(
                children.size() == 2,
                "Maps must have two type parameters, found %s",
                children.size());
        Tuple2<LevelDelegation, WritableColumnVector> keyTuple =
                readData(children.get(0), readNumber, mapVector.getKeyColumnVector(), true);
        Tuple2<LevelDelegation, WritableColumnVector> valueTuple =
                readData(children.get(1), readNumber, mapVector.getValueColumnVector(), true);

        LevelDelegation levelDelegation = keyTuple.f0;

        CollectionPosition collectionPosition =
                NestedPositionUtil.calculateCollectionOffsets(
                        field,
                        levelDelegation.getDefinitionLevel(),
                        levelDelegation.getRepetitionLevel());

        // If map was inside the structure, then we need to renew the vector to reset the
        // capacity.
        if (inside) {
            mapVector =
                    new HeapMapVector(
                            collectionPosition.getValueCount(), keyTuple.f1, valueTuple.f1);
        } else {
            mapVector.setKeys(keyTuple.f1);
            mapVector.setValues(valueTuple.f1);
        }

        if (collectionPosition.getIsNull() != null) {
            setFieldNullFalg(collectionPosition.getIsNull(), mapVector);
        }

        mapVector.setLengths(collectionPosition.getLength());
        mapVector.setOffsets(collectionPosition.getOffsets());

        return Tuple2.of(levelDelegation, mapVector);
    }

    private Tuple2<LevelDelegation, WritableColumnVector> readArray(
            ParquetGroupField field, int readNumber, ColumnVector vector, boolean inside)
            throws IOException {
        HeapArrayVector arrayVector = (HeapArrayVector) vector;
        arrayVector.reset();
        List<ParquetField> children = field.getChildren();
        Preconditions.checkArgument(
                children.size() == 1,
                "Arrays must have a single type parameter, found %s",
                children.size());
        Tuple2<LevelDelegation, WritableColumnVector> tuple =
                readData(children.get(0), readNumber, arrayVector.getChild(), true);

        LevelDelegation levelDelegation = tuple.f0;
        CollectionPosition collectionPosition =
                NestedPositionUtil.calculateCollectionOffsets(
                        field,
                        levelDelegation.getDefinitionLevel(),
                        levelDelegation.getRepetitionLevel());

        // If array was inside the structure, then we need to renew the vector to reset the
        // capacity.
        if (inside) {
            arrayVector = new HeapArrayVector(collectionPosition.getValueCount(), tuple.f1);
        } else {
            arrayVector.setChild(tuple.f1);
        }

        if (collectionPosition.getIsNull() != null) {
            setFieldNullFalg(collectionPosition.getIsNull(), arrayVector);
        }
        arrayVector.setLengths(collectionPosition.getLength());
        arrayVector.setOffsets(collectionPosition.getOffsets());
        return Tuple2.of(levelDelegation, arrayVector);
    }

    private Tuple2<LevelDelegation, WritableColumnVector> readPrimitive(
            ParquetPrimitiveField field, int readNumber, ColumnVector vector) throws IOException {
        ColumnDescriptor descriptor = field.getDescriptor();
        NestedPrimitiveColumnReader reader = columnReaders.get(descriptor);
        if (reader == null) {
            reader =
                    new NestedPrimitiveColumnReader(
                            descriptor,
                            pages.getPageReader(descriptor),
                            isUtcTimestamp,
                            descriptor.getPrimitiveType(),
                            field.getType());
            columnReaders.put(descriptor, reader);
        }
        WritableColumnVector writableColumnVector =
                reader.readAndNewVector(readNumber, (WritableColumnVector) vector);
        return Tuple2.of(reader.getLevelDelegation(), writableColumnVector);
    }

    private static void setFieldNullFalg(boolean[] nullFlags, AbstractHeapVector vector) {
        for (int index = 0; index < vector.getLen() && index < nullFlags.length; index++) {
            if (nullFlags[index]) {
                vector.setNullAt(index);
            }
        }
    }
}
