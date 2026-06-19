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

package org.apache.flink.table.runtime.arrow.writers;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Preconditions;

import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;

/** {@link ArrowFieldWriter} for Map. */
@Internal
public abstract class MapWriter<T> extends ArrowFieldWriter<T> {

    public static MapWriter<RowData> forRow(
            MapVector mapVector,
            ArrowFieldWriter<ArrayData> keyWriter,
            ArrowFieldWriter<ArrayData> valueWriter) {
        return new MapWriterForRow(mapVector, keyWriter, valueWriter);
    }

    public static MapWriter<ArrayData> forArray(
            MapVector mapVector,
            ArrowFieldWriter<ArrayData> keyWriter,
            ArrowFieldWriter<ArrayData> valueWriter) {
        return new MapWriterForArray(mapVector, keyWriter, valueWriter);
    }

    // ------------------------------------------------------------------------------------------

    private final ArrowFieldWriter<ArrayData> keyWriter;

    private final ArrowFieldWriter<ArrayData> valueWriter;

    private MapWriter(
            MapVector mapVector,
            ArrowFieldWriter<ArrayData> keyWriter,
            ArrowFieldWriter<ArrayData> valueWriter) {
        super(mapVector);
        this.keyWriter = Preconditions.checkNotNull(keyWriter);
        this.valueWriter = Preconditions.checkNotNull(valueWriter);
    }

    abstract boolean isNullAt(T in, int ordinal);

    abstract MapData readMap(T in, int ordinal);

    @Override
    public void doWrite(T in, int ordinal) {
        if (!isNullAt(in, ordinal)) {
            ((MapVector) getValueVector()).startNewValue(getCount());

            StructVector structVector =
                    (StructVector) ((MapVector) getValueVector()).getDataVector();
            MapData map = readMap(in, ordinal);
            ArrayData keys = map.keyArray();
            ArrayData values = map.valueArray();
            for (int i = 0; i < map.size(); i++) {
                structVector.setIndexDefined(keyWriter.getCount());
                keyWriter.write(keys, i);
                valueWriter.write(values, i);
            }

            ((MapVector) getValueVector()).endValue(getCount(), map.size());
        }
    }

    // ------------------------------------------------------------------------------------------

    /** {@link MapWriter} for {@link RowData} input. */
    public static final class MapWriterForRow extends MapWriter<RowData> {

        private MapWriterForRow(
                MapVector mapVector,
                ArrowFieldWriter<ArrayData> keyWriter,
                ArrowFieldWriter<ArrayData> valueWriter) {
            super(mapVector, keyWriter, valueWriter);
        }

        @Override
        boolean isNullAt(RowData in, int ordinal) {
            return in.isNullAt(ordinal);
        }

        @Override
        MapData readMap(RowData in, int ordinal) {
            return in.getMap(ordinal);
        }
    }

    /** {@link MapWriter} for {@link ArrayData} input. */
    public static final class MapWriterForArray extends MapWriter<ArrayData> {

        private MapWriterForArray(
                MapVector mapVector,
                ArrowFieldWriter<ArrayData> keyWriter,
                ArrowFieldWriter<ArrayData> valueWriter) {
            super(mapVector, keyWriter, valueWriter);
        }

        @Override
        boolean isNullAt(ArrayData in, int ordinal) {
            return in.isNullAt(ordinal);
        }

        @Override
        MapData readMap(ArrayData in, int ordinal) {
            return in.getMap(ordinal);
        }
    }
}
