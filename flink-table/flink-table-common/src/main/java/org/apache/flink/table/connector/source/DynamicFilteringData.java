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

package org.apache.flink.table.connector.source;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArrayComparator;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import java.io.ByteArrayInputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Data for dynamic filtering. */
@PublicEvolving
public class DynamicFilteringData implements Serializable {

    private final TypeInformation<RowData> typeInfo;
    private final RowType rowType;

    /**
     * Serialized rows for filtering. The types of the row values must be Flink internal data type,
     * i.e. type returned by the FieldGetter. The list should be sorted and distinct.
     */
    private final List<byte[]> serializedData;

    /** Whether the data actually does filter. If false, everything is considered contained. */
    private final boolean isFiltering;

    private transient volatile boolean prepared = false;
    private transient Map<Integer, List<RowData>> dataMap;
    private transient RowData.FieldGetter[] fieldGetters;

    public DynamicFilteringData(
            TypeInformation<RowData> typeInfo,
            RowType rowType,
            List<byte[]> serializedData,
            boolean isFiltering) {
        this.typeInfo = checkNotNull(typeInfo);
        this.rowType = checkNotNull(rowType);
        this.serializedData = checkNotNull(serializedData);
        this.isFiltering = isFiltering;
    }

    public boolean isFiltering() {
        return isFiltering;
    }

    public RowType getRowType() {
        return rowType;
    }

    /**
     * Returns true if the dynamic filtering data contains the specific row.
     *
     * @param row the row to be tested. Types of the row values must be Flink internal data type,
     *     i.e. type returned by the FieldGetter.
     * @return true if the dynamic filtering data contains the specific row
     */
    public boolean contains(RowData row) {
        if (!isFiltering) {
            return true;
        } else if (row.getArity() != rowType.getFieldCount()) {
            throw new TableException("The arity of RowData is different");
        } else {
            prepare();
            List<RowData> mayMatchRowData = dataMap.get(hash(row));
            if (mayMatchRowData == null) {
                return false;
            }
            for (RowData mayMatch : mayMatchRowData) {
                if (matchRow(row, mayMatch)) {
                    return true;
                }
            }
            return false;
        }
    }

    private boolean matchRow(RowData row, RowData mayMatch) {
        for (int i = 0; i < rowType.getFieldCount(); ++i) {
            if (!Objects.equals(
                    fieldGetters[i].getFieldOrNull(row),
                    fieldGetters[i].getFieldOrNull(mayMatch))) {
                return false;
            }
        }
        return true;
    }

    private void prepare() {
        if (!prepared) {
            synchronized (this) {
                if (!prepared) {
                    doPrepare();
                    prepared = true;
                }
            }
        }
    }

    private void doPrepare() {
        this.dataMap = new HashMap<>();
        if (isFiltering) {
            this.fieldGetters =
                    IntStream.range(0, rowType.getFieldCount())
                            .mapToObj(i -> RowData.createFieldGetter(rowType.getTypeAt(i), i))
                            .toArray(RowData.FieldGetter[]::new);

            TypeSerializer<RowData> serializer = typeInfo.createSerializer(new ExecutionConfig());
            for (byte[] bytes : serializedData) {
                try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
                        DataInputViewStreamWrapper inView = new DataInputViewStreamWrapper(bais)) {
                    RowData partition = serializer.deserialize(inView);
                    List<RowData> partitions =
                            dataMap.computeIfAbsent(hash(partition), k -> new ArrayList<>());
                    partitions.add(partition);
                } catch (Exception e) {
                    throw new TableException("Unable to deserialize the value.", e);
                }
            }
        }
    }

    private int hash(RowData row) {
        return Objects.hash(Arrays.stream(fieldGetters).map(g -> g.getFieldOrNull(row)).toArray());
    }

    public static boolean isEqual(DynamicFilteringData data, DynamicFilteringData another) {
        if (data == null) {
            return another == null;
        }
        if (another == null
                || (data.isFiltering != another.isFiltering)
                || !data.typeInfo.equals(another.typeInfo)
                || !data.rowType.equals(another.rowType)
                || data.serializedData.size() != another.serializedData.size()) {
            return false;
        }

        BytePrimitiveArrayComparator comparator = new BytePrimitiveArrayComparator(true);
        for (int i = 0; i < data.serializedData.size(); i++) {
            if (comparator.compare(data.serializedData.get(i), another.serializedData.get(i))
                    != 0) {
                return false;
            }
        }
        return true;
    }

    @VisibleForTesting
    public Collection<RowData> getData() {
        prepare();
        return dataMap.values().stream().flatMap(List::stream).collect(Collectors.toList());
    }

    @Override
    public String toString() {
        return "DynamicFilteringData{"
                + "isFiltering="
                + isFiltering
                + ", data size="
                + serializedData.size()
                + '}';
    }
}
