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

package org.apache.flink.table.planner.typeutils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.dataview.DataView;
import org.apache.flink.table.api.dataview.ListView;
import org.apache.flink.table.api.dataview.MapView;
import org.apache.flink.table.data.binary.LazyBinaryFormat;
import org.apache.flink.table.dataview.NullSerializer;
import org.apache.flink.table.runtime.typeutils.ExternalSerializer;
import org.apache.flink.table.types.CollectionDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.KeyValueDataType;
import org.apache.flink.table.types.inference.TypeTransformation;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RawType;
import org.apache.flink.table.types.logical.StructuredType;
import org.apache.flink.table.types.utils.DataTypeUtils;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.getFieldNames;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.hasNested;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.hasRoot;

/**
 * Utilities to deal with {@link DataView}s.
 *
 * <p>A {@link DataView} is either represented as a regular {@link StructuredType} or as a {@link
 * RawType} that serializes to {@code null} when backed by a state backend. In the latter case, a
 * {@link DataViewSpec} contains all information necessary to store and retrieve data from state.
 */
@Internal
public final class DataViewUtils {

    /** Searches for data views in the data type of an accumulator and extracts them. */
    public static List<DataViewSpec> extractDataViews(int aggIndex, DataType accumulatorDataType) {
        final LogicalType accumulatorType = accumulatorDataType.getLogicalType();
        if (!hasRoot(accumulatorType, LogicalTypeRoot.ROW)
                && !hasRoot(accumulatorType, LogicalTypeRoot.STRUCTURED_TYPE)) {
            return Collections.emptyList();
        }
        final List<String> fieldNames = getFieldNames(accumulatorType);
        final List<DataType> fieldDataTypes = accumulatorDataType.getChildren();

        final List<DataViewSpec> specs = new ArrayList<>();
        for (int fieldIndex = 0; fieldIndex < fieldDataTypes.size(); fieldIndex++) {
            final DataType fieldDataType = fieldDataTypes.get(fieldIndex);
            final LogicalType fieldType = fieldDataType.getLogicalType();
            if (isDataView(fieldType, ListView.class)) {
                specs.add(
                        new ListViewSpec(
                                createStateId(aggIndex, fieldNames.get(fieldIndex)),
                                fieldIndex,
                                fieldDataType.getChildren().get(0)));
            } else if (isDataView(fieldType, MapView.class)) {
                specs.add(
                        new MapViewSpec(
                                createStateId(aggIndex, fieldNames.get(fieldIndex)),
                                fieldIndex,
                                fieldDataType.getChildren().get(0),
                                false));
            }
            if (fieldType.getChildren().stream()
                    .anyMatch(c -> hasNested(c, t -> isDataView(t, DataView.class)))) {
                throw new TableException(
                        "Data views are only supported in the first level of a composite accumulator type.");
            }
        }
        return specs;
    }

    /**
     * Modifies the data type of an accumulator regarding data views.
     *
     * <p>For performance reasons, each data view is wrapped into a RAW type which gives it {@link
     * LazyBinaryFormat} semantics and avoids multiple deserialization steps during access.
     * Furthermore, a data view will not be serialized if a state backend is used (the serializer of
     * the RAW type will be a {@link NullSerializer} in this case).
     */
    public static DataType adjustDataViews(
            DataType accumulatorDataType, boolean hasStateBackedDataViews) {
        final Function<DataType, TypeSerializer<?>> serializer;
        if (hasStateBackedDataViews) {
            serializer = dataType -> NullSerializer.INSTANCE;
        } else {
            serializer = ExternalSerializer::of;
        }
        return DataTypeUtils.transform(
                accumulatorDataType, new DataViewsTransformation(serializer));
    }

    /** Creates a special {@link DataType} for DISTINCT aggregates. */
    public static DataType createDistinctViewDataType(
            DataType keyDataType, int filterArgs, int filterArgsLimit) {
        final DataType valueDataType;
        if (filterArgs <= filterArgsLimit) {
            valueDataType = DataTypes.BIGINT().notNull();
        } else {
            valueDataType = DataTypes.ARRAY(DataTypes.BIGINT().notNull()).bridgedTo(long[].class);
        }
        return MapView.newMapViewDataType(keyDataType, valueDataType);
    }

    /** Creates a special {@link DistinctViewSpec} for DISTINCT aggregates. */
    public static DistinctViewSpec createDistinctViewSpec(
            int index, DataType distinctViewDataType) {
        return new DistinctViewSpec("distinctAcc_" + index, distinctViewDataType);
    }

    // --------------------------------------------------------------------------------------------

    private static boolean isDataView(LogicalType t, Class<? extends DataView> viewClass) {
        return hasRoot(t, LogicalTypeRoot.STRUCTURED_TYPE)
                && ((StructuredType) t)
                        .getImplementationClass()
                        .map(viewClass::isAssignableFrom)
                        .orElse(false);
    }

    private static String createStateId(int fieldIndex, String fieldName) {
        return "agg" + fieldIndex + "$" + fieldName;
    }

    // --------------------------------------------------------------------------------------------

    private static class DataViewsTransformation implements TypeTransformation {

        private final Function<DataType, TypeSerializer<?>> serializer;

        private DataViewsTransformation(Function<DataType, TypeSerializer<?>> serializer) {
            this.serializer = serializer;
        }

        @Override
        @SuppressWarnings({"unchecked", "rawtypes"})
        public DataType transform(DataType dataType) {
            if (isDataView(dataType.getLogicalType(), DataView.class)) {
                return DataTypes.RAW(
                        dataType.getConversionClass(), (TypeSerializer) serializer.apply(dataType));
            }
            return dataType;
        }
    }

    // --------------------------------------------------------------------------------------------

    /** Information about a {@link DataView} stored in state. */
    public abstract static class DataViewSpec implements Serializable {

        private static final long serialVersionUID = 1L;

        private final String stateId;

        private final int fieldIndex;

        private final DataType dataType;

        private DataViewSpec(String stateId, int fieldIndex, DataType dataType) {
            this.stateId = stateId;
            this.fieldIndex = fieldIndex;
            this.dataType = dataType;
        }

        public String getStateId() {
            return stateId;
        }

        public int getFieldIndex() {
            return fieldIndex;
        }

        public DataType getDataType() {
            return dataType;
        }
    }

    /** Specification for a {@link ListView}. */
    public static class ListViewSpec extends DataViewSpec {

        private final @Nullable TypeSerializer<?> elementSerializer;

        public ListViewSpec(String stateId, int fieldIndex, DataType dataType) {
            this(stateId, fieldIndex, dataType, null);
        }

        @Deprecated
        public ListViewSpec(
                String stateId,
                int fieldIndex,
                DataType dataType,
                TypeSerializer<?> elementSerializer) {
            super(stateId, fieldIndex, dataType);
            this.elementSerializer = elementSerializer;
        }

        public DataType getElementDataType() {
            final CollectionDataType arrayDataType = (CollectionDataType) getDataType();
            return arrayDataType.getElementDataType();
        }

        public Optional<TypeSerializer<?>> getElementSerializer() {
            return Optional.ofNullable(elementSerializer);
        }
    }

    /** Specification for a {@link MapView}. */
    public static class MapViewSpec extends DataViewSpec {

        private final boolean containsNullKey;

        private final @Nullable TypeSerializer<?> keySerializer;

        private final @Nullable TypeSerializer<?> valueSerializer;

        public MapViewSpec(
                String stateId, int fieldIndex, DataType dataType, boolean containsNullKey) {
            this(stateId, fieldIndex, dataType, containsNullKey, null, null);
        }

        @Deprecated
        public MapViewSpec(
                String stateId,
                int fieldIndex,
                DataType dataType,
                boolean containsNullKey,
                TypeSerializer<?> keySerializer,
                TypeSerializer<?> valueSerializer) {
            super(stateId, fieldIndex, dataType);
            this.containsNullKey = containsNullKey;
            this.keySerializer = keySerializer;
            this.valueSerializer = valueSerializer;
        }

        public DataType getKeyDataType() {
            final KeyValueDataType mapDataType = (KeyValueDataType) getDataType();
            return mapDataType.getKeyDataType();
        }

        public DataType getValueDataType() {
            final KeyValueDataType mapDataType = (KeyValueDataType) getDataType();
            return mapDataType.getValueDataType();
        }

        public Optional<TypeSerializer<?>> getKeySerializer() {
            return Optional.ofNullable(keySerializer);
        }

        public Optional<TypeSerializer<?>> getValueSerializer() {
            return Optional.ofNullable(valueSerializer);
        }

        public boolean containsNullKey() {
            return containsNullKey;
        }
    }

    /** Specification for a special {@link MapView} for deduplication. */
    public static class DistinctViewSpec extends MapViewSpec {

        public DistinctViewSpec(String stateId, DataType distinctViewDataType) {
            super(
                    stateId,
                    -1,
                    distinctViewDataType.getChildren().get(0),
                    true); // handles null keys
        }
    }

    private DataViewUtils() {
        // no instantiation
    }
}
