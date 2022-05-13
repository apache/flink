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

package org.apache.flink.table.runtime.types;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.CollectionDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.DataTypeVisitor;
import org.apache.flink.table.types.FieldsDataType;
import org.apache.flink.table.types.KeyValueDataType;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampKind;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.ZonedTimestampType;

import java.util.List;
import java.util.stream.IntStream;

import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.getFieldNames;

/**
 * The data type visitor used to fix the precision for data type with the given logical type which
 * carries the correct precisions. The original data type may loses precision because of conversion
 * from {@link org.apache.flink.api.common.typeinfo.TypeInformation}.
 */
public final class DataTypePrecisionFixer implements DataTypeVisitor<DataType> {

    private final LogicalType logicalType;

    /**
     * Creates a new instance with the given logical type.
     *
     * @param logicalType the logical type which carries the correct precisions.
     */
    public DataTypePrecisionFixer(LogicalType logicalType) {
        this.logicalType = logicalType;
    }

    @Override
    public DataType visit(AtomicDataType dataType) {
        switch (logicalType.getTypeRoot()) {
            case DECIMAL:
                DecimalType decimalType = (DecimalType) logicalType;
                return DataTypes
                        // fix the precision and scale, because precision may lose or not correct.
                        // precision from DDL is the only source of truth.
                        // we don't care about nullability for now.
                        .DECIMAL(decimalType.getPrecision(), decimalType.getScale())
                        // only keep the original conversion class
                        .bridgedTo(dataType.getConversionClass());

            case TIMESTAMP_WITHOUT_TIME_ZONE:
                TimestampType timestampType = (TimestampType) logicalType;
                if (timestampType.getKind() == TimestampKind.REGULAR) {
                    return DataTypes.TIMESTAMP(timestampType.getPrecision())
                            .bridgedTo(dataType.getConversionClass());
                } else {
                    // keep the original type if it is time attribute type
                    // because time attribute can only be precision 3
                    // and the original type may be BIGINT.
                    return dataType;
                }

            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                LocalZonedTimestampType localZonedTimestampType =
                        (LocalZonedTimestampType) logicalType;
                return DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(
                                localZonedTimestampType.getPrecision())
                        .bridgedTo(dataType.getConversionClass());

            case TIMESTAMP_WITH_TIME_ZONE:
                ZonedTimestampType zonedTimestampType = (ZonedTimestampType) logicalType;
                return DataTypes.TIMESTAMP_WITH_TIME_ZONE(zonedTimestampType.getPrecision())
                        .bridgedTo(dataType.getConversionClass());

            case TIME_WITHOUT_TIME_ZONE:
                TimeType timeType = (TimeType) logicalType;
                return DataTypes.TIME(timeType.getPrecision())
                        .bridgedTo(dataType.getConversionClass());

            default:
                return dataType;
        }
    }

    @Override
    public DataType visit(CollectionDataType collectionDataType) {
        DataType elementType = collectionDataType.getElementDataType();
        switch (logicalType.getTypeRoot()) {
            case ARRAY:
                ArrayType arrayType = (ArrayType) logicalType;
                DataType newArrayElementType =
                        elementType.accept(new DataTypePrecisionFixer(arrayType.getElementType()));
                return DataTypes.ARRAY(newArrayElementType)
                        .bridgedTo(collectionDataType.getConversionClass());

            case MULTISET:
                MultisetType multisetType = (MultisetType) logicalType;
                DataType newMultisetElementType =
                        elementType.accept(
                                new DataTypePrecisionFixer(multisetType.getElementType()));
                return DataTypes.MULTISET(newMultisetElementType)
                        .bridgedTo(collectionDataType.getConversionClass());

            default:
                throw new UnsupportedOperationException(
                        "Unsupported logical type : " + logicalType);
        }
    }

    @Override
    public DataType visit(FieldsDataType fieldsDataType) {
        final List<DataType> fieldDataTypes = fieldsDataType.getChildren();
        if (logicalType.getTypeRoot() == LogicalTypeRoot.ROW) {
            final List<String> fieldNames = getFieldNames(logicalType);
            DataTypes.Field[] fields =
                    IntStream.range(0, fieldDataTypes.size())
                            .mapToObj(
                                    i -> {
                                        final DataType oldFieldType = fieldDataTypes.get(i);
                                        final DataType newFieldType =
                                                oldFieldType.accept(
                                                        new DataTypePrecisionFixer(
                                                                logicalType.getChildren().get(i)));
                                        return DataTypes.FIELD(fieldNames.get(i), newFieldType);
                                    })
                            .toArray(DataTypes.Field[]::new);
            return DataTypes.ROW(fields).bridgedTo(fieldsDataType.getConversionClass());
        }
        throw new UnsupportedOperationException("Unsupported logical type : " + logicalType);
    }

    @Override
    public DataType visit(KeyValueDataType keyValueDataType) {
        DataType keyType = keyValueDataType.getKeyDataType();
        DataType valueType = keyValueDataType.getValueDataType();
        if (logicalType.getTypeRoot() == LogicalTypeRoot.MAP) {
            MapType mapType = (MapType) logicalType;
            DataType newKeyType = keyType.accept(new DataTypePrecisionFixer(mapType.getKeyType()));
            DataType newValueType =
                    valueType.accept(new DataTypePrecisionFixer(mapType.getValueType()));
            return DataTypes.MAP(newKeyType, newValueType)
                    .bridgedTo(keyValueDataType.getConversionClass());
        }
        throw new UnsupportedOperationException("Unsupported logical type : " + logicalType);
    }
}
