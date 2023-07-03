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

package org.apache.flink.table.jdbc.utils;

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.types.logical.DistinctType;
import org.apache.flink.table.types.logical.LogicalType;

import javax.annotation.Nullable;

import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.getFieldCount;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.getPrecision;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.getScale;

/** Creates an accessor for getting elements in an array data structure at the given position. */
public interface ArrayFieldGetter {
    @Nullable
    Object getObjectOrNull(ArrayData array, int index);

    static ArrayFieldGetter createFieldGetter(LogicalType type) {

        final ArrayFieldGetter fieldGetter;
        // ordered by type root definition
        switch (type.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                fieldGetter = ArrayData::getString;
                break;
            case BOOLEAN:
                fieldGetter = ArrayData::getBoolean;
                break;
            case BINARY:
            case VARBINARY:
                fieldGetter = ArrayData::getBinary;
                break;
            case DECIMAL:
                final int decimalPrecision = getPrecision(type);
                final int decimalScale = getScale(type);
                fieldGetter =
                        (array, index) -> array.getDecimal(index, decimalPrecision, decimalScale);
                break;
            case TINYINT:
                fieldGetter = ArrayData::getByte;
                break;
            case SMALLINT:
                fieldGetter = ArrayData::getShort;
                break;
            case INTEGER:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
            case INTERVAL_YEAR_MONTH:
                fieldGetter = ArrayData::getInt;
                break;
            case BIGINT:
            case INTERVAL_DAY_TIME:
                fieldGetter = ArrayData::getLong;
                break;
            case FLOAT:
                fieldGetter = ArrayData::getFloat;
                break;
            case DOUBLE:
                fieldGetter = ArrayData::getDouble;
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                final int timestampPrecision = getPrecision(type);
                fieldGetter = (array, index) -> array.getTimestamp(index, timestampPrecision);
                break;
            case TIMESTAMP_WITH_TIME_ZONE:
                throw new UnsupportedOperationException();
            case ARRAY:
                fieldGetter = ArrayData::getArray;
                break;
            case MULTISET:
            case MAP:
                fieldGetter = ArrayData::getMap;
                break;
            case ROW:
            case STRUCTURED_TYPE:
                final int arrayFieldCount = getFieldCount(type);
                fieldGetter = (array, index) -> array.getRow(index, arrayFieldCount);
                break;
            case DISTINCT_TYPE:
                fieldGetter = createFieldGetter(((DistinctType) type).getSourceType());
                break;
            case RAW:
                fieldGetter = ArrayData::getRawValue;
                break;
            case NULL:
            case SYMBOL:
            case UNRESOLVED:
            default:
                throw new IllegalArgumentException();
        }
        if (!type.isNullable()) {
            return fieldGetter;
        }
        return (array, index) -> {
            if (array.isNullAt(index)) {
                return null;
            }
            return fieldGetter.getObjectOrNull(array, index);
        };
    }
}
