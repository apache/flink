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

package org.apache.flink.table.gateway.rest.util;

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.utils.DateTimeUtils;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.stream.Collectors;

/**
 * Convert {@link LocalZonedTimestampType} data in {@link RowData} to {@link TimestampData} based on
 * the local time zone.
 */
public class RowDataLocalTimeZoneConverter {
    private final List<RowData.FieldGetter> fieldGetterList;
    private final List<LogicalType> logicalTypeList;
    private final boolean hasTimeZoneData;
    private final TimeZone timeZone;

    public RowDataLocalTimeZoneConverter(List<LogicalType> logicalTypeList, ReadableConfig config) {
        this(logicalTypeList, TimeZone.getTimeZone(getSessionTimeZone(config)));
    }

    public RowDataLocalTimeZoneConverter(List<LogicalType> logicalTypeList, TimeZone timeZone) {
        this.logicalTypeList = logicalTypeList;
        this.timeZone = timeZone;
        this.fieldGetterList = new ArrayList<>(logicalTypeList.size());
        for (int i = 0; i < logicalTypeList.size(); i++) {
            fieldGetterList.add(RowData.createFieldGetter(logicalTypeList.get(i), i));
        }
        this.hasTimeZoneData = checkTimeZoneType(logicalTypeList);
    }

    private boolean checkTimeZoneType(List<LogicalType> logicalTypeList) {
        for (LogicalType logicalType : logicalTypeList) {
            if (logicalType.getTypeRoot() == LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
                return true;
            } else if (logicalType.getTypeRoot() == LogicalTypeRoot.MAP) {
                MapType mapType = (MapType) logicalType;
                if (checkTimeZoneType(
                        Arrays.asList(mapType.getKeyType(), mapType.getValueType()))) {
                    return true;
                }
            } else if (logicalType.getTypeRoot() == LogicalTypeRoot.ARRAY) {
                ArrayType arrayType = (ArrayType) logicalType;
                if (checkTimeZoneType(Collections.singletonList(arrayType.getElementType()))) {
                    return true;
                }
            } else if (logicalType.getTypeRoot() == LogicalTypeRoot.MULTISET) {
                MultisetType multisetType = (MultisetType) logicalType;
                if (checkTimeZoneType(Collections.singletonList(multisetType.getElementType()))) {
                    return true;
                }
            } else if (logicalType.getTypeRoot() == LogicalTypeRoot.ROW) {
                RowType rowType = (RowType) logicalType;
                if (checkTimeZoneType(
                        rowType.getFields().stream()
                                .map(RowType.RowField::getType)
                                .collect(Collectors.toList()))) {
                    return true;
                }
            }
        }
        return false;
    }

    public RowData convertTimeZoneRowData(RowData rowData) {
        if (!hasTimeZoneData()) {
            return rowData;
        }

        GenericRowData result = new GenericRowData(rowData.getRowKind(), rowData.getArity());
        for (int i = 0; i < fieldGetterList.size(); i++) {
            result.setField(
                    i,
                    convertLocalTimeZoneValue(
                            fieldGetterList.get(i).getFieldOrNull(rowData),
                            logicalTypeList.get(i)));
        }
        return result;
    }

    private Object convertLocalTimeZoneValue(Object object, LogicalType dataType) {
        if (object == null) {
            return null;
        }
        switch (dataType.getTypeRoot()) {
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                {
                    return DateTimeUtils.timestampWithLocalZoneToTimestamp(
                            (TimestampData) object, timeZone);
                }
            case MAP:
                {
                    MapType mapType = (MapType) dataType;
                    MapData mapData = (MapData) object;
                    ArrayData keyArray = mapData.keyArray();
                    ArrayData valueArray = mapData.valueArray();
                    ArrayData.ElementGetter keyGetter =
                            ArrayData.createElementGetter(mapType.getKeyType());
                    ArrayData.ElementGetter valueGetter =
                            ArrayData.createElementGetter(mapType.getValueType());
                    Map<Object, Object> mapValue = new HashMap<>();
                    for (int j = 0; j < keyArray.size(); j++) {
                        mapValue.put(
                                convertLocalTimeZoneValue(
                                        keyGetter.getElementOrNull(keyArray, j),
                                        mapType.getKeyType()),
                                convertLocalTimeZoneValue(
                                        valueGetter.getElementOrNull(valueArray, j),
                                        mapType.getValueType()));
                    }
                    return new GenericMapData(mapValue);
                }
            case ARRAY:
                {
                    ArrayType arrayType = (ArrayType) dataType;
                    ArrayData arrayData = (ArrayData) object;
                    ArrayData.ElementGetter dataGetter =
                            ArrayData.createElementGetter(arrayType.getElementType());
                    List<Object> arrayValues = new ArrayList<>(arrayData.size());
                    for (int i = 0; i < arrayData.size(); i++) {
                        arrayValues.add(
                                convertLocalTimeZoneValue(
                                        dataGetter.getElementOrNull(arrayData, i),
                                        arrayType.getElementType()));
                    }
                    return new GenericArrayData(arrayValues.toArray());
                }
            default:
                {
                    return object;
                }
        }
    }

    public boolean hasTimeZoneData() {
        return hasTimeZoneData;
    }

    /** Get time zone from the given session config. */
    private static ZoneId getSessionTimeZone(ReadableConfig sessionConfig) {
        final String zone = sessionConfig.get(TableConfigOptions.LOCAL_TIME_ZONE);
        return TableConfigOptions.LOCAL_TIME_ZONE.defaultValue().equals(zone)
                ? ZoneId.systemDefault()
                : ZoneId.of(zone);
    }
}
