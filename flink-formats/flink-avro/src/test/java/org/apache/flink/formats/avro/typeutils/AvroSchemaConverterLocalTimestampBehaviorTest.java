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
package org.apache.flink.formats.avro.typeutils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Test;

class AvroSchemaConverterLocalTimestampBehaviorTest {

    private static String record(String fieldName, String logicalType) {
        return "{ \"type\": \"record\", \"name\": \"R\", \"fields\": ["
                + "{ \"name\": \""
                + fieldName
                + "\", \"type\": { \"type\": \"long\", \"logicalType\": \""
                + logicalType
                + "\" } } ] }";
    }

    private static DataType firstFieldType(DataType row) {
        RowType rowType = (RowType) row.getLogicalType();
        return DataTypes.of(rowType.getTypeAt(0)).notNull();
    }

    @Test
    void readTimestampMillisSingleArg() {
        DataType dt = AvroSchemaConverter.convertToDataType(record("ts", "timestamp-millis"));
        assertThat(firstFieldType(dt)).isEqualTo(DataTypes.TIMESTAMP(3).notNull());
    }

    @Test
    void readTimestampMicrosSingleArg() {
        DataType dt = AvroSchemaConverter.convertToDataType(record("ts", "timestamp-micros"));
        assertThat(firstFieldType(dt)).isEqualTo(DataTypes.TIMESTAMP(6).notNull());
    }

    @Test
    void readLocalTimestampMillisSingleArg() {
        DataType dt =
                AvroSchemaConverter.convertToDataType(record("ts", "local-timestamp-millis"));
        assertThat(firstFieldType(dt)).isEqualTo(DataTypes.TIMESTAMP(3).notNull());
    }

    @Test
    void readLocalTimestampMicrosSingleArg() {
        DataType dt =
                AvroSchemaConverter.convertToDataType(record("ts", "local-timestamp-micros"));
        assertThat(firstFieldType(dt)).isEqualTo(DataTypes.TIMESTAMP(6).notNull());
    }

    @Test
    void readTimestampMillisTwoArgLegacyFalse() {
        DataType dt =
                AvroSchemaConverter.convertToDataType(record("ts", "timestamp-millis"), false);
        assertThat(firstFieldType(dt))
                .isEqualTo(DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3).notNull());
    }

    @Test
    void readLocalTimestampMillisTwoArgLegacyFalse() {
        DataType dt =
                AvroSchemaConverter.convertToDataType(
                        record("ts", "local-timestamp-millis"), false);
        assertThat(firstFieldType(dt)).isEqualTo(DataTypes.TIMESTAMP(3).notNull());
    }

    @Test
    void readLocalTimestampMicrosTwoArgLegacyFalse() {
        DataType dt =
                AvroSchemaConverter.convertToDataType(
                        record("ts", "local-timestamp-micros"), false);
        assertThat(firstFieldType(dt)).isEqualTo(DataTypes.TIMESTAMP(6).notNull());
    }

    @Test
    void readLocalTimestampMillisTwoArgLegacyTrue() {
        DataType dt =
                AvroSchemaConverter.convertToDataType(record("ts", "local-timestamp-millis"), true);
        assertThat(firstFieldType(dt)).isEqualTo(DataTypes.TIMESTAMP(3).notNull());
    }

    @Test
    void typeInfoLocalTimestampMicrosSingleArg() {
        TypeInformation<?> ti =
                AvroSchemaConverter.convertToTypeInfo(record("ts", "local-timestamp-micros"));
        RowTypeInfoLike.assertFirstField(ti, Types.LOCAL_DATE_TIME);
    }

    @Test
    void typeInfoLocalTimestampMicrosTwoArgLegacyFalse() {
        TypeInformation<?> ti =
                AvroSchemaConverter.convertToTypeInfo(
                        record("ts", "local-timestamp-micros"), false);
        RowTypeInfoLike.assertFirstField(ti, Types.LOCAL_DATE_TIME);
    }

    @Test
    void typeInfoLocalTimestampMicrosTwoArgLegacyTrue() {
        TypeInformation<?> ti =
                AvroSchemaConverter.convertToTypeInfo(
                        record("ts", "local-timestamp-micros"), true);
        RowTypeInfoLike.assertFirstField(ti, Types.LOCAL_DATE_TIME);
    }

    @Test
    void fieldGetterFromConvertedRowTypeAcceptsTimestampData() {
        DataType dt =
                AvroSchemaConverter.convertToDataType(record("ts", "local-timestamp-micros"));
        RowType row = (RowType) dt.getLogicalType();
        RowData.FieldGetter getter = RowData.createFieldGetter(row.getTypeAt(0), 0);
        GenericRowData data = new GenericRowData(1);
        data.setField(0, TimestampData.fromEpochMillis(0L));
        assertThatCode(() -> getter.getFieldOrNull(data)).doesNotThrowAnyException();
    }

    @Test
    void roundTripTimestampMicrosTwoArgLegacyFalse() {
        org.apache.avro.Schema schema =
                AvroSchemaConverter.convertToSchema(
                        DataTypes.ROW(DataTypes.FIELD("ts", DataTypes.TIMESTAMP(6).notNull()))
                                .getLogicalType(),
                        false);
        DataType dt = AvroSchemaConverter.convertToDataType(schema.toString(), false);
        assertThat(firstFieldType(dt)).isEqualTo(DataTypes.TIMESTAMP(6).notNull());
    }

    private static final class RowTypeInfoLike {
        static void assertFirstField(TypeInformation<?> rowTypeInfo, TypeInformation<?> expected) {
            assertThat(rowTypeInfo).isInstanceOf(org.apache.flink.api.java.typeutils.RowTypeInfo.class);
            org.apache.flink.api.java.typeutils.RowTypeInfo r =
                    (org.apache.flink.api.java.typeutils.RowTypeInfo) rowTypeInfo;
            assertThat(r.getTypeAt(0)).isEqualTo(expected);
        }
    }
}
