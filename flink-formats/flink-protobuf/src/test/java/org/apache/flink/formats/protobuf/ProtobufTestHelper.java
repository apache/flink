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

package org.apache.flink.formats.protobuf;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.protobuf.deserialize.PbRowDataDeserializationSchema;
import org.apache.flink.formats.protobuf.serialize.PbRowDataSerializationSchema;
import org.apache.flink.formats.protobuf.util.PbFormatUtils;
import org.apache.flink.formats.protobuf.util.PbToRowTypeUtil;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.types.utils.TypeConversions.fromLogicalToDataType;

/** Test helper class for protobuf format. */
public class ProtobufTestHelper {
    public static RowData validateRow(RowData rowData, RowType rowType) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        StreamTableEnvironment tableEnv =
                StreamTableEnvironment.create(
                        env, EnvironmentSettings.newInstance().inStreamingMode().build());

        DataType rowDataType = fromLogicalToDataType(rowType);
        Row row =
                (Row) DataFormatConverters.getConverterForDataType(rowDataType).toExternal(rowData);
        TypeInformation<Row> rowTypeInfo =
                (TypeInformation<Row>) TypeConversions.fromDataTypeToLegacyInfo(rowDataType);
        DataStream<Row> rows = env.fromCollection(Collections.singletonList(row), rowTypeInfo);

        Table table = tableEnv.fromDataStream(rows);
        tableEnv.createTemporaryView("t", table);
        table = tableEnv.sqlQuery("select * from t");
        List<RowData> resultRows =
                tableEnv.toAppendStream(table, InternalTypeInfo.of(rowType)).executeAndCollect(1);
        return resultRows.get(0);
    }

    public static byte[] rowToPbBytes(RowData row, Class messageClass) throws Exception {
        return rowToPbBytes(row, messageClass, false);
    }

    public static byte[] rowToPbBytes(RowData row, Class messageClass, boolean enumAsInt)
            throws Exception {
        return rowToPbBytes(
                row,
                messageClass,
                new PbFormatConfig(messageClass.getName(), false, false, ""),
                enumAsInt);
    }

    public static byte[] rowToPbBytes(
            RowData row, Class messageClass, PbFormatConfig formatConfig, boolean enumAsInt)
            throws Exception {
        RowType rowType =
                PbToRowTypeUtil.generateRowType(
                        PbFormatUtils.getDescriptor(messageClass.getName()), enumAsInt);
        row = validateRow(row, rowType);
        PbRowDataSerializationSchema serializationSchema =
                new PbRowDataSerializationSchema(rowType, formatConfig);
        serializationSchema.open(null);
        byte[] bytes = serializationSchema.serialize(row);
        return bytes;
    }

    public static RowData pbBytesToRow(Class messageClass, byte[] bytes) throws Exception {
        return pbBytesToRow(messageClass, bytes, false);
    }

    public static RowData pbBytesToRow(Class messageClass, byte[] bytes, boolean enumAsInt)
            throws Exception {
        return pbBytesToRow(
                messageClass,
                bytes,
                new PbFormatConfig(messageClass.getName(), false, false, ""),
                enumAsInt);
    }

    public static RowData pbBytesToRow(
            Class messageClass, byte[] bytes, PbFormatConfig formatConfig, boolean enumAsInt)
            throws Exception {
        RowType rowType =
                PbToRowTypeUtil.generateRowType(
                        PbFormatUtils.getDescriptor(messageClass.getName()), enumAsInt);
        PbRowDataDeserializationSchema deserializationSchema =
                new PbRowDataDeserializationSchema(
                        rowType, InternalTypeInfo.of(rowType), formatConfig);
        deserializationSchema.open(null);
        RowData row = deserializationSchema.deserialize(bytes);
        return ProtobufTestHelper.validateRow(row, rowType);
    }

    public static <K, V> Map<K, V> mapOf(Object... keyValues) {
        Map<K, V> map = new HashMap<>();

        for (int index = 0; index < keyValues.length / 2; index++) {
            map.put((K) keyValues[index * 2], (V) keyValues[index * 2 + 1]);
        }

        return map;
    }
}
