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

package org.apache.flink.formats.protobuf.deserialize;

import org.apache.flink.formats.protobuf.PbCodegenAppender;
import org.apache.flink.formats.protobuf.PbCodegenException;
import org.apache.flink.formats.protobuf.PbCodegenUtils;
import org.apache.flink.formats.protobuf.PbCodegenVarId;
import org.apache.flink.formats.protobuf.PbConstant;
import org.apache.flink.formats.protobuf.PbFormatConfig;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;

import com.google.protobuf.Descriptors;

/** Deserializer to convert proto map type object to flink map type data. */
public class PbCodegenMapDeserializer implements PbCodegenDeserializer {
    private final Descriptors.FieldDescriptor fd;
    private final MapType mapType;
    private final PbFormatConfig formatConfig;

    public PbCodegenMapDeserializer(
            Descriptors.FieldDescriptor fd, MapType mapType, PbFormatConfig formatConfig) {
        this.fd = fd;
        this.mapType = mapType;
        this.formatConfig = formatConfig;
    }

    @Override
    public String codegen(String returnInternalDataVarName, String pbGetStr)
            throws PbCodegenException {
        // The type of messageGetStr is a native Map object,
        // it should be converted to MapData of flink internal type
        PbCodegenVarId varUid = PbCodegenVarId.getInstance();
        int uid = varUid.getAndIncrement();

        LogicalType keyType = mapType.getKeyType();
        LogicalType valueType = mapType.getValueType();
        Descriptors.FieldDescriptor keyFd =
                fd.getMessageType().findFieldByName(PbConstant.PB_MAP_KEY_NAME);
        Descriptors.FieldDescriptor valueFd =
                fd.getMessageType().findFieldByName(PbConstant.PB_MAP_VALUE_NAME);

        PbCodegenAppender appender = new PbCodegenAppender();
        String pbKeyTypeStr = PbCodegenUtils.getTypeStrFromProto(keyFd, false);
        String pbValueTypeStr = PbCodegenUtils.getTypeStrFromProto(valueFd, false);
        String pbMapVar = "pbMap" + uid;
        String pbMapEntryVar = "pbEntry" + uid;
        String resultDataMapVar = "resultDataMap" + uid;
        String keyDataVar = "keyDataVar" + uid;
        String valueDataVar = "valueDataVar" + uid;

        appender.appendLine(
                "Map<"
                        + pbKeyTypeStr
                        + ","
                        + pbValueTypeStr
                        + "> "
                        + pbMapVar
                        + " = "
                        + pbGetStr
                        + ";");
        appender.appendLine("Map " + resultDataMapVar + " = new HashMap()");
        appender.appendSegment(
                "for(Map.Entry<"
                        + pbKeyTypeStr
                        + ","
                        + pbValueTypeStr
                        + "> "
                        + pbMapEntryVar
                        + ": "
                        + pbMapVar
                        + ".entrySet()){");
        appender.appendLine("Object " + keyDataVar + "= null");
        appender.appendLine("Object " + valueDataVar + "= null");
        PbCodegenDeserializer keyDes =
                PbCodegenDeserializeFactory.getPbCodegenDes(keyFd, keyType, formatConfig);
        PbCodegenDeserializer valueDes =
                PbCodegenDeserializeFactory.getPbCodegenDes(valueFd, valueType, formatConfig);
        String keyGenCode =
                keyDes.codegen(
                        keyDataVar, "((" + pbKeyTypeStr + ")" + pbMapEntryVar + ".getKey())");
        appender.appendSegment(keyGenCode);
        String valueGenCode =
                valueDes.codegen(
                        valueDataVar, "((" + pbValueTypeStr + ")" + pbMapEntryVar + ".getValue())");
        appender.appendSegment(valueGenCode);
        appender.appendLine(resultDataMapVar + ".put(" + keyDataVar + ", " + valueDataVar + ")");
        appender.appendSegment("}");
        appender.appendLine(
                returnInternalDataVarName + " = new GenericMapData(" + resultDataMapVar + ")");
        return appender.code();
    }
}
