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

package org.apache.flink.formats.protobuf.serialize;

import org.apache.flink.formats.protobuf.PbCodegenAppender;
import org.apache.flink.formats.protobuf.PbCodegenException;
import org.apache.flink.formats.protobuf.PbCodegenUtils;
import org.apache.flink.formats.protobuf.PbCodegenVarId;
import org.apache.flink.formats.protobuf.PbConstant;
import org.apache.flink.formats.protobuf.PbFormatConfig;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;

import com.google.protobuf.Descriptors;

/** Serializer to convert flink map type data to proto map type object. */
public class PbCodegenMapSerializer implements PbCodegenSerializer {
    private final Descriptors.FieldDescriptor fd;
    private final MapType mapType;
    private final PbFormatConfig formatConfig;

    public PbCodegenMapSerializer(
            Descriptors.FieldDescriptor fd, MapType mapType, PbFormatConfig formatConfig) {
        this.fd = fd;
        this.mapType = mapType;
        this.formatConfig = formatConfig;
    }

    @Override
    public String codegen(String returnPbVarName, String internalDataGetStr)
            throws PbCodegenException {
        PbCodegenVarId varUid = PbCodegenVarId.getInstance();
        int uid = varUid.getAndIncrement();
        LogicalType keyType = mapType.getKeyType();
        LogicalType valueType = mapType.getValueType();
        Descriptors.FieldDescriptor keyFd =
                fd.getMessageType().findFieldByName(PbConstant.PB_MAP_KEY_NAME);
        Descriptors.FieldDescriptor valueFd =
                fd.getMessageType().findFieldByName(PbConstant.PB_MAP_VALUE_NAME);

        PbCodegenAppender appender = new PbCodegenAppender();
        String keyProtoTypeStr = PbCodegenUtils.getTypeStrFromProto(keyFd, false);
        String valueProtoTypeStr = PbCodegenUtils.getTypeStrFromProto(valueFd, false);

        String keyArrDataVar = "keyArrData" + uid;
        String valueArrDataVar = "valueArrData" + uid;
        String iVar = "i" + uid;
        String pbMapVar = "resultPbMap" + uid;
        String keyPbVar = "keyPbVar" + uid;
        String valuePbVar = "valuePbVar" + uid;
        String keyDataVar = "keyDataVar" + uid;
        String valueDataVar = "valueDataVar" + uid;

        appender.appendLine(
                "ArrayData " + keyArrDataVar + " = " + internalDataGetStr + ".keyArray()");
        appender.appendLine(
                "ArrayData " + valueArrDataVar + " = " + internalDataGetStr + ".valueArray()");

        appender.appendLine(
                "Map<"
                        + keyProtoTypeStr
                        + ", "
                        + valueProtoTypeStr
                        + "> "
                        + pbMapVar
                        + " = new HashMap()");
        appender.appendSegment(
                "for(int "
                        + iVar
                        + " = 0; "
                        + iVar
                        + " < "
                        + keyArrDataVar
                        + ".size(); "
                        + iVar
                        + "++){");

        // process key
        String keyGenCode =
                PbCodegenUtils.generateArrElementCodeWithDefaultValue(
                        keyArrDataVar, iVar, keyPbVar, keyDataVar, keyFd, keyType, formatConfig);
        appender.appendSegment(keyGenCode);

        // process value
        String valueGenCode =
                PbCodegenUtils.generateArrElementCodeWithDefaultValue(
                        valueArrDataVar,
                        iVar,
                        valuePbVar,
                        valueDataVar,
                        valueFd,
                        valueType,
                        formatConfig);
        appender.appendSegment(valueGenCode);

        appender.appendLine(pbMapVar + ".put(" + keyPbVar + ", " + valuePbVar + ")");
        appender.appendSegment("}");

        appender.appendLine(returnPbVarName + " = " + pbMapVar);
        return appender.code();
    }
}
