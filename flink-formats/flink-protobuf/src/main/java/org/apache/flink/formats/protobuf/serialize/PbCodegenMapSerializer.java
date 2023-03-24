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

import org.apache.flink.formats.protobuf.PbCodegenException;
import org.apache.flink.formats.protobuf.PbConstant;
import org.apache.flink.formats.protobuf.PbFormatContext;
import org.apache.flink.formats.protobuf.util.PbCodegenAppender;
import org.apache.flink.formats.protobuf.util.PbCodegenUtils;
import org.apache.flink.formats.protobuf.util.PbCodegenVarId;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;

import com.google.protobuf.Descriptors;

/** Serializer to convert flink map type data to proto map type object. */
public class PbCodegenMapSerializer implements PbCodegenSerializer {
    private final Descriptors.FieldDescriptor fd;
    private final MapType mapType;
    private final PbFormatContext formatContext;

    public PbCodegenMapSerializer(
            Descriptors.FieldDescriptor fd, MapType mapType, PbFormatContext formatContext) {
        this.fd = fd;
        this.mapType = mapType;
        this.formatContext = formatContext;
    }

    @Override
    public String codegen(String resultVar, String flinkObjectCode, int indent)
            throws PbCodegenException {
        // The type of flinkObjectCode is a MapData of flink,
        // it should be converted to map of protobuf as resultVariable.
        PbCodegenVarId varUid = PbCodegenVarId.getInstance();
        int uid = varUid.getAndIncrement();
        LogicalType keyType = mapType.getKeyType();
        LogicalType valueType = mapType.getValueType();
        Descriptors.FieldDescriptor keyFd =
                fd.getMessageType().findFieldByName(PbConstant.PB_MAP_KEY_NAME);
        Descriptors.FieldDescriptor valueFd =
                fd.getMessageType().findFieldByName(PbConstant.PB_MAP_VALUE_NAME);

        PbCodegenAppender appender = new PbCodegenAppender(indent);
        String keyProtoTypeStr = PbCodegenUtils.getTypeStrFromProto(keyFd, false);
        String valueProtoTypeStr = PbCodegenUtils.getTypeStrFromProto(valueFd, false);

        String flinkKeyArrDataVar = "keyArrData" + uid;
        String flinkValueArrDataVar = "valueArrData" + uid;
        String iVar = "i" + uid;
        String pbMapVar = "resultPbMap" + uid;
        String keyPbVar = "keyPbVar" + uid;
        String valuePbVar = "valuePbVar" + uid;

        appender.appendLine(
                "ArrayData " + flinkKeyArrDataVar + " = " + flinkObjectCode + ".keyArray()");
        appender.appendLine(
                "ArrayData " + flinkValueArrDataVar + " = " + flinkObjectCode + ".valueArray()");

        appender.appendLine(
                "Map<"
                        + keyProtoTypeStr
                        + ", "
                        + valueProtoTypeStr
                        + "> "
                        + pbMapVar
                        + " = new HashMap()");
        appender.begin(
                "for(int "
                        + iVar
                        + " = 0; "
                        + iVar
                        + " < "
                        + flinkKeyArrDataVar
                        + ".size(); "
                        + iVar
                        + "++){");

        // process key
        String convertFlinkKeyArrayElementToPbCode =
                PbCodegenUtils.convertFlinkArrayElementToPbWithDefaultValueCode(
                        flinkKeyArrDataVar,
                        iVar,
                        keyPbVar,
                        keyFd,
                        keyType,
                        formatContext,
                        appender.currentIndent());
        appender.appendSegment(convertFlinkKeyArrayElementToPbCode);

        // process value
        String convertFlinkValueArrayElementToPbCode =
                PbCodegenUtils.convertFlinkArrayElementToPbWithDefaultValueCode(
                        flinkValueArrDataVar,
                        iVar,
                        valuePbVar,
                        valueFd,
                        valueType,
                        formatContext,
                        appender.currentIndent());
        appender.appendSegment(convertFlinkValueArrayElementToPbCode);

        appender.appendLine(pbMapVar + ".put(" + keyPbVar + ", " + valuePbVar + ")");
        appender.end("}");

        appender.appendLine(resultVar + " = " + pbMapVar);
        return appender.code();
    }
}
