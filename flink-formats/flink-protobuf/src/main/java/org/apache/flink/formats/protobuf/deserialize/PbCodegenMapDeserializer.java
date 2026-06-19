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

import org.apache.flink.formats.protobuf.PbCodegenException;
import org.apache.flink.formats.protobuf.PbConstant;
import org.apache.flink.formats.protobuf.PbFormatContext;
import org.apache.flink.formats.protobuf.util.PbCodegenAppender;
import org.apache.flink.formats.protobuf.util.PbCodegenUtils;
import org.apache.flink.formats.protobuf.util.PbCodegenVarId;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;

import com.google.protobuf.Descriptors;

/** Deserializer to convert proto map type object to flink map type data. */
public class PbCodegenMapDeserializer implements PbCodegenDeserializer {
    private final Descriptors.FieldDescriptor fd;
    private final MapType mapType;
    private final PbFormatContext formatContext;

    public PbCodegenMapDeserializer(
            Descriptors.FieldDescriptor fd, MapType mapType, PbFormatContext formatContext) {
        this.fd = fd;
        this.mapType = mapType;
        this.formatContext = formatContext;
    }

    @Override
    public String codegen(String resultVar, String pbObjectCode, int indent)
            throws PbCodegenException {
        // The type of pbObjectCode is a general Map object,
        // it should be converted to MapData of flink internal type as resultVariable
        PbCodegenVarId varUid = PbCodegenVarId.getInstance();
        int uid = varUid.getAndIncrement();

        LogicalType keyType = mapType.getKeyType();
        LogicalType valueType = mapType.getValueType();
        Descriptors.FieldDescriptor keyFd =
                fd.getMessageType().findFieldByName(PbConstant.PB_MAP_KEY_NAME);
        Descriptors.FieldDescriptor valueFd =
                fd.getMessageType().findFieldByName(PbConstant.PB_MAP_VALUE_NAME);

        PbCodegenAppender appender = new PbCodegenAppender(indent);
        String pbKeyTypeStr = PbCodegenUtils.getTypeStrFromProto(keyFd, false);
        String pbValueTypeStr = PbCodegenUtils.getTypeStrFromProto(valueFd, false);
        String pbMapVar = "pbMap" + uid;
        String pbMapEntryVar = "pbEntry" + uid;
        String resultDataMapVar = "resultDataMap" + uid;
        String flinkKeyVar = "keyDataVar" + uid;
        String flinkValueVar = "valueDataVar" + uid;

        appender.appendLine(
                "Map<"
                        + pbKeyTypeStr
                        + ","
                        + pbValueTypeStr
                        + "> "
                        + pbMapVar
                        + " = "
                        + pbObjectCode
                        + ";");
        appender.appendLine("Map " + resultDataMapVar + " = new HashMap()");
        appender.begin(
                "for(Map.Entry<"
                        + pbKeyTypeStr
                        + ","
                        + pbValueTypeStr
                        + "> "
                        + pbMapEntryVar
                        + ": "
                        + pbMapVar
                        + ".entrySet()){");
        appender.appendLine("Object " + flinkKeyVar + "= null");
        appender.appendLine("Object " + flinkValueVar + "= null");
        PbCodegenDeserializer keyDes =
                PbCodegenDeserializeFactory.getPbCodegenDes(keyFd, keyType, formatContext);
        PbCodegenDeserializer valueDes =
                PbCodegenDeserializeFactory.getPbCodegenDes(valueFd, valueType, formatContext);
        String keyGenCode =
                keyDes.codegen(
                        flinkKeyVar,
                        "((" + pbKeyTypeStr + ")" + pbMapEntryVar + ".getKey())",
                        appender.currentIndent());
        appender.appendSegment(keyGenCode);
        String valueGenCode =
                valueDes.codegen(
                        flinkValueVar,
                        "((" + pbValueTypeStr + ")" + pbMapEntryVar + ".getValue())",
                        appender.currentIndent());
        appender.appendSegment(valueGenCode);
        appender.appendLine(resultDataMapVar + ".put(" + flinkKeyVar + ", " + flinkValueVar + ")");
        appender.end("}");
        appender.appendLine(resultVar + " = new GenericMapData(" + resultDataMapVar + ")");
        return appender.code();
    }
}
