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
import org.apache.flink.formats.protobuf.PbFormatConfig;
import org.apache.flink.table.types.logical.LogicalType;

import com.google.protobuf.Descriptors;

/** Deserializer to convert proto array type object to flink array type data. */
public class PbCodegenArrayDeserializer implements PbCodegenDeserializer {
    private final Descriptors.FieldDescriptor fd;
    private final LogicalType elementType;
    private final PbFormatConfig formatConfig;

    public PbCodegenArrayDeserializer(
            Descriptors.FieldDescriptor fd, LogicalType elementType, PbFormatConfig formatConfig) {
        this.fd = fd;
        this.elementType = elementType;
        this.formatConfig = formatConfig;
    }

    @Override
    public String codegen(String returnInternalDataVarName, String pbGetStr)
            throws PbCodegenException {
        // The type of messageGetStr is a native List object,
        // it should be converted to ArrayData of flink internal type.
        PbCodegenAppender appender = new PbCodegenAppender();
        PbCodegenVarId varUid = PbCodegenVarId.getInstance();
        int uid = varUid.getAndIncrement();
        String protoTypeStr = PbCodegenUtils.getTypeStrFromProto(fd, false);
        String listPbVar = "list" + uid;
        String newArrDataVar = "newArr" + uid;
        String subReturnDataVar = "subReturnVar" + uid;
        String iVar = "i" + uid;
        String subPbObjVar = "subObj" + uid;

        appender.appendLine("List<" + protoTypeStr + "> " + listPbVar + "=" + pbGetStr);
        appender.appendLine(
                "Object[] " + newArrDataVar + "= new " + "Object[" + listPbVar + ".size()]");
        appender.appendSegment(
                "for(int " + iVar + "=0;" + iVar + " < " + listPbVar + ".size(); " + iVar + "++){");
        appender.appendLine("Object " + subReturnDataVar + " = null");
        appender.appendLine(
                protoTypeStr
                        + " "
                        + subPbObjVar
                        + " = ("
                        + protoTypeStr
                        + ")"
                        + listPbVar
                        + ".get("
                        + iVar
                        + ")");
        PbCodegenDeserializer codegenDes =
                PbCodegenDeserializeFactory.getPbCodegenDes(fd, elementType, formatConfig);
        String code = codegenDes.codegen(subReturnDataVar, subPbObjVar);
        appender.appendSegment(code);
        appender.appendLine(newArrDataVar + "[" + iVar + "]=" + subReturnDataVar + "");
        appender.appendSegment("}");
        appender.appendLine(
                returnInternalDataVarName + " = new GenericArrayData(" + newArrDataVar + ")");
        return appender.code();
    }
}
