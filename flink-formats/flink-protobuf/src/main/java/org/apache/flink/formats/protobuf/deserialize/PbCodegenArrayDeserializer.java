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
import org.apache.flink.formats.protobuf.PbFormatContext;
import org.apache.flink.formats.protobuf.util.PbCodegenAppender;
import org.apache.flink.formats.protobuf.util.PbCodegenUtils;
import org.apache.flink.formats.protobuf.util.PbCodegenVarId;
import org.apache.flink.table.types.logical.LogicalType;

import com.google.protobuf.Descriptors;

/** Deserializer to convert proto array type object to flink array type data. */
public class PbCodegenArrayDeserializer implements PbCodegenDeserializer {
    private final Descriptors.FieldDescriptor fd;
    private final LogicalType elementType;
    private final PbFormatContext formatContext;

    public PbCodegenArrayDeserializer(
            Descriptors.FieldDescriptor fd,
            LogicalType elementType,
            PbFormatContext formatContext) {
        this.fd = fd;
        this.elementType = elementType;
        this.formatContext = formatContext;
    }

    @Override
    public String codegen(String resultVar, String pbObjectCode, int indent)
            throws PbCodegenException {
        // The type of pbObjectCode represents a general List object,
        // it should be converted to ArrayData of flink internal type as resultVariable.
        PbCodegenAppender appender = new PbCodegenAppender(indent);
        PbCodegenVarId varUid = PbCodegenVarId.getInstance();
        int uid = varUid.getAndIncrement();
        String protoTypeStr = PbCodegenUtils.getTypeStrFromProto(fd, false);
        String listPbVar = "list" + uid;
        String flinkArrVar = "newArr" + uid;
        String flinkArrEleVar = "subReturnVar" + uid;
        String iVar = "i" + uid;
        String subPbObjVar = "subObj" + uid;

        appender.appendLine("List<" + protoTypeStr + "> " + listPbVar + "=" + pbObjectCode);
        appender.appendLine(
                "Object[] " + flinkArrVar + "= new " + "Object[" + listPbVar + ".size()]");
        appender.begin(
                "for(int " + iVar + "=0;" + iVar + " < " + listPbVar + ".size(); " + iVar + "++){");
        appender.appendLine("Object " + flinkArrEleVar + " = null");
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
                PbCodegenDeserializeFactory.getPbCodegenDes(fd, elementType, formatContext);
        String code = codegenDes.codegen(flinkArrEleVar, subPbObjVar, appender.currentIndent());
        appender.appendSegment(code);
        appender.appendLine(flinkArrVar + "[" + iVar + "]=" + flinkArrEleVar + "");
        appender.end("}");
        appender.appendLine(resultVar + " = new GenericArrayData(" + flinkArrVar + ")");
        return appender.code();
    }
}
