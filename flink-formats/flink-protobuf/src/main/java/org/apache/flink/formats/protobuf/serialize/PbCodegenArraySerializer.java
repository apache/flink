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
import org.apache.flink.formats.protobuf.PbFormatConfig;
import org.apache.flink.table.types.logical.LogicalType;

import com.google.protobuf.Descriptors;

/** Serializer to convert flink array type data to proto array type object. */
public class PbCodegenArraySerializer implements PbCodegenSerializer {
    private final Descriptors.FieldDescriptor fd;
    private final LogicalType elementType;
    private final PbFormatConfig formatConfig;

    public PbCodegenArraySerializer(
            Descriptors.FieldDescriptor fd, LogicalType elementType, PbFormatConfig formatConfig) {
        this.fd = fd;
        this.elementType = elementType;
        this.formatConfig = formatConfig;
    }

    @Override
    public String codegen(String returnPbVarName, String internalDataGetStr)
            throws PbCodegenException {
        PbCodegenVarId varUid = PbCodegenVarId.getInstance();
        int uid = varUid.getAndIncrement();
        PbCodegenAppender appender = new PbCodegenAppender();
        String protoTypeStr = PbCodegenUtils.getTypeStrFromProto(fd, false);
        String pbListVar = "pbList" + uid;
        String arrayDataVar = "arrData" + uid;
        String elementDataVar = "eleData" + uid;
        String elementPbVar = "elementPbVar" + uid;
        String iVar = "i" + uid;

        appender.appendLine("ArrayData " + arrayDataVar + " = " + internalDataGetStr);
        appender.appendLine("List<" + protoTypeStr + "> " + pbListVar + "= new ArrayList()");
        appender.appendSegment(
                "for(int "
                        + iVar
                        + "=0;"
                        + iVar
                        + " < "
                        + arrayDataVar
                        + ".size(); "
                        + iVar
                        + "++){");
        String elementGenCode =
                PbCodegenUtils.generateArrElementCodeWithDefaultValue(
                        arrayDataVar,
                        iVar,
                        elementPbVar,
                        elementDataVar,
                        fd,
                        elementType,
                        formatConfig);
        appender.appendSegment(elementGenCode);
        // add pb element to result list
        appender.appendLine(pbListVar + ".add( " + elementPbVar + ")");
        // end for
        appender.appendSegment("}");
        appender.appendLine(returnPbVarName + " = " + pbListVar);
        return appender.code();
    }
}
