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
import org.apache.flink.formats.protobuf.PbFormatContext;
import org.apache.flink.formats.protobuf.util.PbCodegenAppender;
import org.apache.flink.formats.protobuf.util.PbCodegenUtils;
import org.apache.flink.formats.protobuf.util.PbCodegenVarId;
import org.apache.flink.formats.protobuf.util.PbFormatUtils;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

import com.google.protobuf.Descriptors;

/** Serializer to convert flink row type data to proto row type object. */
public class PbCodegenRowSerializer implements PbCodegenSerializer {
    private final Descriptors.Descriptor descriptor;
    private final RowType rowType;
    private final PbFormatContext formatContext;

    public PbCodegenRowSerializer(
            Descriptors.Descriptor descriptor, RowType rowType, PbFormatContext formatContext) {
        this.rowType = rowType;
        this.descriptor = descriptor;
        this.formatContext = formatContext;
    }

    @Override
    public String codegen(String resultVar, String flinkObjectCode, int indent)
            throws PbCodegenException {
        // The type of flinkObjectCode is a RowData of flink,
        // it should be converted to object of protobuf as resultVariable.
        PbCodegenVarId varUid = PbCodegenVarId.getInstance();
        int uid = varUid.getAndIncrement();
        PbCodegenAppender appender = new PbCodegenAppender(indent);
        String flinkRowDataVar = "rowData" + uid;
        String pbMessageTypeStr = PbFormatUtils.getFullJavaName(descriptor);
        String messageBuilderVar = "messageBuilder" + uid;
        appender.appendLine("RowData " + flinkRowDataVar + " = " + flinkObjectCode);
        appender.appendLine(
                pbMessageTypeStr
                        + ".Builder "
                        + messageBuilderVar
                        + " = "
                        + pbMessageTypeStr
                        + ".newBuilder()");
        int index = 0;
        for (String fieldName : rowType.getFieldNames()) {
            Descriptors.FieldDescriptor elementFd = descriptor.findFieldByName(fieldName);
            LogicalType subType = rowType.getTypeAt(rowType.getFieldIndex(fieldName));
            int subUid = varUid.getAndIncrement();
            String elementPbVar = "elementPbVar" + subUid;
            String elementPbTypeStr;
            if (elementFd.isMapField()) {
                elementPbTypeStr = PbCodegenUtils.getTypeStrFromProto(elementFd, false);
            } else {
                elementPbTypeStr =
                        PbCodegenUtils.getTypeStrFromProto(
                                elementFd, PbFormatUtils.isArrayType(subType));
            }
            String strongCamelFieldName = PbFormatUtils.getStrongCamelCaseJsonName(fieldName);

            // Only set non-null element of flink row to proto object. The real value in proto
            // result depends on protobuf implementation.
            appender.begin("if(!" + flinkRowDataVar + ".isNullAt(" + index + ")){");
            appender.appendLine(elementPbTypeStr + " " + elementPbVar);
            String flinkRowElementCode =
                    PbCodegenUtils.flinkContainerElementCode(flinkRowDataVar, index + "", subType);
            PbCodegenSerializer codegen =
                    PbCodegenSerializeFactory.getPbCodegenSer(elementFd, subType, formatContext);
            String code =
                    codegen.codegen(elementPbVar, flinkRowElementCode, appender.currentIndent());
            appender.appendSegment(code);
            if (subType.getTypeRoot() == LogicalTypeRoot.ARRAY) {
                appender.appendLine(
                        messageBuilderVar
                                + ".addAll"
                                + strongCamelFieldName
                                + "("
                                + elementPbVar
                                + ")");
            } else if (subType.getTypeRoot() == LogicalTypeRoot.MAP) {
                appender.appendLine(
                        messageBuilderVar
                                + ".putAll"
                                + strongCamelFieldName
                                + "("
                                + elementPbVar
                                + ")");
            } else {
                appender.appendLine(
                        messageBuilderVar
                                + ".set"
                                + strongCamelFieldName
                                + "("
                                + elementPbVar
                                + ")");
            }
            appender.end("}");
            index += 1;
        }
        appender.appendLine(resultVar + " = " + messageBuilderVar + ".build()");
        return appender.code();
    }
}
