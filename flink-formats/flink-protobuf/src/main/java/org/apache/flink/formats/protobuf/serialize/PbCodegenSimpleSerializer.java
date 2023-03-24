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
import org.apache.flink.formats.protobuf.util.PbCodegenVarId;
import org.apache.flink.formats.protobuf.util.PbFormatUtils;
import org.apache.flink.table.types.logical.LogicalType;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;

/** Serializer to convert flink simple type data to proto simple type object. */
public class PbCodegenSimpleSerializer implements PbCodegenSerializer {
    private final Descriptors.FieldDescriptor fd;
    private final LogicalType type;
    private final PbFormatContext formatContext;

    public PbCodegenSimpleSerializer(
            Descriptors.FieldDescriptor fd, LogicalType type, PbFormatContext formatContext) {
        this.fd = fd;
        this.type = type;
        this.formatContext = formatContext;
    }

    @Override
    public String codegen(String resultVar, String flinkObjectCode, int indent)
            throws PbCodegenException {
        // the real value of flinkObjectCode may be String, Integer,
        // Long, Double, Float, Boolean, byte[].
        // The type of flinkObject is simple data type of flink, and flinkObject must not be null.
        // it should be converted to protobuf simple data as resultVariable.
        PbCodegenAppender appender = new PbCodegenAppender(indent);
        switch (type.getTypeRoot()) {
            case FLOAT:
            case DOUBLE:
            case BOOLEAN:
                appender.appendLine(resultVar + " = " + flinkObjectCode);
                return appender.code();
            case BIGINT:
            case INTEGER:
            case SMALLINT:
            case TINYINT:
                if (fd.getJavaType() == JavaType.ENUM) {
                    String enumTypeStr = PbFormatUtils.getFullJavaName(fd.getEnumType());
                    appender.appendLine(
                            resultVar
                                    + " = "
                                    + enumTypeStr
                                    + ".forNumber((int)"
                                    + flinkObjectCode
                                    + ")");
                    // choose the first enum element as default value if such value is invalid enum
                    appender.begin("if(null == " + resultVar + "){");
                    appender.appendLine(resultVar + " = " + enumTypeStr + ".values()[0]");
                    appender.end("}");
                } else {
                    appender.appendLine(resultVar + " = " + flinkObjectCode);
                }
                return appender.code();
            case VARCHAR:
            case CHAR:
                int uid = PbCodegenVarId.getInstance().getAndIncrement();
                String fromVar = "fromVar" + uid;
                appender.appendLine("String " + fromVar);
                appender.appendLine(fromVar + " = " + flinkObjectCode + ".toString()");
                if (fd.getJavaType() == JavaType.ENUM) {
                    String enumValueDescVar = "enumValueDesc" + uid;
                    String enumTypeStr = PbFormatUtils.getFullJavaName(fd.getEnumType());
                    appender.appendLine(
                            "Descriptors.EnumValueDescriptor "
                                    + enumValueDescVar
                                    + "="
                                    + enumTypeStr
                                    + ".getDescriptor().findValueByName("
                                    + fromVar
                                    + ")");
                    appender.begin("if(null == " + enumValueDescVar + "){");
                    // choose the first enum element as default value if such value is invalid enum
                    appender.appendLine(resultVar + " = " + enumTypeStr + ".values()[0]");
                    appender.end("}");
                    appender.begin("else{");
                    // choose the exact enum value
                    appender.appendLine(
                            resultVar + " = " + enumTypeStr + ".valueOf(" + enumValueDescVar + ")");
                    appender.end("}");
                } else {
                    appender.appendLine(resultVar + " = " + fromVar);
                }
                return appender.code();
            case VARBINARY:
            case BINARY:
                appender.appendLine(resultVar + " = ByteString.copyFrom(" + flinkObjectCode + ")");
                return appender.code();
            default:
                throw new PbCodegenException("Unsupported data type in schema: " + type);
        }
    }
}
