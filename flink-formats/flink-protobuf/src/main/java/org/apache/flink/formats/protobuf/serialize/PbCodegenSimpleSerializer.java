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
import org.apache.flink.formats.protobuf.PbCodegenVarId;
import org.apache.flink.formats.protobuf.PbFormatConfig;
import org.apache.flink.formats.protobuf.PbFormatUtils;
import org.apache.flink.table.types.logical.LogicalType;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;

/** Serializer to convert flink simple type data to proto simple type object. */
public class PbCodegenSimpleSerializer implements PbCodegenSerializer {
    private final Descriptors.FieldDescriptor fd;
    private final LogicalType type;
    private final PbFormatConfig formatConfig;

    public PbCodegenSimpleSerializer(
            Descriptors.FieldDescriptor fd, LogicalType type, PbFormatConfig formatConfig) {
        this.fd = fd;
        this.type = type;
        this.formatConfig = formatConfig;
    }

    /**
     * @param internalDataGetStr the real value of {@code internalDataGetStr} may be String, int,
     *     long, double, float, boolean, byte[], enum value {@code internalDataGetStr} must not be
     *     null.
     */
    @Override
    public String codegen(String returnPbVarName, String internalDataGetStr) {
        switch (type.getTypeRoot()) {
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case BOOLEAN:
                return returnPbVarName + " = " + internalDataGetStr + ";";
            case VARCHAR:
            case CHAR:
                PbCodegenAppender appender = new PbCodegenAppender();
                int uid = PbCodegenVarId.getInstance().getAndIncrement();
                String fromVar = "fromVar" + uid;
                appender.appendLine("String " + fromVar);
                appender.appendLine(fromVar + " = " + internalDataGetStr + ".toString()");
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
                    appender.appendSegment("if(null == " + enumValueDescVar + "){");
                    // choose the first enum element as default value if such value is invalid enum
                    appender.appendLine(returnPbVarName + " = " + enumTypeStr + ".values()[0]");
                    appender.appendSegment("}");
                    appender.appendSegment("else{");
                    // choose the exact enum value
                    appender.appendLine(
                            returnPbVarName
                                    + " = "
                                    + enumTypeStr
                                    + ".valueOf("
                                    + enumValueDescVar
                                    + ")");
                    appender.appendLine("}");
                } else {
                    appender.appendLine(returnPbVarName + " = " + fromVar);
                }
                return appender.code();
            case VARBINARY:
            case BINARY:
                return returnPbVarName + " = ByteString.copyFrom(" + internalDataGetStr + ");";
            default:
                throw new IllegalArgumentException("Unsupported data type in schema: " + type);
        }
    }
}
