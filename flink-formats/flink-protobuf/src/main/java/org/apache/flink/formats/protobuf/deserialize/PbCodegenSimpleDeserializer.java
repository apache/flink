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
import org.apache.flink.formats.protobuf.util.PbCodegenAppender;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import com.google.protobuf.Descriptors;

/** Deserializer to convert proto simple type object to flink simple type data. */
public class PbCodegenSimpleDeserializer implements PbCodegenDeserializer {
    private final Descriptors.FieldDescriptor fd;
    private final LogicalType logicalType;

    public PbCodegenSimpleDeserializer(Descriptors.FieldDescriptor fd, LogicalType logicalType) {
        this.fd = fd;
        this.logicalType = logicalType;
    }

    @Override
    public String codegen(String resultVar, String pbObjectCode, int indent)
            throws PbCodegenException {
        // the type of pbObjectCode must not be primitive type,
        // it should convert to internal flink row type like StringData.
        PbCodegenAppender appender = new PbCodegenAppender(indent);
        switch (fd.getJavaType()) {
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case BOOLEAN:
                appender.appendLine(resultVar + " = " + pbObjectCode);
                break;
            case BYTE_STRING:
                appender.appendLine(resultVar + " = " + pbObjectCode + ".toByteArray()");
                break;
            case STRING:
                appender.appendLine(
                        resultVar
                                + " = BinaryStringData.fromString("
                                + pbObjectCode
                                + ".toString())");
                break;
            case ENUM:
                if (logicalType.getTypeRoot() == LogicalTypeRoot.CHAR
                        || logicalType.getTypeRoot() == LogicalTypeRoot.VARCHAR) {
                    appender.appendLine(
                            resultVar
                                    + " = BinaryStringData.fromString("
                                    + pbObjectCode
                                    + ".toString())");
                } else if (logicalType.getTypeRoot() == LogicalTypeRoot.TINYINT
                        || logicalType.getTypeRoot() == LogicalTypeRoot.SMALLINT
                        || logicalType.getTypeRoot() == LogicalTypeRoot.INTEGER
                        || logicalType.getTypeRoot() == LogicalTypeRoot.BIGINT) {
                    appender.appendLine(resultVar + " = " + pbObjectCode + ".getNumber()");
                } else {
                    throw new PbCodegenException(
                            "Illegal type for protobuf enum, only char/vachar/int/bigint is supported");
                }
                break;
            default:
                throw new PbCodegenException(
                        "Unsupported protobuf simple type: " + fd.getJavaType());
        }
        return appender.code();
    }
}
