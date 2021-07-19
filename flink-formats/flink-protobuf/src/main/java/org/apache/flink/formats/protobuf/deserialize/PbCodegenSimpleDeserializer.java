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

import com.google.protobuf.Descriptors;

/** Deserializer to convert proto simple type object to flink simple type data. */
public class PbCodegenSimpleDeserializer implements PbCodegenDeserializer {
    private final Descriptors.FieldDescriptor fd;

    public PbCodegenSimpleDeserializer(Descriptors.FieldDescriptor fd) {
        this.fd = fd;
    }

    /** {@code pbGetStr} is primitive type and must not be null. */
    @Override
    public String codegen(String returnInternalDataVarName, String pbGetStr) {
        // the type of messageGetStr must not be primitive type,
        // it should convert to internal flink row type like StringData.
        StringBuilder sb = new StringBuilder();
        switch (fd.getJavaType()) {
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case BOOLEAN:
                sb.append(returnInternalDataVarName + " = " + pbGetStr + ";");
                break;
            case BYTE_STRING:
                sb.append(returnInternalDataVarName + " = " + pbGetStr + ".toByteArray();");
                break;
            case STRING:
            case ENUM:
                sb.append(
                        returnInternalDataVarName
                                + " = BinaryStringData.fromString("
                                + pbGetStr
                                + ".toString());");
                break;
        }
        return sb.toString();
    }
}
