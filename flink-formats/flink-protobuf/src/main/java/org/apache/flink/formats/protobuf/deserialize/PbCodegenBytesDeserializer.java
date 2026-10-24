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

import com.google.protobuf.Descriptors.FieldDescriptor;

/**
 * Deserializer that converts a protobuf message to its raw binary bytes. Used when a recursive
 * message type is detected and represented as BYTES in the Flink schema, preserving the data for
 * optional later unpacking via a UDF.
 */
public class PbCodegenBytesDeserializer implements PbCodegenDeserializer {
    private final FieldDescriptor fd;

    public PbCodegenBytesDeserializer(FieldDescriptor fd) {
        this.fd = fd;
    }

    @Override
    public String codegen(String resultVar, String pbObjectCode, int indent)
            throws PbCodegenException {
        PbCodegenAppender appender = new PbCodegenAppender(indent);
        appender.appendLine(resultVar + " = " + pbObjectCode + ".toByteArray()");
        return appender.code();
    }
}
