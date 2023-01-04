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
import org.apache.flink.formats.protobuf.PbConstant;
import org.apache.flink.formats.protobuf.PbFormatConfig;
import org.apache.flink.formats.protobuf.PbFormatContext;
import org.apache.flink.formats.protobuf.deserialize.ProtoToRowConverter;
import org.apache.flink.formats.protobuf.util.PbCodegenAppender;
import org.apache.flink.formats.protobuf.util.PbCodegenUtils;
import org.apache.flink.formats.protobuf.util.PbFormatUtils;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.RowType;

import com.google.protobuf.AbstractMessage;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * {@link RowToProtoConverter} can convert flink row data to binary protobuf message data by codegen
 * process.
 */
public class RowToProtoConverter {
    private static final Logger LOG = LoggerFactory.getLogger(ProtoToRowConverter.class);
    private final Method encodeMethod;

    public RowToProtoConverter(RowType rowType, PbFormatConfig formatConfig)
            throws PbCodegenException {
        try {
            Descriptors.Descriptor descriptor =
                    PbFormatUtils.getDescriptor(formatConfig.getMessageClassName());
            PbFormatContext formatContext = new PbFormatContext(formatConfig);

            PbCodegenAppender codegenAppender = new PbCodegenAppender(0);
            String uuid = UUID.randomUUID().toString().replaceAll("\\-", "");
            String generatedClassName = "GeneratedRowToProto_" + uuid;
            String generatedPackageName = RowToProtoConverter.class.getPackage().getName();
            codegenAppender.appendLine("package " + generatedPackageName);
            codegenAppender.appendLine("import " + AbstractMessage.class.getName());
            codegenAppender.appendLine("import " + Descriptors.class.getName());
            codegenAppender.appendLine("import " + RowData.class.getName());
            codegenAppender.appendLine("import " + ArrayData.class.getName());
            codegenAppender.appendLine("import " + StringData.class.getName());
            codegenAppender.appendLine("import " + ByteString.class.getName());
            codegenAppender.appendLine("import " + List.class.getName());
            codegenAppender.appendLine("import " + ArrayList.class.getName());
            codegenAppender.appendLine("import " + Map.class.getName());
            codegenAppender.appendLine("import " + HashMap.class.getName());

            codegenAppender.begin("public class " + generatedClassName + "{");
            codegenAppender.begin(
                    "public static AbstractMessage "
                            + PbConstant.GENERATED_ENCODE_METHOD
                            + "(RowData rowData){");
            codegenAppender.appendLine("AbstractMessage message = null");
            PbCodegenSerializer codegenSer =
                    PbCodegenSerializeFactory.getPbCodegenTopRowSer(
                            descriptor, rowType, formatContext);
            String genCode =
                    codegenSer.codegen("message", "rowData", codegenAppender.currentIndent());
            codegenAppender.appendSegment(genCode);
            codegenAppender.appendLine("return message");
            codegenAppender.end("}");
            codegenAppender.end("}");

            String printCode = codegenAppender.printWithLineNumber();
            LOG.debug("Protobuf encode codegen: \n" + printCode);
            Class generatedClass =
                    PbCodegenUtils.compileClass(
                            Thread.currentThread().getContextClassLoader(),
                            generatedPackageName + "." + generatedClassName,
                            codegenAppender.code());
            encodeMethod =
                    generatedClass.getMethod(PbConstant.GENERATED_ENCODE_METHOD, RowData.class);
        } catch (Exception ex) {
            throw new PbCodegenException(ex);
        }
    }

    public byte[] convertRowToProtoBinary(RowData rowData) throws Exception {
        AbstractMessage message = (AbstractMessage) encodeMethod.invoke(null, rowData);
        return message.toByteArray();
    }
}
