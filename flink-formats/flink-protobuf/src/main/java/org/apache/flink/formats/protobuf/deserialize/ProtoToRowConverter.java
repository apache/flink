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
import org.apache.flink.formats.protobuf.PbConstant;
import org.apache.flink.formats.protobuf.PbFormatUtils;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.types.logical.RowType;

import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.FileDescriptor.Syntax;
import org.codehaus.janino.ScriptEvaluator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * {@link ProtoToRowConverter} can convert binary protobuf message data to flink row data by codegen
 * process.
 */
public class ProtoToRowConverter {
    private static final Logger LOG = LoggerFactory.getLogger(ProtoToRowConverter.class);
    private final ScriptEvaluator se;
    private final Method parseFromMethod;

    public ProtoToRowConverter(String messageClassName, RowType rowType, boolean readDefaultValues)
            throws PbCodegenException {
        try {
            Descriptors.Descriptor descriptor = PbFormatUtils.getDescriptor(messageClassName);
            Class<?> messageClass = Class.forName(messageClassName);
            if (descriptor.getFile().getSyntax() == Syntax.PROTO3) {
                readDefaultValues = true;
            }
            se = new ScriptEvaluator();
            se.setParameters(new String[] {"message"}, new Class[] {messageClass});
            se.setReturnType(RowData.class);
            se.setDefaultImports(
                    RowData.class.getName(),
                    ArrayData.class.getName(),
                    BinaryStringData.class.getName(),
                    GenericRowData.class.getName(),
                    GenericMapData.class.getName(),
                    GenericArrayData.class.getName(),
                    ArrayList.class.getName(),
                    List.class.getName(),
                    Map.class.getName(),
                    HashMap.class.getName());

            PbCodegenAppender codegenAppender = new PbCodegenAppender();
            codegenAppender.appendLine("RowData rowData=null");
            PbCodegenDeserializer codegenDes =
                    PbCodegenDeserializeFactory.getPbCodegenTopRowDes(
                            descriptor, rowType, readDefaultValues);
            String genCode = codegenDes.codegen("rowData", "message");
            codegenAppender.appendSegment(genCode);
            codegenAppender.appendLine("return rowData");

            String printCode = codegenAppender.printWithLineNumber();
            LOG.debug("Protobuf decode codegen: \n" + printCode);

            se.cook(codegenAppender.code());
            parseFromMethod = messageClass.getMethod(PbConstant.PB_METHOD_PARSE_FROM, byte[].class);
        } catch (Exception ex) {
            throw new PbCodegenException(ex);
        }
    }

    public RowData convertProtoBinaryToRow(byte[] data) throws Exception {
        Object messageObj = parseFromMethod.invoke(null, data);
        return (RowData) se.evaluate(new Object[] {messageObj});
    }
}
