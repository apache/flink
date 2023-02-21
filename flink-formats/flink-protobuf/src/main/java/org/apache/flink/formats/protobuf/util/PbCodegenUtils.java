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

package org.apache.flink.formats.protobuf.util;

import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.formats.protobuf.PbCodegenException;
import org.apache.flink.formats.protobuf.PbConstant;
import org.apache.flink.formats.protobuf.PbFormatContext;
import org.apache.flink.formats.protobuf.serialize.PbCodegenSerializeFactory;
import org.apache.flink.formats.protobuf.serialize.PbCodegenSerializer;
import org.apache.flink.table.types.logical.LogicalType;

import com.google.protobuf.Descriptors.FieldDescriptor;
import org.codehaus.janino.SimpleCompiler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Codegen utils only used in protobuf format. */
public class PbCodegenUtils {
    private static final Logger LOG = LoggerFactory.getLogger(PbCodegenUtils.class);

    /**
     * @param flinkContainerCode code phrase which represent flink container type like row/array in
     *     codegen sections
     * @param index the index number in flink container type
     * @param eleType the element type
     */
    public static String flinkContainerElementCode(
            String flinkContainerCode, String index, LogicalType eleType) {
        switch (eleType.getTypeRoot()) {
            case INTEGER:
                return flinkContainerCode + ".getInt(" + index + ")";
            case BIGINT:
                return flinkContainerCode + ".getLong(" + index + ")";
            case FLOAT:
                return flinkContainerCode + ".getFloat(" + index + ")";
            case DOUBLE:
                return flinkContainerCode + ".getDouble(" + index + ")";
            case BOOLEAN:
                return flinkContainerCode + ".getBoolean(" + index + ")";
            case VARCHAR:
            case CHAR:
                return flinkContainerCode + ".getString(" + index + ")";
            case VARBINARY:
            case BINARY:
                return flinkContainerCode + ".getBinary(" + index + ")";
            case ROW:
                int size = eleType.getChildren().size();
                return flinkContainerCode + ".getRow(" + index + ", " + size + ")";
            case MAP:
                return flinkContainerCode + ".getMap(" + index + ")";
            case ARRAY:
                return flinkContainerCode + ".getArray(" + index + ")";
            default:
                throw new IllegalArgumentException("Unsupported data type in schema: " + eleType);
        }
    }

    /**
     * Get java type str from {@link FieldDescriptor} which directly fetched from protobuf object.
     *
     * @return The returned code phrase will be used as java type str in codegen sections.
     * @throws PbCodegenException
     */
    public static String getTypeStrFromProto(FieldDescriptor fd, boolean isList)
            throws PbCodegenException {
        String typeStr;
        switch (fd.getJavaType()) {
            case MESSAGE:
                if (fd.isMapField()) {
                    // map
                    FieldDescriptor keyFd =
                            fd.getMessageType().findFieldByName(PbConstant.PB_MAP_KEY_NAME);
                    FieldDescriptor valueFd =
                            fd.getMessageType().findFieldByName(PbConstant.PB_MAP_VALUE_NAME);
                    // key and value cannot be repeated
                    String keyTypeStr = getTypeStrFromProto(keyFd, false);
                    String valueTypeStr = getTypeStrFromProto(valueFd, false);
                    typeStr = "Map<" + keyTypeStr + "," + valueTypeStr + ">";
                } else {
                    // simple message
                    typeStr = PbFormatUtils.getFullJavaName(fd.getMessageType());
                }
                break;
            case INT:
                typeStr = "Integer";
                break;
            case LONG:
                typeStr = "Long";
                break;
            case STRING:
                typeStr = "String";
                break;
            case ENUM:
                typeStr = PbFormatUtils.getFullJavaName(fd.getEnumType());
                break;
            case FLOAT:
                typeStr = "Float";
                break;
            case DOUBLE:
                typeStr = "Double";
                break;
            case BYTE_STRING:
                typeStr = "ByteString";
                break;
            case BOOLEAN:
                typeStr = "Boolean";
                break;
            default:
                throw new PbCodegenException("do not support field type: " + fd.getJavaType());
        }
        if (isList) {
            return "List<" + typeStr + ">";
        } else {
            return typeStr;
        }
    }

    /**
     * Get java type str from {@link LogicalType} which directly fetched from flink type.
     *
     * @return The returned code phrase will be used as java type str in codegen sections.
     */
    public static String getTypeStrFromLogicType(LogicalType type) {
        switch (type.getTypeRoot()) {
            case INTEGER:
                return "int";
            case BIGINT:
                return "long";
            case FLOAT:
                return "float";
            case DOUBLE:
                return "double";
            case BOOLEAN:
                return "boolean";
            case VARCHAR:
            case CHAR:
                return "StringData";
            case VARBINARY:
            case BINARY:
                return "byte[]";
            case ROW:
                return "RowData";
            case MAP:
                return "MapData";
            case ARRAY:
                return "ArrayData";
            default:
                throw new IllegalArgumentException("Unsupported data type in schema: " + type);
        }
    }

    /**
     * Get protobuf default value from {@link FieldDescriptor}.
     *
     * @return The java code phrase which represents default value calculation.
     */
    public static String pbDefaultValueCode(
            FieldDescriptor fieldDescriptor, PbFormatContext pbFormatContext)
            throws PbCodegenException {
        String nullLiteral = pbFormatContext.getPbFormatConfig().getWriteNullStringLiterals();
        switch (fieldDescriptor.getJavaType()) {
            case MESSAGE:
                return PbFormatUtils.getFullJavaName(fieldDescriptor.getMessageType())
                        + ".getDefaultInstance()";
            case INT:
                return "0";
            case LONG:
                return "0L";
            case STRING:
                return "\"" + nullLiteral + "\"";
            case ENUM:
                return PbFormatUtils.getFullJavaName(fieldDescriptor.getEnumType())
                        + ".values()[0]";
            case FLOAT:
                return "0.0f";
            case DOUBLE:
                return "0.0d";
            case BYTE_STRING:
                return "ByteString.EMPTY";
            case BOOLEAN:
                return "false";
            default:
                throw new PbCodegenException(
                        "do not support field type: " + fieldDescriptor.getJavaType());
        }
    }

    /**
     * This method will be called from serializer of flink array/map type because flink contains
     * both array/map type in array format. Map/Array cannot contain null value in pb object then we
     * must do conversion in case of null values in map/array type.
     *
     * @param flinkArrDataVar code phrase represent arrayData of arr type or keyData/valueData in
     *     map type.
     * @param iVar the index in arrDataVar
     * @param resultPbVar the returned pb variable name in codegen.
     * @param elementPbFd {@link FieldDescriptor} of element type in proto object
     * @param elementDataType {@link LogicalType} of element type in flink object
     * @return The java code segment which represents field value retrieval.
     */
    public static String convertFlinkArrayElementToPbWithDefaultValueCode(
            String flinkArrDataVar,
            String iVar,
            String resultPbVar,
            FieldDescriptor elementPbFd,
            LogicalType elementDataType,
            PbFormatContext pbFormatContext,
            int indent)
            throws PbCodegenException {
        PbCodegenVarId varUid = PbCodegenVarId.getInstance();
        int uid = varUid.getAndIncrement();
        String flinkElementVar = "elementVar" + uid;
        PbCodegenAppender appender = new PbCodegenAppender(indent);
        String protoTypeStr = PbCodegenUtils.getTypeStrFromProto(elementPbFd, false);
        String dataTypeStr = PbCodegenUtils.getTypeStrFromLogicType(elementDataType);
        appender.appendLine(protoTypeStr + " " + resultPbVar);
        appender.begin("if(" + flinkArrDataVar + ".isNullAt(" + iVar + ")){");
        appender.appendLine(
                resultPbVar
                        + "="
                        + PbCodegenUtils.pbDefaultValueCode(elementPbFd, pbFormatContext));
        appender.end("}else{");
        appender.begin();
        appender.appendLine(dataTypeStr + " " + flinkElementVar);
        String flinkContainerElementCode =
                PbCodegenUtils.flinkContainerElementCode(flinkArrDataVar, iVar, elementDataType);
        appender.appendLine(flinkElementVar + " = " + flinkContainerElementCode);
        PbCodegenSerializer codegenSer =
                PbCodegenSerializeFactory.getPbCodegenSer(
                        elementPbFd, elementDataType, pbFormatContext);
        String code = codegenSer.codegen(resultPbVar, flinkElementVar, appender.currentIndent());
        appender.appendSegment(code);
        appender.end("}");
        return appender.code();
    }

    public static Class compileClass(ClassLoader classloader, String className, String code)
            throws ClassNotFoundException {
        SimpleCompiler simpleCompiler = new SimpleCompiler();
        simpleCompiler.setParentClassLoader(classloader);
        try {
            simpleCompiler.cook(code);
        } catch (Throwable t) {
            LOG.error("Protobuf codegen compile error: \n" + code);
            throw new InvalidProgramException(
                    "Program cannot be compiled. This is a bug. Please file an issue.", t);
        }
        return simpleCompiler.getClassLoader().loadClass(className);
    }
}
