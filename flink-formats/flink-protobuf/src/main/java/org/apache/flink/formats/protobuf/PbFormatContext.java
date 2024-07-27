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

package org.apache.flink.formats.protobuf;

import org.apache.flink.formats.protobuf.util.PbCodegenAppender;
import org.apache.flink.formats.protobuf.util.PbCodegenVarId;

import java.util.ArrayList;
import java.util.List;

/** store config and common information. */
public class PbFormatContext {
    private final PbFormatConfig pbFormatConfig;
    private final List<String> splitMethodStack = new ArrayList<>();
    private final boolean readDefaultValuesForPrimitiveTypes;

    public PbFormatContext(
            PbFormatConfig pbFormatConfig, boolean readDefaultValuesForPrimitiveTypes) {
        this.pbFormatConfig = pbFormatConfig;
        this.readDefaultValuesForPrimitiveTypes = readDefaultValuesForPrimitiveTypes;
    }

    private String createSplitMethod(
            String rowDataType,
            String rowDataVar,
            String messageTypeStr,
            String messageTypeVar,
            String code) {
        int uid = PbCodegenVarId.getInstance().getAndIncrement();
        String splitMethodName = "split" + uid;
        PbCodegenAppender pbCodegenAppender = new PbCodegenAppender();
        pbCodegenAppender.appendSegment(
                String.format(
                        "private static void %s (%s %s, %s %s) {\n %s \n}",
                        splitMethodName,
                        rowDataType,
                        rowDataVar,
                        messageTypeStr,
                        messageTypeVar,
                        code));
        splitMethodStack.add(pbCodegenAppender.code());
        return String.format("%s(%s, %s);", splitMethodName, rowDataVar, messageTypeVar);
    }

    public String splitDeserializerRowTypeMethod(
            String rowDataVar, String messageTypeStr, String messageTypeVar, String code) {
        return createSplitMethod(
                "GenericRowData", rowDataVar, messageTypeStr, messageTypeVar, code);
    }

    public String splitSerializerRowTypeMethod(
            String rowDataVar, String messageTypeStr, String messageTypeVar, String code) {
        return createSplitMethod("RowData", rowDataVar, messageTypeStr, messageTypeVar, code);
    }

    public List<String> getSplitMethodStack() {
        return splitMethodStack;
    }

    public PbFormatConfig getPbFormatConfig() {
        return pbFormatConfig;
    }

    public boolean getReadDefaultValuesForPrimitiveTypes() {
        return readDefaultValuesForPrimitiveTypes;
    }
}
