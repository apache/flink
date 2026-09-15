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
import org.apache.flink.formats.protobuf.util.PbCodegenAppender;
import org.apache.flink.formats.protobuf.util.PbCodegenVarId;
import org.apache.flink.formats.protobuf.util.PbFormatUtils;

import com.google.protobuf.Descriptors.FieldDescriptor;

/**
 * Serializer for a recursive message field, which {@link
 * org.apache.flink.formats.protobuf.util.PbToRowTypeUtil} represents as BYTES through its cycle
 * detection. It is the inverse of {@link
 * org.apache.flink.formats.protobuf.deserialize.PbCodegenBytesDeserializer}: the deserializer emits
 * the sub-message as {@code message.toByteArray()}, and here we parse those bytes back into the
 * message so the enclosing {@link PbCodegenRowSerializer} can set it on the builder.
 */
public class PbCodegenBytesSerializer implements PbCodegenSerializer {
    private final FieldDescriptor fd;

    public PbCodegenBytesSerializer(FieldDescriptor fd) {
        this.fd = fd;
    }

    @Override
    public String codegen(String resultVar, String flinkObjectCode, int indent)
            throws PbCodegenException {
        // flinkObjectCode is a non-null flink BYTES value holding a serialized protobuf message.
        PbCodegenAppender appender = new PbCodegenAppender(indent);
        int uid = PbCodegenVarId.getInstance().getAndIncrement();
        String bytesVar = "recursiveBytes" + uid;
        String messageTypeStr = PbFormatUtils.getFullJavaName(fd.getMessageType());

        appender.appendLine("byte[] " + bytesVar + " = " + flinkObjectCode);
        codegenRequiredFieldGuard(appender, bytesVar, messageTypeStr);
        appender.begin("try{");
        appender.appendLine(resultVar + " = " + messageTypeStr + ".parseFrom(" + bytesVar + ")");
        appender.end("}");
        appender.begin("catch(com.google.protobuf.InvalidProtocolBufferException e){");
        appender.appendLine(
                "throw new RuntimeException("
                        + "\"failed to parse recursive protobuf bytes for field '"
                        + fd.getName()
                        + "'\", e)");
        appender.end("}");
        return appender.code();
    }

    // A recursive message type with required fields cannot round-trip an absent field read with
    // read-default-values=true: it is materialized on read as byte[0], which parseFrom cannot
    // rebuild. getDefaultInstance().isInitialized() is false only for such types, so this guard is
    // a no-op for proto3 and for proto2 without required fields.
    private void codegenRequiredFieldGuard(
            PbCodegenAppender appender, String bytesVar, String messageTypeStr) {
        appender.begin(
                "if("
                        + bytesVar
                        + ".length == 0 && !"
                        + messageTypeStr
                        + ".getDefaultInstance().isInitialized()){");
        appender.appendLine(
                "throw new RuntimeException("
                        + "\"cannot serialize field '"
                        + fd.getName()
                        + "': recursive proto2 message has required fields and was read with "
                        + "read-default-values=true; set read-default-values=false, or ensure the "
                        + "field is present\")");
        appender.end("}");
    }
}
