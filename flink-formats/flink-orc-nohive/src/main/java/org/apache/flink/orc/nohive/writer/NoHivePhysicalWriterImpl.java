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

package org.apache.flink.orc.nohive.writer;

import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.orc.writer.PhysicalWriterImpl;

import com.google.protobuf25.CodedOutputStream;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcProto;

import java.io.IOException;

/**
 * Protobuf is relocated in orc-core-nohive, therefore method calls involving PB classes need to use
 * the relocated class names here.
 */
public class NoHivePhysicalWriterImpl extends PhysicalWriterImpl {

    // relocated PB class in orc-core-nohive
    private final CodedOutputStream noHiveProtobufWriter;

    public NoHivePhysicalWriterImpl(FSDataOutputStream out, OrcFile.WriterOptions opts)
            throws IOException {
        super(out, opts);
        noHiveProtobufWriter = CodedOutputStream.newInstance(writer);
    }

    @Override
    protected void writeMetadata(OrcProto.Metadata metadata) throws IOException {
        metadata.writeTo(noHiveProtobufWriter);
        noHiveProtobufWriter.flush();
        writer.flush();
    }

    @Override
    protected void writeFileFooter(OrcProto.Footer footer) throws IOException {
        footer.writeTo(noHiveProtobufWriter);
        noHiveProtobufWriter.flush();
        writer.flush();
    }

    @Override
    protected void writeStripeFooter(OrcProto.StripeFooter footer) throws IOException {
        footer.writeTo(noHiveProtobufWriter);
        noHiveProtobufWriter.flush();
        writer.flush();
    }
}
