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

package org.apache.flink.formats.avro;

import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.core.fs.FSDataOutputStream;

import org.apache.avro.file.DataFileWriter;

import java.io.IOException;
import java.io.OutputStream;

/**
 * A factory that creates an {@link AvroBulkWriter}. The factory takes a user-supplied builder to
 * assemble Parquet's writer and then turns it into a Flink {@code BulkWriter}.
 *
 * @param <T> The type of record to write.
 */
public class AvroWriterFactory<T> implements BulkWriter.Factory<T> {
    private static final long serialVersionUID = 1L;

    /** The builder to construct the Avro {@link DataFileWriter}. */
    private final AvroBuilder<T> avroBuilder;

    /** Creates a new AvroWriterFactory using the given builder to assemble the ParquetWriter. */
    public AvroWriterFactory(AvroBuilder<T> avroBuilder) {
        this.avroBuilder = avroBuilder;
    }

    @Override
    public BulkWriter<T> create(FSDataOutputStream out) throws IOException {
        return new AvroBulkWriter<>(avroBuilder.createWriter(new CloseShieldOutputStream(out)));
    }

    /** Proxy output stream that prevents the underlying output stream from being closed. */
    private static class CloseShieldOutputStream extends OutputStream {
        private final OutputStream out;

        public CloseShieldOutputStream(OutputStream out) {
            this.out = out;
        }

        @Override
        public void write(int b) throws IOException {
            out.write(b);
        }

        @Override
        public void write(byte[] buffer) throws IOException {
            out.write(buffer);
        }

        @Override
        public void write(byte[] buffer, int off, int len) throws IOException {
            out.write(buffer, off, len);
        }

        @Override
        public void flush() throws IOException {
            out.flush();
        }

        @Override
        public void close() throws IOException {
            // we do not actually close the internal stream here to prevent that the finishing
            // of the Avro Writer closes the target output stream.
        }
    }
}
