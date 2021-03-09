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

package org.apache.flink.formats.parquet.protobuf;

import org.apache.flink.formats.parquet.ParquetBuilder;
import org.apache.flink.formats.parquet.ParquetWriterFactory;

import com.google.protobuf.Message;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.proto.ProtoWriteSupport;

/** Convenience builder for creating {@link ParquetWriterFactory} instances for Protobuf classes. */
public class ParquetProtoWriters {

    /**
     * Creates a {@link ParquetWriterFactory} for the given type. The type should represent a
     * Protobuf message.
     *
     * @param type The class of the type to write.
     */
    public static <T extends Message> ParquetWriterFactory<T> forType(Class<T> type) {
        ParquetBuilder<T> builder = (out) -> new ParquetProtoWriterBuilder<>(out, type).build();
        return new ParquetWriterFactory<>(builder);
    }

    // ------------------------------------------------------------------------

    /** The builder for Protobuf {@link ParquetWriter}. */
    private static class ParquetProtoWriterBuilder<T extends Message>
            extends ParquetWriter.Builder<T, ParquetProtoWriterBuilder<T>> {

        private final Class<T> clazz;

        protected ParquetProtoWriterBuilder(OutputFile outputFile, Class<T> clazz) {
            super(outputFile);
            this.clazz = clazz;
        }

        @Override
        protected ParquetProtoWriterBuilder<T> self() {
            return this;
        }

        @Override
        protected WriteSupport<T> getWriteSupport(Configuration conf) {
            return new ProtoWriteSupport<>(clazz);
        }
    }

    /** Class is not meant to be instantiated. */
    private ParquetProtoWriters() {}
}
