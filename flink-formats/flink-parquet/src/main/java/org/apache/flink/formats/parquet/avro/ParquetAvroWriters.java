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

package org.apache.flink.formats.parquet.avro;

import org.apache.flink.formats.parquet.ParquetWriterFactory;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificRecordBase;

/**
 * Convenience builder to create {@link ParquetWriterFactory} instances for the different Avro
 * types.
 *
 * @deprecated use {@link AvroParquetWriters} instead.
 */
@Deprecated
public class ParquetAvroWriters {

    /**
     * Creates a ParquetWriterFactory for an Avro specific type. The Parquet writers will use the
     * schema of that specific type to build and write the columnar data.
     *
     * @param type The class of the type to write.
     */
    public static <T extends SpecificRecordBase> ParquetWriterFactory<T> forSpecificRecord(
            Class<T> type) {
        return AvroParquetWriters.forSpecificRecord(type);
    }

    /**
     * Creates a ParquetWriterFactory that accepts and writes Avro generic types. The Parquet
     * writers will use the given schema to build and write the columnar data.
     *
     * @param schema The schema of the generic type.
     */
    public static ParquetWriterFactory<GenericRecord> forGenericRecord(Schema schema) {
        return AvroParquetWriters.forGenericRecord(schema);
    }

    /**
     * Creates a ParquetWriterFactory for the given type. The Parquet writers will use Avro to
     * reflectively create a schema for the type and use that schema to write the columnar data.
     *
     * @param type The class of the type to write.
     */
    public static <T> ParquetWriterFactory<T> forReflectRecord(Class<T> type) {
        return AvroParquetWriters.forReflectRecord(type);
    }

    // ------------------------------------------------------------------------

    /** Class is not meant to be instantiated. */
    private ParquetAvroWriters() {}
}
