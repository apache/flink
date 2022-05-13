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

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecordBase;

import java.io.IOException;
import java.io.OutputStream;
import java.util.function.Function;

/**
 * Convenience builder to create {@link AvroWriterFactory} instances for the different Avro types.
 */
public class AvroWriters {

    /**
     * Creates an {@link AvroWriterFactory} for an Avro specific type. The Avro writers will use the
     * schema of that specific type to build and write the records.
     *
     * @param type The class of the type to write.
     */
    public static <T extends SpecificRecordBase> AvroWriterFactory<T> forSpecificRecord(
            Class<T> type) {
        String schemaString = SpecificData.get().getSchema(type).toString();
        AvroBuilder<T> builder =
                (out) -> createAvroDataFileWriter(schemaString, SpecificDatumWriter::new, out);
        return new AvroWriterFactory<>(builder);
    }

    /**
     * Creates an {@link AvroWriterFactory} that accepts and writes Avro generic types. The Avro
     * writers will use the given schema to build and write the records.
     *
     * @param schema The schema of the generic type.
     */
    public static AvroWriterFactory<GenericRecord> forGenericRecord(Schema schema) {
        String schemaString = schema.toString();
        AvroBuilder<GenericRecord> builder =
                (out) -> createAvroDataFileWriter(schemaString, GenericDatumWriter::new, out);
        return new AvroWriterFactory<>(builder);
    }

    /**
     * Creates an {@link AvroWriterFactory} for the given type. The Avro writers will use reflection
     * to create the schema for the type and use that schema to write the records.
     *
     * @param type The class of the type to write.
     */
    public static <T> AvroWriterFactory<T> forReflectRecord(Class<T> type) {
        String schemaString = ReflectData.get().getSchema(type).toString();
        AvroBuilder<T> builder =
                (out) -> createAvroDataFileWriter(schemaString, ReflectDatumWriter::new, out);
        return new AvroWriterFactory<>(builder);
    }

    private static <T> DataFileWriter<T> createAvroDataFileWriter(
            String schemaString,
            Function<Schema, DatumWriter<T>> datumWriterFactory,
            OutputStream out)
            throws IOException {

        Schema schema = new Schema.Parser().parse(schemaString);
        DatumWriter<T> datumWriter = datumWriterFactory.apply(schema);

        DataFileWriter<T> dataFileWriter = new DataFileWriter<>(datumWriter);
        dataFileWriter.create(schema, out);
        return dataFileWriter;
    }

    // ------------------------------------------------------------------------

    /** Class is not meant to be instantiated. */
    private AvroWriters() {}
}
