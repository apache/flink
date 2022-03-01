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

import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.connector.file.src.reader.StreamFormat;
import org.apache.flink.formats.avro.typeutils.AvroTypeInfo;
import org.apache.flink.formats.avro.typeutils.GenericRecordAvroTypeInfo;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecordBase;

/**
 * A convenience builder to create {@link AvroParquetRecordFormat} instances for the different kinds
 * of Avro record types.
 */
@Experimental
public class AvroParquetReaders {

    /**
     * Creates a new {@link AvroParquetRecordFormat} that reads the parquet file into Avro {@link
     * org.apache.avro.specific.SpecificRecord SpecificRecords}.
     *
     * <p>To read into Avro {@link GenericRecord GenericRecords}, use the {@link
     * #forGenericRecord(Schema)} method.
     *
     * @see #forGenericRecord(Schema)
     */
    public static <T extends SpecificRecordBase> StreamFormat<T> forSpecificRecord(
            final Class<T> typeClass) {
        return new AvroParquetRecordFormat<>(
                new AvroTypeInfo<>(typeClass), () -> SpecificData.get());
    }

    /**
     * Creates a new {@link AvroParquetRecordFormat} that reads the parquet file into Avro records
     * via reflection.
     *
     * <p>To read into Avro {@link GenericRecord GenericRecords}, use the {@link
     * #forGenericRecord(Schema)} method.
     *
     * <p>To read into Avro {@link org.apache.avro.specific.SpecificRecord SpecificRecords}, use the
     * {@link #forSpecificRecord(Class)} method.
     *
     * @see #forGenericRecord(Schema)
     * @see #forSpecificRecord(Class)
     */
    public static <T> StreamFormat<T> forReflectRecord(final Class<T> typeClass) {
        if (SpecificRecordBase.class.isAssignableFrom(typeClass)) {
            throw new IllegalArgumentException(
                    "Please use AvroParquetReaders.forSpecificRecord(Class<T>) for SpecificRecord.");
        } else if (GenericRecord.class.isAssignableFrom(typeClass)) {
            throw new IllegalArgumentException(
                    "Please use AvroParquetReaders.forGenericRecord(Class<T>) for GenericRecord."
                            + "Cannot read and create Avro GenericRecord without specifying the Avro Schema. "
                            + "This is because Flink needs to be able serialize the results in its data flow, which is"
                            + "very inefficient without the schema. And while the Schema is stored in the Avro file header,"
                            + "Flink needs this schema during 'pre-flight' time when the data flow is set up and wired,"
                            + "which is before there is access to the files");
        }

        // this is a PoJo that Avo will reader via reflect de-serialization
        // for Flink, this is just a plain PoJo type
        return new AvroParquetRecordFormat<>(
                TypeExtractor.createTypeInfo(typeClass), () -> ReflectData.get());
    }

    /**
     * Creates a new {@link AvroParquetRecordFormat} that reads the parquet file into Avro {@link
     * GenericRecord GenericRecords}.
     *
     * <p>To read into {@link GenericRecord GenericRecords}, this method needs an Avro Schema. That
     * is because Flink needs to be able to serialize the results in its data flow, which is very
     * inefficient without the schema. And while the Schema is stored in the Avro file header, Flink
     * needs this schema during 'pre-flight' time when the data flow is set up and wired, which is
     * before there is access to the files.
     */
    public static StreamFormat<GenericRecord> forGenericRecord(final Schema schema) {
        return new AvroParquetRecordFormat<>(
                new GenericRecordAvroTypeInfo(schema), () -> GenericData.get());
    }

    // ------------------------------------------------------------------------

    /** Class is not meant to be instantiated. */
    private AvroParquetReaders() {}
}
