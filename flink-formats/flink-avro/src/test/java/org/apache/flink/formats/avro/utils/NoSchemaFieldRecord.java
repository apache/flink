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

package org.apache.flink.formats.avro.utils;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.specific.SpecificRecordBase;

/**
 * A {@link SpecificRecordBase} shaped like the classes avrohugger generates: the schema lives on
 * the Scala companion object, so the record class has no static {@code SCHEMA$} or {@code MODEL$}
 * field.
 */
public class NoSchemaFieldRecord extends SpecificRecordBase {

    private static final Schema SCHEMA =
            SchemaBuilder.record(NoSchemaFieldRecord.class.getSimpleName())
                    .namespace(NoSchemaFieldRecord.class.getPackage().getName())
                    .fields()
                    .requiredString("name")
                    .requiredInt("count")
                    .endRecord();

    private CharSequence name;
    private int count;

    public NoSchemaFieldRecord() {
        this("", 0);
    }

    public NoSchemaFieldRecord(CharSequence name, int count) {
        this.name = name;
        this.count = count;
    }

    @Override
    public Schema getSchema() {
        return SCHEMA;
    }

    @Override
    public Object get(int field) {
        switch (field) {
            case 0:
                return name;
            case 1:
                return count;
            default:
                throw new IndexOutOfBoundsException("Invalid field index: " + field);
        }
    }

    @Override
    public void put(int field, Object value) {
        switch (field) {
            case 0:
                name = (CharSequence) value;
                break;
            case 1:
                count = (Integer) value;
                break;
            default:
                throw new IndexOutOfBoundsException("Invalid field index: " + field);
        }
    }
}
