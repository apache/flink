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

package org.apache.flink.formats.avro.typeutils;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** TypeInformation for {@link GenericRecord}. */
public class GenericRecordAvroTypeInfo extends TypeInformation<GenericRecord> {

    private static final long serialVersionUID = 4141977586453820650L;

    private transient Schema schema;

    public GenericRecordAvroTypeInfo(Schema schema) {
        this.schema = checkNotNull(schema);
    }

    @Override
    public boolean isBasicType() {
        return false;
    }

    @Override
    public boolean isTupleType() {
        return false;
    }

    @Override
    public int getArity() {
        return 1;
    }

    @Override
    public int getTotalFields() {
        return 1;
    }

    @Override
    public Class<GenericRecord> getTypeClass() {
        return GenericRecord.class;
    }

    @Override
    public boolean isKeyType() {
        return false;
    }

    @Override
    public TypeSerializer<GenericRecord> createSerializer(ExecutionConfig config) {
        return new AvroSerializer<>(GenericRecord.class, schema);
    }

    @Override
    public String toString() {
        return String.format("GenericRecord(\"%s\")", schema.toString());
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof GenericRecordAvroTypeInfo) {
            GenericRecordAvroTypeInfo avroTypeInfo = (GenericRecordAvroTypeInfo) obj;
            return Objects.equals(avroTypeInfo.schema, schema);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(schema);
    }

    @Override
    public boolean canEqual(Object obj) {
        return obj instanceof GenericRecordAvroTypeInfo;
    }

    private void writeObject(ObjectOutputStream oos) throws IOException {
        byte[] schemaStrInBytes = schema.toString(false).getBytes(StandardCharsets.UTF_8);
        oos.writeInt(schemaStrInBytes.length);
        oos.write(schemaStrInBytes);
    }

    private void readObject(ObjectInputStream ois) throws ClassNotFoundException, IOException {
        int len = ois.readInt();
        byte[] content = new byte[len];
        ois.readFully(content);
        this.schema = new Schema.Parser().parse(new String(content, StandardCharsets.UTF_8));
    }
}
