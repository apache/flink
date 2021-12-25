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

package org.apache.flink.connector.pulsar.common.schema;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;

import java.util.Objects;

/** Wrap the pulsar {@code Schema} into a flink {@code TypeInformation}. */
@Internal
public class PulsarSchemaTypeInformation<T> extends TypeInformation<T> {
    private static final long serialVersionUID = 7284667318651333519L;

    private final PulsarSchema<T> schema;

    public PulsarSchemaTypeInformation(PulsarSchema<T> schema) {
        this.schema = schema;
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
    public Class<T> getTypeClass() {
        return schema.getRecordClass();
    }

    @Override
    public boolean isKeyType() {
        return false;
    }

    @Override
    public TypeSerializer<T> createSerializer(ExecutionConfig config) {
        return new PulsarSchemaTypeSerializer<>(schema);
    }

    @Override
    public String toString() {
        return schema.toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof PulsarSchemaTypeInformation) {
            PulsarSchemaTypeInformation<?> that = (PulsarSchemaTypeInformation<?>) obj;
            return Objects.equals(schema, that.schema);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return schema.hashCode();
    }

    @Override
    public boolean canEqual(Object obj) {
        return obj instanceof PulsarSchemaTypeInformation;
    }
}
