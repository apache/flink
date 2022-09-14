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
import org.apache.flink.api.common.typeinfo.TypeInformation;

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

/**
 * The schema factory for a specified {@link SchemaType}. We add this factory because of pulsar
 * don't provide a serializable schema and we can't create it directly from {@link SchemaInfo}. So
 * we have to implement this creation logic.
 */
@Internal
public interface PulsarSchemaFactory<T> {

    /** The supported schema type for this factory. We would classify the factory by the type. */
    SchemaType type();

    /** Create the schema by the given info. */
    Schema<T> createSchema(SchemaInfo info);

    /** Create the flink type information by the given schema info. */
    TypeInformation<T> createTypeInfo(SchemaInfo info);
}
