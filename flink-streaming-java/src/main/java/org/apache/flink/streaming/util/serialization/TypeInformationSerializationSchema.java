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

package org.apache.flink.streaming.util.serialization;

import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;

/**
 * A serialization and deserialization schema that uses Flink's serialization stack to transform
 * typed from and to byte arrays.
 *
 * @param <T> The type to be serialized.
 * @deprecated Use {@link
 *     org.apache.flink.api.common.serialization.TypeInformationSerializationSchema} instead.
 */
@Public
@Deprecated
@SuppressWarnings("deprecation")
public class TypeInformationSerializationSchema<T>
        extends org.apache.flink.api.common.serialization.TypeInformationSerializationSchema<T>
        implements DeserializationSchema<T>, SerializationSchema<T> {

    private static final long serialVersionUID = -5359448468131559102L;

    /**
     * Creates a new de-/serialization schema for the given type.
     *
     * @param typeInfo The type information for the type de-/serialized by this schema.
     * @param ec The execution config, which is used to parametrize the type serializers.
     */
    public TypeInformationSerializationSchema(TypeInformation<T> typeInfo, ExecutionConfig ec) {
        super(typeInfo, ec);
    }
}
