/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.util.serialization;

import org.apache.flink.annotation.PublicEvolving;

import java.nio.charset.Charset;

/**
 * Very simple serialization schema for strings.
 *
 * <p>By default, the serializer uses "UTF-8" for string/byte conversion.
 *
 * @deprecated Use {@link org.apache.flink.api.common.serialization.SimpleStringSchema} instead.
 */
@PublicEvolving
@Deprecated
@SuppressWarnings("deprecation")
public class SimpleStringSchema extends org.apache.flink.api.common.serialization.SimpleStringSchema
        implements SerializationSchema<String>, DeserializationSchema<String> {

    private static final long serialVersionUID = 1L;

    public SimpleStringSchema() {
        super();
    }

    /**
     * Creates a new SimpleStringSchema that uses the given charset to convert between strings and
     * bytes.
     *
     * @param charset The charset to use to convert between strings and bytes.
     */
    public SimpleStringSchema(Charset charset) {
        super(charset);
    }
}
