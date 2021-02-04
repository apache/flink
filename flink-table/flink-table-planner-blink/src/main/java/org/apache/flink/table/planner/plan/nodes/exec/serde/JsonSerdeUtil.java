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

package org.apache.flink.table.planner.plan.nodes.exec.serde;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.MapperFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.deser.BeanDeserializerFactory;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.deser.DefaultDeserializationContext;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;

/** An utility class that provide abilities for JSON serialization and deserialization. */
public class JsonSerdeUtil {

    /** Return true if the given class's constructors have @JsonCreator annotation, else false. */
    public static boolean hasJsonCreatorAnnotation(Class<?> clazz) {
        for (Constructor<?> constructor : clazz.getDeclaredConstructors()) {
            for (Annotation annotation : constructor.getAnnotations()) {
                if (annotation instanceof JsonCreator) {
                    return true;
                }
            }
        }
        return false;
    }

    /** Create an {@link ObjectMapper} which DeserializationContext wraps a {@link SerdeContext}. */
    public static ObjectMapper createObjectMapper(SerdeContext serdeCtx) {
        FlinkDeserializationContext ctx =
                new FlinkDeserializationContext(
                        new DefaultDeserializationContext.Impl(BeanDeserializerFactory.instance),
                        serdeCtx);
        ObjectMapper mapper =
                new ObjectMapper(
                        null, // JsonFactory
                        null, // DefaultSerializerProvider
                        ctx);
        mapper.configure(MapperFeature.USE_GETTERS_AS_SETTERS, false);
        ctx.setObjectMapper(mapper);
        return mapper;
    }

    private JsonSerdeUtil() {}
}
