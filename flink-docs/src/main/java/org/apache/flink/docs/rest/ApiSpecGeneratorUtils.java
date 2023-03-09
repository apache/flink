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

package org.apache.flink.docs.rest;

import org.apache.flink.annotation.docs.Documentation;
import org.apache.flink.annotation.docs.FlinkJsonSchema;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.MessageParameters;
import org.apache.flink.runtime.rest.messages.RequestBody;
import org.apache.flink.runtime.rest.messages.ResponseBody;

import java.util.Optional;

/** Helper methods for generation API documentation. */
public class ApiSpecGeneratorUtils {

    private ApiSpecGeneratorUtils() {}

    /**
     * Checks whether the given endpoint should be documented.
     *
     * @param spec endpoint to check
     * @return true if the endpoint should be documented
     */
    public static boolean shouldBeDocumented(
            MessageHeaders<
                            ? extends RequestBody,
                            ? extends ResponseBody,
                            ? extends MessageParameters>
                    spec) {
        return spec.getClass().getAnnotation(Documentation.ExcludeFromDocumentation.class) == null;
    }

    /**
     * Find whether the class contains dynamic fields that need to be documented.
     *
     * @param clazz class to check
     * @return optional that is non-empty if the class is annotated with {@link
     *     FlinkJsonSchema.AdditionalFields}
     */
    public static Optional<Class<?>> findAdditionalFieldType(Class<?> clazz) {
        final FlinkJsonSchema.AdditionalFields annotation =
                clazz.getAnnotation(FlinkJsonSchema.AdditionalFields.class);
        return Optional.ofNullable(annotation).map(FlinkJsonSchema.AdditionalFields::type);
    }
}
