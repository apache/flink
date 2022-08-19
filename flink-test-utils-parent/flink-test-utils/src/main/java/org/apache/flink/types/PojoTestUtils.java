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

package org.apache.flink.types;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.java.typeutils.runtime.PojoSerializer;

import static org.assertj.core.api.Assertions.assertThat;

/** Test utils around POJOs. */
@PublicEvolving
public class PojoTestUtils {
    /**
     * Verifies that instances of the given class fulfill all conditions to be serialized with the
     * {@link PojoSerializer}, as documented <a
     * href="https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/fault-tolerance/serialization/types_serialization/#pojos">here</a>.
     *
     * <p>Note that this check will succeed even if the Pojo is partially serialized with Kryo. If
     * this is not desired, use {@link #assertSerializedAsPojoWithoutKryo(Class)} instead.
     *
     * @param clazz class to analyze
     * @param <T> class type
     * @throws AssertionError if instances of the class cannot be serialized as a POJO
     */
    public static <T> void assertSerializedAsPojo(Class<T> clazz) throws AssertionError {
        final TypeInformation<T> typeInformation = TypeInformation.of(clazz);
        final TypeSerializer<T> actualSerializer =
                typeInformation.createSerializer(new ExecutionConfig());

        assertThat(actualSerializer)
                .withFailMessage(
                        "Instances of the class '%s' cannot be serialized as a POJO, but would use a '%s' instead. %n"
                                + "Re-run this test with INFO logging enabled and check messages from the '%s' for possible reasons.",
                        clazz.getSimpleName(),
                        actualSerializer.getClass().getSimpleName(),
                        TypeExtractor.class.getCanonicalName())
                .isInstanceOf(PojoSerializer.class);
    }

    /**
     * Verifies that instances of the given class fulfill all conditions to be serialized with the
     * {@link PojoSerializer}, as documented <a
     * href="https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/fault-tolerance/serialization/types_serialization/#pojos">here</a>,
     * without any field being serialized with Kryo.
     *
     * @param clazz class to analyze
     * @param <T> class type
     * @throws AssertionError if instances of the class cannot be serialized as a POJO or required
     *     Kryo for one or more fields
     */
    public static <T> void assertSerializedAsPojoWithoutKryo(Class<T> clazz) throws AssertionError {
        final ExecutionConfig executionConfig = new ExecutionConfig();
        executionConfig.disableGenericTypes();

        final TypeInformation<T> typeInformation = TypeInformation.of(clazz);
        final TypeSerializer<T> actualSerializer;
        try {
            actualSerializer = typeInformation.createSerializer(executionConfig);
        } catch (UnsupportedOperationException e) {
            throw new AssertionError(e);
        }

        assertThat(actualSerializer)
                .withFailMessage(
                        "Instances of the class '%s' cannot be serialized as a POJO, but would use a '%s' instead. %n"
                                + "Re-run this test with INFO logging enabled and check messages from the '%s' for possible reasons.",
                        clazz.getSimpleName(),
                        actualSerializer.getClass().getSimpleName(),
                        TypeExtractor.class.getCanonicalName())
                .isInstanceOf(PojoSerializer.class);
    }
}
