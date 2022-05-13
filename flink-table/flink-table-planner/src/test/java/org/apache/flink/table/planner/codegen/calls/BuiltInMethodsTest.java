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

package org.apache.flink.table.planner.codegen.calls;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** Check if the static methods defined in {@link BuiltInMethods} are valid. */
class BuiltInMethodsTest {

    private static Stream<Method> testMethodsAreAvailable() {
        return Arrays.stream(BuiltInMethods.class.getMethods())
                .filter(
                        m ->
                                Modifier.isStatic(m.getModifiers())
                                        && Modifier.isPublic(m.getModifiers()));
    }

    @ParameterizedTest
    @MethodSource
    void testMethodsAreAvailable(Method m) throws Exception {
        assertThat(m.invoke(null)).isNotNull();
    }
}
