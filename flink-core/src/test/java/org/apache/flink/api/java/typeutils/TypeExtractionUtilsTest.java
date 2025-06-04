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

package org.apache.flink.api.java.typeutils;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.api.java.typeutils.TypeExtractionUtils.getAllDeclaredMethods;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link TypeExtractionUtils}. */
@SuppressWarnings("rawtypes")
public class TypeExtractionUtilsTest {

    @Test
    void testIsGeneric() throws Exception {
        Method method = getMethod(IsGeneric.class, "m1");
        Type firstParam = method.getGenericParameterTypes()[0];
        assertThat(TypeExtractionUtils.isGenericOfClass(List.class, firstParam)).isTrue();

        method = getMethod(IsGeneric.class, "m2");
        firstParam = method.getGenericParameterTypes()[0];
        assertThat(TypeExtractionUtils.isGenericOfClass(List.class, firstParam)).isTrue();
    }

    @Test
    void testGetParameterizedType() throws Exception {
        Method method = getMethod(IsGeneric.class, "m1");
        Type firstParam = method.getGenericParameterTypes()[0];
        Optional<ParameterizedType> parameterizedType =
                TypeExtractionUtils.getParameterizedType(firstParam);
        assertThat(parameterizedType).isPresent();
        assertThat(parameterizedType.get().getRawType()).isEqualTo(List.class);
        assertThat(parameterizedType.get().getActualTypeArguments()[0]).isEqualTo(Integer.class);

        method = getMethod(IsGeneric.class, "m2");
        firstParam = method.getGenericParameterTypes()[0];
        assertThat(TypeExtractionUtils.getParameterizedType(firstParam)).isEmpty();
    }

    private Method getMethod(Class<?> clazz, String name) throws Exception {
        return getAllDeclaredMethods(clazz).stream()
                .filter(m -> m.getName().equals(name))
                .findFirst()
                .orElseThrow();
    }

    public static class IsGeneric {
        public void m1(List<Integer> list) {}

        public void m2(List list) {}
    }
}
