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

package org.apache.flink.testutils;

import org.junit.jupiter.api.Test;

import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

/** Tests for the {@link ClassLoaderUtils}. */
class ClassLoaderUtilsTest {

    @Test
    void testObjectFromNewClassLoaderObject() {
        testObjectFromNewClassLoaderObject(
                ClassLoaderUtils::createSerializableObjectFromNewClassLoader);
    }

    @Test
    void testObjectFromNewClassLoaderClassLoaders() {
        testObjectFromNewClassLoaderClassLoaders(
                ClassLoaderUtils::createSerializableObjectFromNewClassLoader);
    }

    @Test
    void testExceptionObjectFromNewClassLoaderObject() {
        testObjectFromNewClassLoaderObject(
                ClassLoaderUtils::createExceptionObjectFromNewClassLoader);
    }

    @Test
    void testExceptionObjectFromNewClassLoaderClassLoaders() {
        testObjectFromNewClassLoaderClassLoaders(
                ClassLoaderUtils::createExceptionObjectFromNewClassLoader);
    }

    private static <X> void testObjectFromNewClassLoaderObject(
            Supplier<ClassLoaderUtils.ObjectAndClassLoader<X>> supplier) {
        final ClassLoaderUtils.ObjectAndClassLoader<X> objectAndClassLoader = supplier.get();
        final Object o = objectAndClassLoader.getObject();

        assertThat(ClassLoader.getSystemClassLoader()).isNotEqualTo(o.getClass().getClassLoader());

        try {
            Class.forName(o.getClass().getName());
            fail("should not be able to load class from the system class loader");
        } catch (ClassNotFoundException ignored) {
        }
    }

    private static <X> void testObjectFromNewClassLoaderClassLoaders(
            Supplier<ClassLoaderUtils.ObjectAndClassLoader<X>> supplier) {
        final ClassLoaderUtils.ObjectAndClassLoader<X> objectAndClassLoader = supplier.get();

        assertThat(ClassLoader.getSystemClassLoader())
                .isNotEqualTo(objectAndClassLoader.getClassLoader())
                .isEqualTo(objectAndClassLoader.getClassLoader().getParent());
    }
}
