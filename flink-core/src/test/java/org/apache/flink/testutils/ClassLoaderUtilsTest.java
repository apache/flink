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

import org.junit.Test;

import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.fail;

/** Tests for the {@link ClassLoaderUtils}. */
public class ClassLoaderUtilsTest {

    @Test
    public void testObjectFromNewClassLoaderObject() throws Exception {
        testObjectFromNewClassLoaderObject(
                ClassLoaderUtils::createSerializableObjectFromNewClassLoader);
    }

    @Test
    public void testObjectFromNewClassLoaderClassLoaders() throws Exception {
        testObjectFromNewClassLoaderClassLoaders(
                ClassLoaderUtils::createSerializableObjectFromNewClassLoader);
    }

    @Test
    public void testExceptionObjectFromNewClassLoaderObject() throws Exception {
        testObjectFromNewClassLoaderObject(
                ClassLoaderUtils::createExceptionObjectFromNewClassLoader);
    }

    @Test
    public void testExceptionObjectFromNewClassLoaderClassLoaders() throws Exception {
        testObjectFromNewClassLoaderClassLoaders(
                ClassLoaderUtils::createExceptionObjectFromNewClassLoader);
    }

    private static <X> void testObjectFromNewClassLoaderObject(
            Supplier<ClassLoaderUtils.ObjectAndClassLoader<X>> supplier) {
        final ClassLoaderUtils.ObjectAndClassLoader<X> objectAndClassLoader = supplier.get();
        final Object o = objectAndClassLoader.getObject();

        assertNotEquals(ClassLoader.getSystemClassLoader(), o.getClass().getClassLoader());

        try {
            Class.forName(o.getClass().getName());
            fail("should not be able to load class from the system class loader");
        } catch (ClassNotFoundException ignored) {
        }
    }

    private static <X> void testObjectFromNewClassLoaderClassLoaders(
            Supplier<ClassLoaderUtils.ObjectAndClassLoader<X>> supplier) {
        final ClassLoaderUtils.ObjectAndClassLoader<X> objectAndClassLoader = supplier.get();

        assertNotEquals(ClassLoader.getSystemClassLoader(), objectAndClassLoader.getClassLoader());
        assertEquals(
                ClassLoader.getSystemClassLoader(),
                objectAndClassLoader.getClassLoader().getParent());
    }
}
