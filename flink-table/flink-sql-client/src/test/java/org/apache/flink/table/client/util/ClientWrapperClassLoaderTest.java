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

package org.apache.flink.table.client.util;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.util.UserClassLoaderJarTestUtils;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.table.utils.UserDefinedFunctions.GENERATED_LOWER_UDF_CLASS;
import static org.apache.flink.table.utils.UserDefinedFunctions.GENERATED_LOWER_UDF_CODE;
import static org.apache.flink.table.utils.UserDefinedFunctions.GENERATED_UPPER_UDF_CLASS;
import static org.apache.flink.table.utils.UserDefinedFunctions.GENERATED_UPPER_UDF_CODE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for classloading and class loader utilities. */
public class ClientWrapperClassLoaderTest {

    private static File userJar;

    @BeforeAll
    public static void prepare(@TempDir File tempDir) throws Exception {
        Map<String, String> classNameCodes = new HashMap<>();
        classNameCodes.put(
                GENERATED_LOWER_UDF_CLASS,
                String.format(GENERATED_LOWER_UDF_CODE, GENERATED_LOWER_UDF_CLASS));
        classNameCodes.put(
                GENERATED_UPPER_UDF_CLASS,
                String.format(GENERATED_UPPER_UDF_CODE, GENERATED_UPPER_UDF_CLASS));
        userJar =
                UserClassLoaderJarTestUtils.createJarFile(
                        tempDir, "test-classloader.jar", classNameCodes);
    }

    @Test
    public void testClassLoadingByAddURL() throws Exception {
        Configuration configuration = new Configuration();
        final ClientWrapperClassLoader classLoader =
                new ClientWrapperClassLoader(
                        ClientClassloaderUtil.buildUserClassLoader(
                                Collections.emptyList(),
                                getClass().getClassLoader(),
                                configuration),
                        configuration);

        // test class loader before add jar url to ClassLoader
        assertClassNotFoundException(GENERATED_LOWER_UDF_CLASS, classLoader);

        // add jar url to ClassLoader
        classLoader.addURL(userJar.toURI().toURL());

        assertEquals(1, classLoader.getURLs().length);

        final Class<?> clazz1 = Class.forName(GENERATED_LOWER_UDF_CLASS, false, classLoader);
        final Class<?> clazz2 = Class.forName(GENERATED_LOWER_UDF_CLASS, false, classLoader);

        assertEquals(clazz1, clazz2);

        classLoader.close();
    }

    @Test
    public void testClassLoadingByRemoveURL() throws Exception {
        URL jarURL = userJar.toURI().toURL();
        Configuration configuration = new Configuration();
        final ClientWrapperClassLoader classLoader =
                new ClientWrapperClassLoader(
                        ClientClassloaderUtil.buildUserClassLoader(
                                Collections.singletonList(jarURL),
                                getClass().getClassLoader(),
                                configuration),
                        configuration);

        final Class<?> clazz1 = Class.forName(GENERATED_LOWER_UDF_CLASS, false, classLoader);
        final Class<?> clazz2 = Class.forName(GENERATED_LOWER_UDF_CLASS, false, classLoader);
        assertEquals(clazz1, clazz2);

        // remove jar url
        classLoader.removeURL(jarURL);

        assertEquals(0, classLoader.getURLs().length);

        // test class loader after remove jar url from ClassLoader
        assertClassNotFoundException(GENERATED_UPPER_UDF_CLASS, classLoader);

        // add jar url to ClassLoader again
        classLoader.addURL(jarURL);

        assertEquals(1, classLoader.getURLs().length);

        final Class<?> clazz3 = Class.forName(GENERATED_UPPER_UDF_CLASS, false, classLoader);
        final Class<?> clazz4 = Class.forName(GENERATED_UPPER_UDF_CLASS, false, classLoader);
        assertEquals(clazz3, clazz4);

        classLoader.close();
    }

    @Test
    public void testParallelCapable() {
        // It will be true only if all the super classes (except class Object) of the caller are
        // registered as parallel capable.
        assertTrue(TestClientWrapperClassLoader.IS_PARALLEL_CAPABLE);
    }

    private void assertClassNotFoundException(String className, ClassLoader classLoader) {
        CommonTestUtils.assertThrows(
                className,
                ClassNotFoundException.class,
                () -> Class.forName(className, false, classLoader));
    }

    private static class TestClientWrapperClassLoader extends ClientWrapperClassLoader {

        public static final boolean IS_PARALLEL_CAPABLE = ClassLoader.registerAsParallelCapable();

        TestClientWrapperClassLoader() {
            super(
                    ClientClassloaderUtil.buildUserClassLoader(
                            Collections.emptyList(),
                            TestClientWrapperClassLoader.class.getClassLoader(),
                            new Configuration()),
                    new Configuration());
        }
    }
}
