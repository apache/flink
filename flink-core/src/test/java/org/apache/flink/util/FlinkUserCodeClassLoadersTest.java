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

package org.apache.flink.util;

import org.apache.flink.testutils.junit.utils.TempDirUtils;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;

import static org.apache.flink.util.FlinkUserCodeClassLoader.NOOP_EXCEPTION_HANDLER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for classloading and class loader utilities. */
class FlinkUserCodeClassLoadersTest {

    @TempDir private static java.nio.file.Path tempFolder;

    public static final String USER_CLASS = "UserClass";
    public static final String USER_CLASS_CODE =
            "import java.io.Serializable;\n"
                    + "public class "
                    + USER_CLASS
                    + " implements Serializable {}";

    private static File userJar;

    @BeforeAll
    static void prepare() throws Exception {
        userJar =
                UserClassLoaderJarTestUtils.createJarFile(
                        TempDirUtils.newFolder(tempFolder, "test-jar"),
                        "test-classloader.jar",
                        USER_CLASS,
                        USER_CLASS_CODE);
    }

    @Test
    void testParentFirstClassLoading() throws Exception {
        final ClassLoader parentClassLoader = getClass().getClassLoader();

        // collect the libraries / class folders with RocksDB related code: the state backend and
        // RocksDB itself
        final URL childCodePath = getClass().getProtectionDomain().getCodeSource().getLocation();

        final URLClassLoader childClassLoader1 =
                createParentFirstClassLoader(childCodePath, parentClassLoader);

        final URLClassLoader childClassLoader2 =
                createParentFirstClassLoader(childCodePath, parentClassLoader);

        final String className = FlinkUserCodeClassLoadersTest.class.getName();

        final Class<?> clazz1 = Class.forName(className, false, parentClassLoader);
        final Class<?> clazz2 = Class.forName(className, false, childClassLoader1);
        final Class<?> clazz3 = Class.forName(className, false, childClassLoader2);

        assertThat(clazz2).isEqualTo(clazz1);
        assertThat(clazz3).isEqualTo(clazz1);

        childClassLoader1.close();
        childClassLoader2.close();
    }

    @Test
    void testChildFirstClassLoading() throws Exception {
        final ClassLoader parentClassLoader = getClass().getClassLoader();

        // collect the libraries / class folders with RocksDB related code: the state backend and
        // RocksDB itself
        final URL childCodePath = getClass().getProtectionDomain().getCodeSource().getLocation();

        final URLClassLoader childClassLoader1 =
                createChildFirstClassLoader(childCodePath, parentClassLoader);

        final URLClassLoader childClassLoader2 =
                createChildFirstClassLoader(childCodePath, parentClassLoader);

        final String className = FlinkUserCodeClassLoadersTest.class.getName();

        final Class<?> clazz1 = Class.forName(className, false, parentClassLoader);
        final Class<?> clazz2 = Class.forName(className, false, childClassLoader1);
        final Class<?> clazz3 = Class.forName(className, false, childClassLoader2);

        assertThat(clazz2).isNotEqualTo(clazz1);
        assertThat(clazz3).isNotEqualTo(clazz1);
        assertThat(clazz3).isNotEqualTo(clazz2);

        childClassLoader1.close();
        childClassLoader2.close();
    }

    @Test
    void testRepeatedChildFirstClassLoading() throws Exception {
        final ClassLoader parentClassLoader = getClass().getClassLoader();

        // collect the libraries / class folders with RocksDB related code: the state backend and
        // RocksDB itself
        final URL childCodePath = getClass().getProtectionDomain().getCodeSource().getLocation();

        final URLClassLoader childClassLoader =
                createChildFirstClassLoader(childCodePath, parentClassLoader);

        final String className = FlinkUserCodeClassLoadersTest.class.getName();

        final Class<?> clazz1 = Class.forName(className, false, parentClassLoader);
        final Class<?> clazz2 = Class.forName(className, false, childClassLoader);
        final Class<?> clazz3 = Class.forName(className, false, childClassLoader);
        final Class<?> clazz4 = Class.forName(className, false, childClassLoader);

        assertThat(clazz2).isNotEqualTo(clazz1);

        assertThat(clazz3).isEqualTo(clazz2);
        assertThat(clazz4).isEqualTo(clazz2);

        childClassLoader.close();
    }

    @Test
    void testRepeatedParentFirstPatternClass() throws Exception {
        final String className = FlinkUserCodeClassLoadersTest.class.getName();
        final String parentFirstPattern = className.substring(0, className.lastIndexOf('.'));

        final ClassLoader parentClassLoader = getClass().getClassLoader();

        // collect the libraries / class folders with RocksDB related code: the state backend and
        // RocksDB itself
        final URL childCodePath = getClass().getProtectionDomain().getCodeSource().getLocation();

        final URLClassLoader childClassLoader =
                FlinkUserCodeClassLoaders.childFirst(
                        new URL[] {childCodePath},
                        parentClassLoader,
                        new String[] {parentFirstPattern},
                        NOOP_EXCEPTION_HANDLER,
                        true);

        final Class<?> clazz1 = Class.forName(className, false, parentClassLoader);
        final Class<?> clazz2 = Class.forName(className, false, childClassLoader);
        final Class<?> clazz3 = Class.forName(className, false, childClassLoader);
        final Class<?> clazz4 = Class.forName(className, false, childClassLoader);

        assertThat(clazz2).isEqualTo(clazz1);
        assertThat(clazz3).isEqualTo(clazz1);
        assertThat(clazz4).isEqualTo(clazz1);

        childClassLoader.close();
    }

    @Test
    void testGetClassLoaderInfo() throws Exception {
        final ClassLoader parentClassLoader = getClass().getClassLoader();

        final URL childCodePath = getClass().getProtectionDomain().getCodeSource().getLocation();

        final URLClassLoader childClassLoader =
                createChildFirstClassLoader(childCodePath, parentClassLoader);

        String formattedURL = ClassLoaderUtil.formatURL(childCodePath);

        assertThat("URL ClassLoader:" + formattedURL)
                .isEqualTo(ClassLoaderUtil.getUserCodeClassLoaderInfo(childClassLoader));

        childClassLoader.close();
    }

    @Test
    void testGetClassLoaderInfoWithClassLoaderClosed() throws Exception {
        final ClassLoader parentClassLoader = getClass().getClassLoader();

        final URL childCodePath = getClass().getProtectionDomain().getCodeSource().getLocation();

        final URLClassLoader childClassLoader =
                createChildFirstClassLoader(childCodePath, parentClassLoader);

        childClassLoader.close();

        assertThat(ClassLoaderUtil.getUserCodeClassLoaderInfo(childClassLoader))
                .startsWith("Cannot access classloader info due to an exception.");
    }

    private static MutableURLClassLoader createParentFirstClassLoader(
            URL childCodePath, ClassLoader parentClassLoader) {
        return FlinkUserCodeClassLoaders.parentFirst(
                new URL[] {childCodePath}, parentClassLoader, NOOP_EXCEPTION_HANDLER, true);
    }

    private static MutableURLClassLoader createChildFirstClassLoader(
            URL childCodePath, ClassLoader parentClassLoader) {
        return FlinkUserCodeClassLoaders.childFirst(
                new URL[] {childCodePath},
                parentClassLoader,
                new String[0],
                NOOP_EXCEPTION_HANDLER,
                true);
    }

    @Test
    void testClosingOfClassloader() throws Exception {
        final String className = ClassToLoad.class.getName();

        final ClassLoader parentClassLoader = ClassLoader.getSystemClassLoader().getParent();

        final URL childCodePath = getClass().getProtectionDomain().getCodeSource().getLocation();

        final URLClassLoader childClassLoader =
                createChildFirstClassLoader(childCodePath, parentClassLoader);

        final Class<?> loadedClass = childClassLoader.loadClass(className);

        assertThat(loadedClass).isNotSameAs(ClassToLoad.class);

        childClassLoader.close();

        // after closing, no loaded class should be reachable anymore
        assertThatThrownBy(() -> childClassLoader.loadClass(className))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testParallelCapable() {
        // It will be true only if all the super classes (except class Object) of the caller are
        // registered as parallel capable.
        assertThat(TestParentFirstClassLoader.isParallelCapable).isTrue();
    }

    @Test
    void testParentFirstClassLoadingByAddURL() throws Exception {
        // collect the libraries / class folders with RocksDB related code: the state backend and
        // RocksDB itself
        final URL childCodePath = getClass().getProtectionDomain().getCodeSource().getLocation();

        final MutableURLClassLoader parentClassLoader =
                createChildFirstClassLoader(childCodePath, getClass().getClassLoader());
        final MutableURLClassLoader childClassLoader1 =
                createParentFirstClassLoader(childCodePath, parentClassLoader);
        final MutableURLClassLoader childClassLoader2 =
                createParentFirstClassLoader(childCodePath, parentClassLoader);

        // test class loader before add user jar ulr to ClassLoader
        assertClassNotFoundException(USER_CLASS, false, parentClassLoader);
        assertClassNotFoundException(USER_CLASS, false, childClassLoader1);
        assertClassNotFoundException(USER_CLASS, false, childClassLoader2);

        // only add jar url to parent ClassLoader
        parentClassLoader.addURL(userJar.toURI().toURL());

        // test class loader after add jar url
        final Class<?> clazz1 = Class.forName(USER_CLASS, false, parentClassLoader);
        final Class<?> clazz2 = Class.forName(USER_CLASS, false, childClassLoader1);
        final Class<?> clazz3 = Class.forName(USER_CLASS, false, childClassLoader2);

        assertThat(clazz2).isEqualTo(clazz1);
        assertThat(clazz3).isEqualTo(clazz1);

        parentClassLoader.close();
        childClassLoader1.close();
        childClassLoader2.close();
    }

    @Test
    void testChildFirstClassLoadingByAddURL() throws Exception {

        // collect the libraries / class folders with RocksDB related code: the state backend and
        // RocksDB itself
        final URL childCodePath = getClass().getProtectionDomain().getCodeSource().getLocation();

        final MutableURLClassLoader parentClassLoader =
                createChildFirstClassLoader(childCodePath, getClass().getClassLoader());
        final MutableURLClassLoader childClassLoader1 =
                createChildFirstClassLoader(childCodePath, parentClassLoader);
        final MutableURLClassLoader childClassLoader2 =
                createChildFirstClassLoader(childCodePath, parentClassLoader);

        // test class loader before add user jar ulr to ClassLoader
        assertClassNotFoundException(USER_CLASS, false, parentClassLoader);
        assertClassNotFoundException(USER_CLASS, false, childClassLoader1);
        assertClassNotFoundException(USER_CLASS, false, childClassLoader2);

        // only add jar url to child ClassLoader
        URL userJarURL = userJar.toURI().toURL();
        childClassLoader1.addURL(userJarURL);
        childClassLoader2.addURL(userJarURL);

        // test class loader after add jar url
        assertClassNotFoundException(USER_CLASS, false, parentClassLoader);

        final Class<?> clazz1 = Class.forName(USER_CLASS, false, childClassLoader1);
        final Class<?> clazz2 = Class.forName(USER_CLASS, false, childClassLoader2);

        assertThat(clazz2).isNotEqualTo(clazz1);

        parentClassLoader.close();
        childClassLoader1.close();
        childClassLoader2.close();
    }

    private void assertClassNotFoundException(
            String className, boolean initialize, ClassLoader classLoader) {
        assertThatThrownBy(() -> Class.forName(className, initialize, classLoader))
                .isInstanceOf(ClassNotFoundException.class);
    }

    private static class TestParentFirstClassLoader
            extends FlinkUserCodeClassLoaders.ParentFirstClassLoader {
        public static boolean isParallelCapable;

        static {
            isParallelCapable = ClassLoader.registerAsParallelCapable();
        }

        TestParentFirstClassLoader() {
            super(null, null, null);
        }
    }

    private static class ClassToLoad {}
}
