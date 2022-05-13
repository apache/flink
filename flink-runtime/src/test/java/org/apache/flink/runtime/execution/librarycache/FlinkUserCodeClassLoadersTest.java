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

package org.apache.flink.runtime.execution.librarycache;

import org.apache.flink.runtime.rpc.messages.RemoteRpcInvocation;
import org.apache.flink.runtime.util.ClassLoaderUtil;
import org.apache.flink.testutils.ClassLoaderUtils;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.TestLogger;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.net.URL;
import java.net.URLClassLoader;

import static org.apache.flink.util.FlinkUserCodeClassLoader.NOOP_EXCEPTION_HANDLER;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.isA;
import static org.hamcrest.Matchers.hasItemInArray;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/** Tests for classloading and class loader utilities. */
public class FlinkUserCodeClassLoadersTest extends TestLogger {

    @ClassRule public static TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Rule public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testMessageDecodingWithUnavailableClass() throws Exception {
        final ClassLoader systemClassLoader = getClass().getClassLoader();

        final String className = "UserClass";
        final URLClassLoader userClassLoader =
                ClassLoaderUtils.compileAndLoadJava(
                        temporaryFolder.newFolder(),
                        className + ".java",
                        "import java.io.Serializable;\n"
                                + "public class "
                                + className
                                + " implements Serializable {}");

        RemoteRpcInvocation method =
                new RemoteRpcInvocation(
                        className,
                        "test",
                        new Class<?>[] {
                            int.class, Class.forName(className, false, userClassLoader)
                        },
                        new Object[] {
                            1, Class.forName(className, false, userClassLoader).newInstance()
                        });

        SerializedValue<RemoteRpcInvocation> serializedMethod = new SerializedValue<>(method);

        expectedException.expect(ClassNotFoundException.class);
        expectedException.expect(
                allOf(
                        isA(ClassNotFoundException.class),
                        hasProperty(
                                "suppressed",
                                hasItemInArray(
                                        allOf(
                                                isA(ClassNotFoundException.class),
                                                hasProperty(
                                                        "message",
                                                        containsString(
                                                                "Could not deserialize 1th parameter type of method test(int, ...).")))))));

        RemoteRpcInvocation deserializedMethod =
                serializedMethod.deserializeValue(systemClassLoader);
        deserializedMethod.getMethodName();

        userClassLoader.close();
    }

    @Test
    public void testParentFirstClassLoading() throws Exception {
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

        assertEquals(clazz1, clazz2);
        assertEquals(clazz1, clazz3);

        childClassLoader1.close();
        childClassLoader2.close();
    }

    @Test
    public void testChildFirstClassLoading() throws Exception {
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

        assertNotEquals(clazz1, clazz2);
        assertNotEquals(clazz1, clazz3);
        assertNotEquals(clazz2, clazz3);

        childClassLoader1.close();
        childClassLoader2.close();
    }

    @Test
    public void testRepeatedChildFirstClassLoading() throws Exception {
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

        assertNotEquals(clazz1, clazz2);

        assertEquals(clazz2, clazz3);
        assertEquals(clazz2, clazz4);

        childClassLoader.close();
    }

    @Test
    public void testRepeatedParentFirstPatternClass() throws Exception {
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

        assertEquals(clazz1, clazz2);
        assertEquals(clazz1, clazz3);
        assertEquals(clazz1, clazz4);

        childClassLoader.close();
    }

    @Test
    public void testGetClassLoaderInfo() throws Exception {
        final ClassLoader parentClassLoader = getClass().getClassLoader();

        final URL childCodePath = getClass().getProtectionDomain().getCodeSource().getLocation();

        final URLClassLoader childClassLoader =
                createChildFirstClassLoader(childCodePath, parentClassLoader);

        String formattedURL = ClassLoaderUtil.formatURL(childCodePath);

        assertEquals(
                ClassLoaderUtil.getUserCodeClassLoaderInfo(childClassLoader),
                "URL ClassLoader:" + formattedURL);

        childClassLoader.close();
    }

    @Test
    public void testGetClassLoaderInfoWithClassLoaderClosed() throws Exception {
        final ClassLoader parentClassLoader = getClass().getClassLoader();

        final URL childCodePath = getClass().getProtectionDomain().getCodeSource().getLocation();

        final URLClassLoader childClassLoader =
                createChildFirstClassLoader(childCodePath, parentClassLoader);

        childClassLoader.close();

        assertThat(
                ClassLoaderUtil.getUserCodeClassLoaderInfo(childClassLoader),
                startsWith("Cannot access classloader info due to an exception."));
    }

    private static URLClassLoader createParentFirstClassLoader(
            URL childCodePath, ClassLoader parentClassLoader) {
        return FlinkUserCodeClassLoaders.parentFirst(
                new URL[] {childCodePath}, parentClassLoader, NOOP_EXCEPTION_HANDLER, true);
    }

    private static URLClassLoader createChildFirstClassLoader(
            URL childCodePath, ClassLoader parentClassLoader) {
        return FlinkUserCodeClassLoaders.childFirst(
                new URL[] {childCodePath},
                parentClassLoader,
                new String[0],
                NOOP_EXCEPTION_HANDLER,
                true);
    }

    @Test
    public void testClosingOfClassloader() throws Exception {
        final String className = ClassToLoad.class.getName();

        final ClassLoader parentClassLoader = ClassLoader.getSystemClassLoader().getParent();

        final URL childCodePath = getClass().getProtectionDomain().getCodeSource().getLocation();

        final URLClassLoader childClassLoader =
                createChildFirstClassLoader(childCodePath, parentClassLoader);

        final Class<?> loadedClass = childClassLoader.loadClass(className);

        assertNotSame(ClassToLoad.class, loadedClass);

        childClassLoader.close();

        // after closing, no loaded class should be reachable anymore
        expectedException.expect(isA(IllegalStateException.class));
        childClassLoader.loadClass(className);
    }

    @Test
    public void testParallelCapable() {
        // It will be true only if all the super classes (except class Object) of the caller are
        // registered as parallel capable.
        assertTrue(TestParentFirstClassLoader.isParallelCapable);
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
