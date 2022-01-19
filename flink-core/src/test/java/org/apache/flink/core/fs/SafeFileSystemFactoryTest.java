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

package org.apache.flink.core.fs;

import org.apache.flink.util.ChildFirstClassLoader;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URL;
import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.ProtectionDomain;
import java.util.List;
import java.util.Vector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatIOException;

/** Tests for the {@link SafeFileSystemFactory}. */
public class SafeFileSystemFactoryTest {

    /**
     * We need to load this class in separate classloader to leak the classloader through the {@link
     * ProtectionDomain}.
     */
    private static class ProtectionDomainLeakTest {

        ProtectionDomainLeakTest(FileSystemFactory factory) throws Exception {
            factory.create(URI.create("test://filesystem"));
        }
    }

    private abstract static class TestFileSystemFactory implements FileSystemFactory {

        @Override
        public String getScheme() {
            throw new UnsupportedOperationException("Test.");
        }
    }

    private static Stream<Class<?>> getLoadedClasses(ClassLoader classLoader) throws Exception {
        final Field field = ClassLoader.class.getDeclaredField("classes");
        field.setAccessible(true);
        @SuppressWarnings("unchecked")
        final Vector<Class<?>> classes = (Vector<Class<?>>) field.get(classLoader);
        return classes.stream();
    }

    private static List<Class<?>> getLoadedFlinkClasses(ClassLoader classLoader) throws Exception {
        return getLoadedClasses(classLoader)
                .filter(c -> c.getName().startsWith("org.apache.flink"))
                .collect(Collectors.toList());
    }

    private static ProtectionDomain[] getProtectionDomains() {
        try {
            final AccessControlContext context = AccessController.getContext();
            final Method domainResolver = context.getClass().getDeclaredMethod("getContext");
            domainResolver.setAccessible(true);
            return (ProtectionDomain[]) domainResolver.invoke(context);
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void testProtectionDomainLeaksWithRegularFileSystemFactory() throws Exception {
        testProtectionDomainLeak(
                new TestFileSystemFactory() {

                    @Override
                    public FileSystem create(URI fsUri) {
                        boolean leak = false;
                        for (ProtectionDomain protectionDomain : getProtectionDomains()) {
                            if (protectionDomain.getClassLoader()
                                    instanceof ChildFirstClassLoader) {
                                leak = true;
                                break;
                            }
                        }
                        assertThat(leak).isTrue();
                        return null;
                    }
                });
    }

    @Test
    void testProtectionDomainDoesNotLeakWithWrappedFileSystemFactory() throws Exception {
        testProtectionDomainLeak(
                SafeFileSystemFactory.of(
                        new TestFileSystemFactory() {

                            @Override
                            public FileSystem create(URI fsUri) {
                                for (ProtectionDomain protectionDomain : getProtectionDomains()) {
                                    assertThat(protectionDomain.getCodeSource())
                                            .isNotInstanceOf(ChildFirstClassLoader.class);
                                }
                                return null;
                            }
                        }));
    }

    private void testProtectionDomainLeak(FileSystemFactory factory) throws Exception {
        final URL testClassLocation =
                getClass().getProtectionDomain().getCodeSource().getLocation();
        final ChildFirstClassLoader leakyClassLoader =
                new ChildFirstClassLoader(
                        new URL[] {testClassLocation},
                        Thread.currentThread().getContextClassLoader(),
                        new String[] {"java."},
                        error -> {
                            // No-op.
                        });
        final Class<?> clazz =
                Class.forName(ProtectionDomainLeakTest.class.getName(), true, leakyClassLoader);
        final Constructor<?> declaredConstructor =
                clazz.getDeclaredConstructor(FileSystemFactory.class);
        declaredConstructor.setAccessible(true);
        declaredConstructor.newInstance(factory);
        // We have to assert class name here, because the class we want to assert has been loaded in
        // a different classloader.
        assertThat(getLoadedFlinkClasses(leakyClassLoader))
                .map(Class::getName)
                .singleElement()
                .isEqualTo(ProtectionDomainLeakTest.class.getName());
    }

    @Test
    void testIOExceptionHandlingDuringFileSystemCreation() {
        final FileSystemFactory factory =
                SafeFileSystemFactory.of(
                        new TestFileSystemFactory() {

                            @Override
                            public FileSystem create(URI fsUri) throws IOException {
                                throw new IOException("Test.");
                            }
                        });
        assertThatIOException()
                .isThrownBy(() -> factory.create(URI.create("test://filesystem")))
                .withNoCause()
                .withMessage("Test.");
    }

    @Test
    void testUncheckedExceptionHandlingDuringFileSystemCreation() {
        final FileSystemFactory factory =
                SafeFileSystemFactory.of(
                        new TestFileSystemFactory() {

                            @Override
                            public FileSystem create(URI fsUri) {
                                throw new IllegalStateException("Test.");
                            }
                        });
        assertThatExceptionOfType(IOException.class)
                .isThrownBy(() -> factory.create(URI.create("test://filesystem")))
                .withCauseExactlyInstanceOf(IllegalStateException.class)
                .havingCause()
                .withMessage("Test.");
    }
}
