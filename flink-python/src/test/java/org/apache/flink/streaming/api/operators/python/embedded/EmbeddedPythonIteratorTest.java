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

package org.apache.flink.streaming.api.operators.python.embedded;

import org.junit.jupiter.api.Test;
import pemja.core.object.PyIterator;

import java.lang.reflect.Constructor;
import java.net.URL;
import java.net.URLClassLoader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link EmbeddedPythonIterator}. */
class EmbeddedPythonIteratorTest {

    @Test
    void testReadsIteratorLoadedByDifferentClassLoader() throws Exception {
        URL classesUrl =
                EmbeddedPythonIteratorTest.class
                        .getProtectionDomain()
                        .getCodeSource()
                        .getLocation();

        try (URLClassLoader classLoader = new URLClassLoader(new URL[] {classesUrl}, null)) {
            Class<?> iteratorClass =
                    Class.forName(
                            "org.apache.flink.streaming.api.operators.python.embedded."
                                    + "ForeignClassLoaderIterator",
                            true,
                            classLoader);
            Object iterator =
                    iteratorClass
                            .getConstructor(Object[].class)
                            .newInstance((Object) new Object[] {"first", "second"});

            assertThat(iterator.getClass().getClassLoader())
                    .isNotSameAs(EmbeddedPythonIteratorTest.class.getClassLoader());

            try (EmbeddedPythonIterator embeddedPythonIterator =
                    EmbeddedPythonIterator.from(iterator)) {
                assertThat(embeddedPythonIterator.hasNext()).isTrue();
                assertThat(embeddedPythonIterator.next()).isEqualTo("first");
                assertThat(embeddedPythonIterator.hasNext()).isTrue();
                assertThat(embeddedPythonIterator.next()).isEqualTo("second");
                assertThat(embeddedPythonIterator.hasNext()).isFalse();
            }

            assertThat(iteratorClass.getMethod("isClosed").invoke(iterator)).isEqualTo(true);
        }
    }

    @Test
    void testReproducesDirectPemjaCastFailureAcrossClassLoaders() throws Exception {
        Object iterator = createForeignPemjaIterator();

        assertThat(iterator.getClass().getName()).isEqualTo(PyIterator.class.getName());
        assertThat(iterator.getClass()).isNotEqualTo(PyIterator.class);
        assertThat(iterator.getClass().getClassLoader())
                .isNotSameAs(PyIterator.class.getClassLoader());

        assertThatThrownBy(() -> castToLocalPemjaIterator(iterator))
                .isInstanceOf(ClassCastException.class)
                .hasMessageContaining("pemja.core.object.PyIterator")
                .hasMessageContaining("cannot be cast");
    }

    @Test
    void testWrapsPemjaIteratorLoadedByDifferentClassLoaderWithoutCastFailure() throws Exception {
        Object iterator = createForeignPemjaIterator();

        assertThatCode(() -> EmbeddedPythonIterator.from(iterator)).doesNotThrowAnyException();
    }

    private static Object createForeignPemjaIterator() throws Exception {
        URL pemjaJarUrl = PyIterator.class.getProtectionDomain().getCodeSource().getLocation();

        try (URLClassLoader classLoader = new URLClassLoader(new URL[] {pemjaJarUrl}, null)) {
            Class<?> iteratorClass =
                    Class.forName("pemja.core.object.PyIterator", true, classLoader);
            Constructor<?> constructor =
                    iteratorClass.getDeclaredConstructor(long.class, long.class);
            constructor.setAccessible(true);
            return constructor.newInstance(0L, 0L);
        }
    }

    private static PyIterator castToLocalPemjaIterator(Object iterator) {
        return (PyIterator) iterator;
    }
}
