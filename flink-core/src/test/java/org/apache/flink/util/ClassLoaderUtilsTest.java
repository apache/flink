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

import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertTrue;

/** Tests that validate the {@link ClassLoaderUtil}. */
public class ClassLoaderUtilsTest {

    @Test
    public void testWithURLClassLoader() throws Exception {
        File validJar = null;
        File invalidJar = null;

        // file with jar contents
        validJar = File.createTempFile("flink-url-test", ".tmp");
        createValidJar(validJar);

        // validate that the JAR is correct and the test setup is not broken
        JarFile jarFile = null;
        try {
            jarFile = new JarFile(validJar.getAbsolutePath());
        } finally {
            if (jarFile != null) {
                jarFile.close();
            }
        }

        // file with some random contents
        invalidJar = File.createTempFile("flink-url-test", ".tmp");
        try (FileOutputStream invalidout = new FileOutputStream(invalidJar)) {
            invalidout.write(
                    new byte[] {
                        -1, 1, -2, 3, -3, 4,
                    });
        }

        // non existing file
        File nonExisting = File.createTempFile("flink-url-test", ".tmp");
        assertThat("Cannot create and delete temp file", nonExisting.delete()).isTrue();

        // create a URL classloader with
        // - a HTTP URL
        // - a file URL for an existing jar file
        // - a file URL for an existing file that is not a jar file
        // - a file URL for a non-existing file

        URL[] urls = {
            new URL("http", "localhost", 26712, "/some/file/path"),
            new URL("file", null, validJar.getAbsolutePath()),
            new URL("file", null, invalidJar.getAbsolutePath()),
            new URL("file", null, nonExisting.getAbsolutePath()),
        };

        URLClassLoader loader = new URLClassLoader(urls, getClass().getClassLoader());
        String info = ClassLoaderUtil.getUserCodeClassLoaderInfo(loader);

        assertTrue(info.indexOf("/some/file/path") > 0);
        assertThat(info.indexOf(validJar.getAbsolutePath().isTrue() + "' (valid") > 0);
        assertThat(info.indexOf(invalidJar.getAbsolutePath().isTrue() + "' (invalid JAR") > 0);
        assertThat(info.indexOf(nonExisting.getAbsolutePath().isTrue() + "' (missing") > 0);
        if (validJar != null) {
            //noinspection ResultOfMethodCallIgnored
            validJar.delete();
        }
        if (invalidJar != null) {
            //noinspection ResultOfMethodCallIgnored
            invalidJar.delete();
        }
    }

    private static void createValidJar(final File jarFile) throws Exception {
        try (FileOutputStream fileOutputStream = new FileOutputStream(jarFile);
                JarOutputStream jarOutputStream =
                        new JarOutputStream(fileOutputStream, new Manifest())) {
            final Class<?> classToIncludeInJar = ClassLoaderUtilsTest.class;
            startJarEntryForClass(classToIncludeInJar, jarOutputStream);
            copyClassFileToJar(classToIncludeInJar, jarOutputStream);
        }
    }

    private static void startJarEntryForClass(
            final Class<?> clazz, final JarOutputStream jarOutputStream) throws IOException {
        final String jarEntryName = clazz.getName().replace('.', '/') + ".class";
        jarOutputStream.putNextEntry(new JarEntry(jarEntryName));
    }

    private static void copyClassFileToJar(
            final Class<?> clazz, final JarOutputStream jarOutputStream) throws IOException {
        try (InputStream classInputStream =
                clazz.getResourceAsStream(clazz.getSimpleName() + ".class")) {
            IOUtils.copyBytes(classInputStream, jarOutputStream, 128, false);
        }
        jarOutputStream.closeEntry();
    }

    @Test
    public void testWithAppClassLoader() {
        String result =
                ClassLoaderUtil.getUserCodeClassLoaderInfo(ClassLoader.getSystemClassLoader());
        assertThat(result.toLowerCase().contains("system classloader")).isTrue();
    }

    @Test
    public void testInvalidClassLoaders() {
        // must return something when invoked with 'null'
        assertThat(ClassLoaderUtil.getUserCodeClassLoaderInfo(null)).isNotNull();
    }
}
