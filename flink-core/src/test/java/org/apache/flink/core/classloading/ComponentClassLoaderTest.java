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

package org.apache.flink.core.classloading;

import org.apache.flink.util.TestLogger;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Enumeration;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

/** Tests for the {@link ComponentClassLoader}. */
public class ComponentClassLoaderTest extends TestLogger {

    private static final String NON_EXISTENT_CLASS_NAME = "foo.Bar";
    private static final Class<?> CLASS_TO_LOAD = Class.class;
    private static final Class<?> CLASS_RETURNED_BY_OWNER = ComponentClassLoaderTest.class;

    private static final String NON_EXISTENT_RESOURCE_NAME = "foo/Bar";
    private static String resourceToLoad;
    private static final URL RESOURCE_RETURNED_BY_OWNER = createURL();

    @ClassRule public static final TemporaryFolder TMP = new TemporaryFolder();

    @BeforeClass
    public static void setup() throws IOException {
        resourceToLoad = TMP.newFile("tmpfile").getName();
    }

    // ----------------------------------------------------------------------------------------------
    // Class loading
    // ----------------------------------------------------------------------------------------------

    @Test(expected = ClassNotFoundException.class)
    public void testComponentOnlyIsDefaultForClasses() throws Exception {
        TestUrlClassLoader owner =
                new TestUrlClassLoader(NON_EXISTENT_CLASS_NAME, CLASS_RETURNED_BY_OWNER);

        final ComponentClassLoader componentClassLoader =
                new ComponentClassLoader(new URL[0], owner, new String[0], new String[0]);

        componentClassLoader.loadClass(NON_EXISTENT_CLASS_NAME);
    }

    @Test
    public void testOwnerFirstClassFoundIgnoresComponent() throws Exception {
        TestUrlClassLoader owner =
                new TestUrlClassLoader(CLASS_TO_LOAD.getName(), CLASS_RETURNED_BY_OWNER);

        final ComponentClassLoader componentClassLoader =
                new ComponentClassLoader(
                        new URL[0], owner, new String[] {CLASS_TO_LOAD.getName()}, new String[0]);

        final Class<?> loadedClass = componentClassLoader.loadClass(CLASS_TO_LOAD.getName());
        assertThat(loadedClass, sameInstance(CLASS_RETURNED_BY_OWNER));
    }

    @Test
    public void testOwnerFirstClassNotFoundFallsBackToComponent() throws Exception {
        TestUrlClassLoader owner = new TestUrlClassLoader();

        final ComponentClassLoader componentClassLoader =
                new ComponentClassLoader(
                        new URL[0], owner, new String[] {CLASS_TO_LOAD.getName()}, new String[0]);

        final Class<?> loadedClass = componentClassLoader.loadClass(CLASS_TO_LOAD.getName());
        assertThat(loadedClass, sameInstance(CLASS_TO_LOAD));
    }

    @Test
    public void testComponentFirstClassFoundIgnoresOwner() throws Exception {
        TestUrlClassLoader owner =
                new TestUrlClassLoader(CLASS_TO_LOAD.getName(), CLASS_RETURNED_BY_OWNER);

        final ComponentClassLoader componentClassLoader =
                new ComponentClassLoader(
                        new URL[0], owner, new String[0], new String[] {CLASS_TO_LOAD.getName()});

        final Class<?> loadedClass = componentClassLoader.loadClass(CLASS_TO_LOAD.getName());
        assertThat(loadedClass, sameInstance(CLASS_TO_LOAD));
    }

    @Test
    public void testComponentFirstClassNotFoundFallsBackToOwner() throws Exception {
        TestUrlClassLoader owner =
                new TestUrlClassLoader(NON_EXISTENT_CLASS_NAME, CLASS_RETURNED_BY_OWNER);

        final ComponentClassLoader componentClassLoader =
                new ComponentClassLoader(
                        new URL[0], owner, new String[0], new String[] {NON_EXISTENT_CLASS_NAME});

        final Class<?> loadedClass = componentClassLoader.loadClass(NON_EXISTENT_CLASS_NAME);
        assertThat(loadedClass, sameInstance(CLASS_RETURNED_BY_OWNER));
    }

    // ----------------------------------------------------------------------------------------------
    // Resource loading
    // ----------------------------------------------------------------------------------------------

    @Test
    public void testComponentOnlyIsDefaultForResources() throws IOException {
        TestUrlClassLoader owner = new TestUrlClassLoader();

        final ComponentClassLoader componentClassLoader =
                new ComponentClassLoader(new URL[0], owner, new String[0], new String[0]);

        assertThat(componentClassLoader.getResource(NON_EXISTENT_RESOURCE_NAME), nullValue());
        assertThat(
                componentClassLoader.getResources(NON_EXISTENT_RESOURCE_NAME).hasMoreElements(),
                is(false));
    }

    @Test
    public void testOwnerFirstResourceFoundIgnoresComponent() {
        TestUrlClassLoader owner =
                new TestUrlClassLoader(resourceToLoad, RESOURCE_RETURNED_BY_OWNER);

        final ComponentClassLoader componentClassLoader =
                new ComponentClassLoader(
                        new URL[] {}, owner, new String[] {resourceToLoad}, new String[0]);

        final URL loadedResource = componentClassLoader.getResource(resourceToLoad);
        assertThat(loadedResource, sameInstance(RESOURCE_RETURNED_BY_OWNER));
    }

    @Test
    public void testOwnerFirstResourceNotFoundFallsBackToComponent() throws Exception {
        TestUrlClassLoader owner = new TestUrlClassLoader();

        final ComponentClassLoader componentClassLoader =
                new ComponentClassLoader(
                        new URL[] {TMP.getRoot().toURI().toURL()},
                        owner,
                        new String[] {resourceToLoad},
                        new String[0]);

        final URL loadedResource = componentClassLoader.getResource(resourceToLoad);
        assertThat(loadedResource.toString(), containsString(resourceToLoad));
    }

    @Test
    public void testComponentFirstResourceFoundIgnoresOwner() throws Exception {
        TestUrlClassLoader owner =
                new TestUrlClassLoader(resourceToLoad, RESOURCE_RETURNED_BY_OWNER);

        final ComponentClassLoader componentClassLoader =
                new ComponentClassLoader(
                        new URL[] {TMP.getRoot().toURI().toURL()},
                        owner,
                        new String[0],
                        new String[] {resourceToLoad});

        final URL loadedResource = componentClassLoader.getResource(resourceToLoad);
        assertThat(loadedResource.toString(), containsString(resourceToLoad));
    }

    @Test
    public void testComponentFirstResourceNotFoundFallsBackToOwner() {
        TestUrlClassLoader owner =
                new TestUrlClassLoader(NON_EXISTENT_RESOURCE_NAME, RESOURCE_RETURNED_BY_OWNER);

        final ComponentClassLoader componentClassLoader =
                new ComponentClassLoader(
                        new URL[0],
                        owner,
                        new String[0],
                        new String[] {NON_EXISTENT_RESOURCE_NAME});

        final URL loadedResource = componentClassLoader.getResource(NON_EXISTENT_RESOURCE_NAME);
        assertThat(loadedResource, sameInstance(RESOURCE_RETURNED_BY_OWNER));
    }

    private static class TestUrlClassLoader extends URLClassLoader {

        private final String nameToCheck;
        private final Class<?> classToReturn;
        private final URL resourceToReturn;

        public TestUrlClassLoader() {
            this(null, null, null);
        }

        public TestUrlClassLoader(String resourceNameToCheck, URL resourceToReturn) {
            this(checkNotNull(resourceNameToCheck), null, checkNotNull(resourceToReturn));
        }

        public TestUrlClassLoader(String classNameToCheck, Class<?> classToReturn) {
            this(checkNotNull(classNameToCheck), checkNotNull(classToReturn), null);
        }

        public TestUrlClassLoader(
                String classNameToCheck, Class<?> classToReturn, URL resourceToReturn) {
            super(new URL[0], null);
            this.nameToCheck = classNameToCheck;
            this.classToReturn = classToReturn;
            this.resourceToReturn = resourceToReturn;
        }

        @Override
        public Class<?> loadClass(String name) throws ClassNotFoundException {
            if (nameToCheck == null) {
                throw new ClassNotFoundException();
            }
            if (nameToCheck.equals(name)) {
                return classToReturn;
            }
            return super.loadClass(name);
        }

        @Override
        public URL getResource(String name) {
            if (nameToCheck == null) {
                return null;
            }
            if (nameToCheck.equals(name)) {
                return resourceToReturn;
            }
            return super.getResource(name);
        }

        @Override
        public Enumeration<URL> getResources(String name) throws IOException {
            if (nameToCheck != null && nameToCheck.equals(name)) {
                return new ComponentClassLoader.IteratorBackedEnumeration<>(
                        Collections.singleton(resourceToReturn).iterator());
            }
            return super.getResources(name);
        }
    }

    private static URL createURL() {
        try {
            return Paths.get("").toUri().toURL();
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }
}
