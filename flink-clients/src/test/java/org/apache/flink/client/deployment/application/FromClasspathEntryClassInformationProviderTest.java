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

package org.apache.flink.client.deployment.application;

import org.apache.flink.client.testjar.ClasspathProvider;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.TestLogger;

import org.apache.commons.io.FilenameUtils;
import org.hamcrest.CoreMatchers;
import org.hamcrest.collection.IsIterableContainingInAnyOrder;
import org.hamcrest.core.IsCollectionContaining;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

/**
 * {@code FromClasspathEntryClassInformationProviderTest} tests {@link
 * FromClasspathEntryClassInformationProvider}.
 */
public class FromClasspathEntryClassInformationProviderTest extends TestLogger {

    @Rule
    public ClasspathProvider noEntryClassClasspathProvider =
            ClasspathProvider.createWithNoEntryClass();

    @Rule
    public ClasspathProvider singleEntryClassClasspathProvider =
            ClasspathProvider.createWithSingleEntryClass();

    @Rule
    public ClasspathProvider multipleEntryClassesClasspathProvider =
            ClasspathProvider.createWithMultipleEntryClasses();

    @Rule
    public ClasspathProvider testJobEntryClassClasspathProvider =
            ClasspathProvider.createWithTestJobOnly();

    @Rule
    public ClasspathProvider onlyTextFileClasspathProvider =
            ClasspathProvider.createWithTextFileOnly();

    @Test
    public void testJobClassOnUserClasspathWithExplicitJobClassName()
            throws IOException, FlinkException {
        FromClasspathEntryClassInformationProvider testInstance =
                FromClasspathEntryClassInformationProvider.create(
                        singleEntryClassClasspathProvider.getJobClassName(),
                        singleEntryClassClasspathProvider.getURLUserClasspath());

        assertThat(testInstance.getJobClassName().isPresent(), is(true));
        assertThat(
                testInstance.getJobClassName().get(),
                is(singleEntryClassClasspathProvider.getJobClassName()));
        assertThat(testInstance.getJarFile().isPresent(), is(false));
    }

    @Test(expected = FlinkException.class)
    public void testJobClassOnUserClasspathWithOnlyTestFileOnClasspath()
            throws IOException, FlinkException {
        // we want to check that the right exception is thrown if the user classpath is empty
        FromClasspathEntryClassInformationProvider.create(
                "SomeJobClassName", onlyTextFileClasspathProvider.getURLUserClasspath());
    }

    @Test(expected = NullPointerException.class)
    public void testJobClassOnUserClasspathWithMissingJobClassName()
            throws IOException, FlinkException {
        FromClasspathEntryClassInformationProvider.create(
                null, singleEntryClassClasspathProvider.getURLUserClasspath());
    }

    @Test(expected = NullPointerException.class)
    public void testJobClassOnUserClasspathWithMissingUserClasspath()
            throws IOException, FlinkException {
        FromClasspathEntryClassInformationProvider.create("jobClassName", null);
    }

    @Test
    public void testJobClassOnUserClasspathWithoutExplicitJobClassName()
            throws IOException, FlinkException {
        FromClasspathEntryClassInformationProvider testInstance =
                FromClasspathEntryClassInformationProvider.createFromClasspath(
                        singleEntryClassClasspathProvider.getURLUserClasspath());

        assertThat(testInstance.getJobClassName().isPresent(), is(true));
        assertThat(
                testInstance.getJobClassName().get(),
                is(singleEntryClassClasspathProvider.getJobClassName()));
        assertThat(testInstance.getJarFile().isPresent(), is(false));
    }

    @Test(expected = FlinkException.class)
    public void testMissingJobClassOnUserClasspathWithoutExplicitJobClassName()
            throws IOException, FlinkException {
        FromClasspathEntryClassInformationProvider.createFromClasspath(
                noEntryClassClasspathProvider.getURLUserClasspath());
    }

    @Test(expected = FlinkException.class)
    public void testTooManyMainMethodsOnUserClasspath() throws IOException, FlinkException {
        FromClasspathEntryClassInformationProvider.createFromClasspath(
                multipleEntryClassesClasspathProvider.getURLUserClasspath());
    }

    @Test(expected = NullPointerException.class)
    public void testJobClassOnUserClasspathWithoutExplicitJobClassNameAndMissingUserClasspath()
            throws IOException, FlinkException {
        FromClasspathEntryClassInformationProvider.createFromClasspath(null);
    }

    @Test
    public void testJobClassNameFromSystemClasspath() throws IOException, FlinkException {
        singleEntryClassClasspathProvider.setSystemClasspath();
        FromClasspathEntryClassInformationProvider testInstance =
                FromClasspathEntryClassInformationProvider.createFromSystemClasspath();
        assertThat(testInstance.getJobClassName().isPresent(), is(true));
        assertThat(
                testInstance.getJobClassName().get(),
                is(singleEntryClassClasspathProvider.getJobClassName()));
        assertThat(testInstance.getJarFile().isPresent(), is(false));
    }

    @Test(expected = FlinkException.class)
    public void testMissingJobClassNameFromSystemClasspath() throws IOException, FlinkException {
        noEntryClassClasspathProvider.setSystemClasspath();
        FromClasspathEntryClassInformationProvider.createFromSystemClasspath();
    }

    @Test(expected = FlinkException.class)
    public void testTooManyMainMethodsOnSystemClasspath() throws IOException, FlinkException {
        multipleEntryClassesClasspathProvider.setSystemClasspath();
        FromClasspathEntryClassInformationProvider.createFromSystemClasspath();
    }

    @Test
    public void testJarFromSystemClasspathSanityCheck() {
        // Junit executes this test, so it should be returned as part of JARs on the classpath
        final Iterable<File> systemClasspath =
                FromClasspathEntryClassInformationProvider.extractSystemClasspath();
        assertThat(
                StreamSupport.stream(systemClasspath.spliterator(), false)
                        .map(File::getName)
                        .collect(Collectors.toList()),
                IsCollectionContaining.hasItem(CoreMatchers.containsString("junit")));
    }

    @Test
    public void testJarFromSystemClasspath() throws MalformedURLException {
        multipleEntryClassesClasspathProvider.setSystemClasspath();
        final Collection<String> systemClasspath =
                StreamSupport.stream(
                                FromClasspathEntryClassInformationProvider.extractSystemClasspath()
                                        .spliterator(),
                                false)
                        .map(File::getName)
                        .collect(Collectors.toList());
        final Collection<String> expectedContent =
                StreamSupport.stream(
                                multipleEntryClassesClasspathProvider
                                        .getURLUserClasspath()
                                        .spliterator(),
                                false)
                        .map(URL::getPath)
                        .map(FilenameUtils::getName)
                        // we're excluding any non-jar files
                        .filter(name -> name.endsWith("jar"))
                        .collect(Collectors.toList());
        assertThat(
                systemClasspath,
                IsIterableContainingInAnyOrder.containsInAnyOrder(expectedContent.toArray()));
    }
}
