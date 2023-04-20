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

import org.apache.flink.client.testjar.ClasspathProviderExtension;
import org.apache.flink.util.FlinkException;

import org.apache.commons.io.FilenameUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@code FromClasspathEntryClassInformationProviderTest} tests {@link
 * FromClasspathEntryClassInformationProvider}.
 */
class FromClasspathEntryClassInformationProviderTest {

    @RegisterExtension
    ClasspathProviderExtension noEntryClassClasspathProvider =
            ClasspathProviderExtension.createWithNoEntryClass();

    @RegisterExtension
    ClasspathProviderExtension singleEntryClassClasspathProvider =
            ClasspathProviderExtension.createWithSingleEntryClass();

    @RegisterExtension
    ClasspathProviderExtension multipleEntryClassesClasspathProvider =
            ClasspathProviderExtension.createWithMultipleEntryClasses();

    @RegisterExtension
    ClasspathProviderExtension testJobEntryClassClasspathProvider =
            ClasspathProviderExtension.createWithTestJobOnly();

    @Test
    void testJobClassOnUserClasspathWithExplicitJobClassName() throws IOException, FlinkException {
        FromClasspathEntryClassInformationProvider testInstance =
                FromClasspathEntryClassInformationProvider.create(
                        singleEntryClassClasspathProvider.getJobClassName(),
                        singleEntryClassClasspathProvider.getURLUserClasspath());

        assertThat(testInstance.getJobClassName())
                .contains(singleEntryClassClasspathProvider.getJobClassName());
        assertThat(testInstance.getJarFile()).isEmpty();
    }

    @Test
    void testJobClassOnUserClasspathWithMissingJobClassName() {
        assertThatThrownBy(
                        () ->
                                FromClasspathEntryClassInformationProvider.create(
                                        null,
                                        singleEntryClassClasspathProvider.getURLUserClasspath()))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void testJobClassOnUserClasspathWithMissingUserClasspath() {
        assertThatThrownBy(
                        () ->
                                FromClasspathEntryClassInformationProvider.create(
                                        "jobClassName", null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void testJobClassOnUserClasspathWithoutExplicitJobClassName()
            throws IOException, FlinkException {
        FromClasspathEntryClassInformationProvider testInstance =
                FromClasspathEntryClassInformationProvider.createFromClasspath(
                        singleEntryClassClasspathProvider.getURLUserClasspath());

        assertThat(testInstance.getJobClassName())
                .contains(singleEntryClassClasspathProvider.getJobClassName());
        assertThat(testInstance.getJarFile()).isEmpty();
    }

    @Test
    void testMissingJobClassOnUserClasspathWithoutExplicitJobClassName() {
        assertThatThrownBy(
                        () ->
                                FromClasspathEntryClassInformationProvider.createFromClasspath(
                                        noEntryClassClasspathProvider.getURLUserClasspath()))
                .isInstanceOf(FlinkException.class);
    }

    @Test
    void testTooManyMainMethodsOnUserClasspath() {
        assertThatThrownBy(
                        () ->
                                FromClasspathEntryClassInformationProvider.createFromClasspath(
                                        multipleEntryClassesClasspathProvider
                                                .getURLUserClasspath()))
                .isInstanceOf(FlinkException.class);
    }

    @Test
    void testJobClassOnUserClasspathWithoutExplicitJobClassNameAndMissingUserClasspath() {
        assertThatThrownBy(
                        () -> FromClasspathEntryClassInformationProvider.createFromClasspath(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void testJobClassNameFromSystemClasspath() throws IOException, FlinkException {
        singleEntryClassClasspathProvider.setSystemClasspath();
        FromClasspathEntryClassInformationProvider testInstance =
                FromClasspathEntryClassInformationProvider.createFromSystemClasspath();
        assertThat(testInstance.getJobClassName())
                .contains(singleEntryClassClasspathProvider.getJobClassName());
        assertThat(testInstance.getJarFile()).isEmpty();
    }

    @Test
    void testMissingJobClassNameFromSystemClasspath() {
        assertThatThrownBy(
                        () -> {
                            noEntryClassClasspathProvider.setSystemClasspath();
                            FromClasspathEntryClassInformationProvider.createFromSystemClasspath();
                        })
                .isInstanceOf(FlinkException.class);
    }

    @Test
    void testTooManyMainMethodsOnSystemClasspath() {
        assertThatThrownBy(
                        () -> {
                            multipleEntryClassesClasspathProvider.setSystemClasspath();
                            FromClasspathEntryClassInformationProvider.createFromSystemClasspath();
                        })
                .isInstanceOf(FlinkException.class);
    }

    @Test
    void testJarFromSystemClasspathSanityCheck() {
        // Junit executes this test, so it should be returned as part of JARs on the classpath
        final Iterable<File> systemClasspath =
                FromClasspathEntryClassInformationProvider.extractSystemClasspath();
        assertThat(StreamSupport.stream(systemClasspath.spliterator(), false).map(File::getName))
                .anyMatch(s -> s.contains("junit"));
    }

    @Test
    void testJarFromSystemClasspath() throws MalformedURLException {
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
        assertThat(systemClasspath).isEqualTo(expectedContent);
    }
}
