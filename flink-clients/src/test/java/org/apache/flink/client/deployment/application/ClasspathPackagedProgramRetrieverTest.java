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

import org.apache.flink.api.common.JobID;
import org.apache.flink.client.deployment.application.ClasspathPackagedProgramRetriever.JarsOnClasspath;
import org.apache.flink.client.program.PackagedProgramRetriever;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.client.testjar.TestJob;
import org.apache.flink.client.testjar.TestJobInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.PipelineOptionsInternal;
import org.apache.flink.runtime.execution.librarycache.FlinkUserCodeClassLoaders;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Collections;

import static org.apache.flink.client.deployment.application.ClasspathPackagedProgramRetriever.JarsOnClasspath.JAVA_CLASSPATH;
import static org.apache.flink.client.deployment.application.ClasspathPackagedProgramRetriever.JarsOnClasspath.PATH_SEPARATOR;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/** Tests for the {@link ClasspathPackagedProgramRetriever}. */
public class ClasspathPackagedProgramRetrieverTest extends PackagedProgramRetrieverTestBase {

    @Test
    public void testJobGraphRetrievalFromClasspath()
            throws IOException, FlinkException, ProgramInvocationException {
        final int parallelism = 42;
        final JobID jobId = new JobID();

        final Configuration configuration = new Configuration();
        configuration.setInteger(CoreOptions.DEFAULT_PARALLELISM, parallelism);
        configuration.set(PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID, jobId.toHexString());

        final PackagedProgramRetriever retrieverUnderTest =
                PackagedProgramRetrieverAdapter.newBuilder(PROGRAM_ARGUMENTS, configuration)
                        .setJobClassName(TestJob.class.getCanonicalName())
                        .build();

        final JobGraph jobGraph = retrieveJobGraph(retrieverUnderTest, configuration);

        assertThat(jobGraph.getName(), is(equalTo(TestJob.class.getCanonicalName() + "-suffix")));
        assertThat(
                jobGraph.getSavepointRestoreSettings(),
                is(equalTo(SavepointRestoreSettings.none())));
        assertThat(jobGraph.getMaximumParallelism(), is(parallelism));
        assertEquals(jobGraph.getJobID(), jobId);
    }

    @Test
    public void testJobGraphRetrievalJobClassNameHasPrecedenceOverClasspath()
            throws IOException, FlinkException, ProgramInvocationException {
        final File testJar = new File("non-existing");

        final PackagedProgramRetriever retrieverUnderTest =
                PackagedProgramRetrieverAdapter.newBuilder(PROGRAM_ARGUMENTS, CONFIGURATION)
                        // Both a class name is specified and a JAR "is" on the class path
                        // The class name should have precedence.
                        .setJobClassName(TestJob.class.getCanonicalName())
                        .setJarsOnClasspath(() -> Collections.singleton(testJar))
                        .build();

        final JobGraph jobGraph = retrieveJobGraph(retrieverUnderTest, CONFIGURATION);

        assertThat(jobGraph.getName(), is(equalTo(TestJob.class.getCanonicalName() + "-suffix")));
    }

    @Test
    public void testJobGraphRetrievalFailIfJobDirDoesNotHaveEntryClass()
            throws IOException, ProgramInvocationException {
        final PackagedProgramRetriever retrieverUnderTest =
                PackagedProgramRetrieverAdapter.newBuilder(PROGRAM_ARGUMENTS, CONFIGURATION)
                        .setUserLibDirectory(userDirHasNotEntryClass)
                        .setJarsOnClasspath(() -> Collections.singleton(testJar))
                        .build();
        try {
            retrieveJobGraph(retrieverUnderTest, CONFIGURATION);
            Assert.fail("This case should throw exception !");
        } catch (FlinkException e) {
            assertTrue(
                    ExceptionUtils.findThrowableWithMessage(
                                    e, "Failed to find job JAR on class path")
                            .isPresent());
        }
    }

    @Test
    public void testJobGraphRetrievalFailIfDoesNotFindTheEntryClassInTheJobDir()
            throws IOException, ProgramInvocationException {
        final PackagedProgramRetriever retrieverUnderTest =
                PackagedProgramRetrieverAdapter.newBuilder(PROGRAM_ARGUMENTS, CONFIGURATION)
                        .setUserLibDirectory(userDirHasNotEntryClass)
                        .setJobClassName(TestJobInfo.JOB_CLASS)
                        .setJarsOnClasspath(Collections::emptyList)
                        .build();
        try {
            retrieveJobGraph(retrieverUnderTest, CONFIGURATION);
            Assert.fail("This case should throw class not found exception!!");
        } catch (FlinkException e) {
            assertTrue(
                    ExceptionUtils.findThrowableWithMessage(
                                    e, "Could not find the provided job class")
                            .isPresent());
        }
    }

    @Test
    public void testJarFromClasspathSupplier() throws IOException {
        final File file1 = temporaryFolder.newFile();
        final File file2 = temporaryFolder.newFile();
        final File directory = temporaryFolder.newFolder();

        // Mock java.class.path property. The empty strings are important as the shell scripts
        // that prepare the Flink class path often have such entries.
        final String classpath =
                javaClasspath(
                        "",
                        "",
                        "",
                        file1.getAbsolutePath(),
                        "",
                        directory.getAbsolutePath(),
                        "",
                        file2.getAbsolutePath(),
                        "",
                        "");

        Iterable<File> jarFiles = setClasspathAndGetJarsOnClasspath(classpath);

        assertThat(jarFiles, contains(file1, file2));
    }

    @Test
    public void testJarFromClasspathSupplierSanityCheck() {
        Iterable<File> jarFiles = JarsOnClasspath.INSTANCE.get();

        // Junit executes this test, so it should be returned as part of JARs on the class path
        assertThat(jarFiles, hasItem(hasProperty("name", containsString("junit"))));
    }

    @Test
    public void testSavepointRestoreSettings()
            throws FlinkException, IOException, ProgramInvocationException {
        final PackagedProgramRetriever retrieverUnderTest =
                PackagedProgramRetrieverAdapter.newBuilder(PROGRAM_ARGUMENTS, CONFIGURATION)
                        .setJobClassName(TestJob.class.getCanonicalName())
                        .build();
        testSavepointRestoreSettings(retrieverUnderTest);
    }

    @Test
    public void testRetrieveCorrectUserClasspathsWithoutSpecifiedEntryClass()
            throws IOException, FlinkException, ProgramInvocationException {
        final PackagedProgramRetriever retrieverUnderTest =
                PackagedProgramRetrieverAdapter.newBuilder(PROGRAM_ARGUMENTS, CONFIGURATION)
                        .setUserLibDirectory(userDirHasEntryClass)
                        .setJarsOnClasspath(Collections::emptyList)
                        .build();
        testRetrieveCorrectUserClasspathsWithoutSpecifiedEntryClass(retrieverUnderTest);
    }

    @Test
    public void testRetrieveCorrectUserClasspathsWithSpecifiedEntryClass()
            throws IOException, FlinkException, ProgramInvocationException {
        final PackagedProgramRetriever retrieverUnderTest =
                PackagedProgramRetrieverAdapter.newBuilder(PROGRAM_ARGUMENTS, CONFIGURATION)
                        .setUserLibDirectory(userDirHasEntryClass)
                        .setJobClassName(TestJobInfo.JOB_CLASS)
                        .setJarsOnClasspath(Collections::emptyList)
                        .build();
        testRetrieveCorrectUserClasspathsWithSpecifiedEntryClass(retrieverUnderTest);
    }

    @Test
    public void testGetPackagedProgramWithConfiguration() throws IOException, FlinkException {
        final Configuration configuration = new Configuration();
        configuration.setString(CoreOptions.CLASSLOADER_RESOLVE_ORDER, "parent-first");
        configuration.setBoolean(CoreOptions.CHECK_LEAKED_CLASSLOADER, false);

        final PackagedProgramRetriever retrieverUnderTest =
                PackagedProgramRetrieverAdapter.newBuilder(PROGRAM_ARGUMENTS, configuration)
                        .setJobClassName(TestJob.class.getCanonicalName())
                        .build();
        assertTrue(
                retrieverUnderTest.getPackagedProgram().getUserCodeClassLoader()
                        instanceof FlinkUserCodeClassLoaders.ParentFirstClassLoader);
    }

    private static String javaClasspath(String... entries) {
        String pathSeparator = System.getProperty(PATH_SEPARATOR);
        return String.join(pathSeparator, entries);
    }

    private static Iterable<File> setClasspathAndGetJarsOnClasspath(String classpath) {
        final String originalClasspath = System.getProperty(JAVA_CLASSPATH);
        try {
            System.setProperty(JAVA_CLASSPATH, classpath);
            return JarsOnClasspath.INSTANCE.get();
        } finally {
            // Reset property
            System.setProperty(JAVA_CLASSPATH, originalClasspath);
        }
    }
}
