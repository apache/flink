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

package org.apache.flink.client.program;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.client.deployment.application.EntryClassInformationProvider;
import org.apache.flink.client.deployment.executors.PipelineExecutorUtils;
import org.apache.flink.client.testjar.ClasspathProviderExtension;
import org.apache.flink.configuration.ConfigUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.PipelineOptionsInternal;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.util.ChildFirstClassLoader;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.FlinkUserCodeClassLoaders;
import org.apache.flink.util.Preconditions;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** {@code PackagedProgramRetrieverImplTest} tests {@link DefaultPackagedProgramRetriever}. */
class DefaultPackagedProgramRetrieverTest {

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
    void testDeriveEntryClassInformationForCustomJar()
            throws FlinkException, MalformedURLException {
        // clearing the system classpath to make sure that no data is collected from there
        noEntryClassClasspathProvider.setSystemClasspath();

        final String jobClassName = "SomeJobClassName";
        final File jarFile = new File("some/jar/file.jar");
        final EntryClassInformationProvider informationProvider =
                DefaultPackagedProgramRetriever.createEntryClassInformationProvider(
                        null, jarFile, jobClassName, new String[0]);
        assertThat(informationProvider.getJobClassName()).get().isEqualTo(jobClassName);
        assertThat(informationProvider.getJarFile()).get().isEqualTo(jarFile);
    }

    @Test
    void testDeriveEntryClassInformationFromSystemClasspathWithNonExistingJobClassName()
            throws IOException, FlinkException {
        // this test succeeds even though we could make the code fail early of we start validating
        // the existing of the passed Java class on the system classpath analogously to what is done
        // for the user classpath
        singleEntryClassClasspathProvider.setSystemClasspath();

        final String jobClassName = "SomeJobClassNotBeingOnTheSystemClasspath";
        final EntryClassInformationProvider informationProvider =
                DefaultPackagedProgramRetriever.createEntryClassInformationProvider(
                        null, null, jobClassName, new String[0]);
        assertThat(informationProvider.getJobClassName()).get().isEqualTo(jobClassName);
        assertThat(informationProvider.getJarFile()).isEmpty();
    }

    @Test
    void testDeriveEntryClassInformationFromSystemClasspathWithExistingJobClassName()
            throws IOException, FlinkException {
        singleEntryClassClasspathProvider.setSystemClasspath();

        final EntryClassInformationProvider informationProvider =
                DefaultPackagedProgramRetriever.createEntryClassInformationProvider(
                        null,
                        null,
                        singleEntryClassClasspathProvider.getJobClassName(),
                        new String[0]);
        assertThat(informationProvider.getJobClassName())
                .get()
                .isEqualTo(singleEntryClassClasspathProvider.getJobClassName());
        assertThat(informationProvider.getJarFile()).isEmpty();
    }

    @Test
    void testDeriveEntryClassInformationFromSystemClasspathExtractingTheJobClassFromThere()
            throws IOException, FlinkException {
        singleEntryClassClasspathProvider.setSystemClasspath();

        final EntryClassInformationProvider informationProvider =
                DefaultPackagedProgramRetriever.createEntryClassInformationProvider(
                        null, null, null, new String[0]);
        assertThat(informationProvider.getJobClassName())
                .get()
                .isEqualTo(singleEntryClassClasspathProvider.getJobClassName());
        assertThat(informationProvider.getJarFile()).isEmpty();
    }

    @Test
    void testDeriveEntryClassInformationFromClasspathWithJobClass()
            throws IOException, FlinkException {
        final EntryClassInformationProvider informationProvider =
                DefaultPackagedProgramRetriever.createEntryClassInformationProvider(
                        multipleEntryClassesClasspathProvider.getURLUserClasspath(),
                        null,
                        // we have to specify the job class - otherwise the call would fail due to
                        // two main method being present
                        multipleEntryClassesClasspathProvider.getJobClassName(),
                        new String[0]);
        assertThat(informationProvider.getJobClassName())
                .get()
                .isEqualTo(multipleEntryClassesClasspathProvider.getJobClassName());
        assertThat(informationProvider.getJarFile()).isEmpty();
    }

    @Test
    void testDeriveEntryClassInformationFromClasspathWithNoJobClass()
            throws IOException, FlinkException {
        final EntryClassInformationProvider informationProvider =
                DefaultPackagedProgramRetriever.createEntryClassInformationProvider(
                        singleEntryClassClasspathProvider.getURLUserClasspath(),
                        null,
                        // no job class name is specified which enables looking for the entry class
                        // on the user classpath
                        null,
                        new String[0]);
        assertThat(informationProvider.getJobClassName())
                .get()
                .isEqualTo(singleEntryClassClasspathProvider.getJobClassName());
        assertThat(informationProvider.getJarFile()).isEmpty();
    }

    @Test
    void testCreateWithUserLibDir() throws FlinkException {
        final PackagedProgramRetriever retriever =
                DefaultPackagedProgramRetriever.create(
                        singleEntryClassClasspathProvider.getDirectory(),
                        null,
                        singleEntryClassClasspathProvider.getJobClassName(),
                        new String[0],
                        new Configuration());

        // the right information is picked up without any error
        assertThat(retriever.getPackagedProgram().getMainClassName())
                .isEqualTo(singleEntryClassClasspathProvider.getJobClassName());
    }

    @Test
    void testJobGraphRetrieval() throws IOException, FlinkException, ProgramInvocationException {
        final int parallelism = 42;
        final JobID jobId = new JobID();

        final Configuration configuration = new Configuration();
        configuration.setInteger(CoreOptions.DEFAULT_PARALLELISM, parallelism);
        configuration.set(PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID, jobId.toHexString());

        final String expectedSuffix = "suffix";
        final PackagedProgramRetriever retriever =
                DefaultPackagedProgramRetriever.create(
                        null,
                        testJobEntryClassClasspathProvider.getJobClassName(),
                        ClasspathProviderExtension.parametersForTestJob(expectedSuffix),
                        new Configuration());

        final JobGraph jobGraph = retrieveJobGraph(retriever, configuration);

        assertThat(jobGraph.getName())
                .isEqualTo(
                        testJobEntryClassClasspathProvider.getJobClassName()
                                + "-"
                                + expectedSuffix);
        assertThat(jobGraph.getSavepointRestoreSettings())
                .isEqualTo(SavepointRestoreSettings.none());
        assertThat(jobGraph.getMaximumParallelism()).isEqualTo(parallelism);
        assertThat(jobGraph.getJobID()).isEqualTo(jobId);
    }

    @Test
    void testJobGraphRetrievalFromJar()
            throws IOException, FlinkException, ProgramInvocationException {
        final String expectedSuffix = "suffix";
        final PackagedProgramRetriever retrieverUnderTest =
                DefaultPackagedProgramRetriever.create(
                        testJobEntryClassClasspathProvider.getDirectory(),
                        null,
                        null,
                        ClasspathProviderExtension.parametersForTestJob(expectedSuffix),
                        new Configuration());

        final JobGraph jobGraph = retrieveJobGraph(retrieverUnderTest, new Configuration());

        assertThat(jobGraph.getName())
                .isEqualTo(
                        testJobEntryClassClasspathProvider.getJobClassName()
                                + "-"
                                + expectedSuffix);
    }

    @Test
    void testParameterConsiderationForMultipleJobsOnSystemClasspath()
            throws IOException, FlinkException, ProgramInvocationException {
        final String expectedSuffix = "suffix";
        final PackagedProgramRetriever retrieverUnderTest =
                // Both a class name is specified and a JAR "is" on the class path
                // The class name should have precedence.
                DefaultPackagedProgramRetriever.create(
                        null,
                        testJobEntryClassClasspathProvider.getJobClassName(),
                        ClasspathProviderExtension.parametersForTestJob(expectedSuffix),
                        new Configuration());

        final JobGraph jobGraph = retrieveJobGraph(retrieverUnderTest, new Configuration());

        assertThat(jobGraph.getName())
                .isEqualTo(testJobEntryClassClasspathProvider.getJobClassName() + "-suffix");
    }

    @Test
    void testSavepointRestoreSettings()
            throws FlinkException, IOException, ProgramInvocationException {
        final Configuration configuration = new Configuration();
        final SavepointRestoreSettings savepointRestoreSettings =
                SavepointRestoreSettings.forPath("foobar", true);
        final JobID jobId = new JobID();

        configuration.setString(PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID, jobId.toHexString());
        SavepointRestoreSettings.toConfiguration(savepointRestoreSettings, configuration);

        final String expectedSuffix = "suffix";
        final PackagedProgramRetriever retrieverUnderTest =
                DefaultPackagedProgramRetriever.create(
                        null,
                        testJobEntryClassClasspathProvider.getJobClassName(),
                        ClasspathProviderExtension.parametersForTestJob(expectedSuffix),
                        new Configuration());

        final JobGraph jobGraph = retrieveJobGraph(retrieverUnderTest, configuration);

        assertThat(jobGraph.getSavepointRestoreSettings()).isEqualTo(savepointRestoreSettings);
        assertThat(jobGraph.getJobID()).isEqualTo(jobId);
    }

    @Test
    void testEntryClassNotFoundOnSystemClasspath() {
        assertThatThrownBy(
                        () -> {
                            final PackagedProgramRetriever testInstance =
                                    DefaultPackagedProgramRetriever.create(
                                            null,
                                            "NotExistingClass",
                                            new String[0],
                                            new Configuration());
                            // the getPackagedProgram fails do to the missing class. We could make
                            // it fail earlier by
                            // validating the existence of the passed Java class on the system
                            // classpath (analogously to
                            // what we already do for the user classpath)
                            // see
                            // testDeriveEntryClassInformationFromSystemClasspathWithNonExistingJobClassName
                            testInstance.getPackagedProgram();
                        })
                .isInstanceOf(FlinkException.class);
    }

    @Test
    void testEntryClassNotFoundOnUserClasspath() {
        final String jobClassName = "NotExistingClass";
        assertThatThrownBy(
                        () ->
                                DefaultPackagedProgramRetriever.create(
                                                noEntryClassClasspathProvider.getDirectory(),
                                                jobClassName,
                                                new String[0],
                                                new Configuration())
                                        .getPackagedProgram())
                .isInstanceOf(FlinkException.class)
                .hasMessageContaining("Could not load the provided entrypoint class.")
                .cause()
                .hasMessageContaining(
                        "The program's entry point class '%s' was not found in the jar file.",
                        jobClassName);
    }

    @Test
    void testWithoutJobClassAndMultipleEntryClassesOnUserClasspath() {
        assertThatThrownBy(
                        () -> {
                            // without a job class name specified deriving the entry class from
                            // classpath is impossible
                            // if the classpath contains multiple classes with main methods
                            DefaultPackagedProgramRetriever.create(
                                    multipleEntryClassesClasspathProvider.getDirectory(),
                                    null,
                                    new String[0],
                                    new Configuration());
                        })
                .isInstanceOf(FlinkException.class);
    }

    @Test
    void testWithoutJobClassAndMultipleEntryClassesOnSystemClasspath() {
        assertThatThrownBy(
                        () ->
                                DefaultPackagedProgramRetriever.create(
                                        null, null, new String[0], new Configuration()))
                .isInstanceOf(FlinkException.class);
    }

    @Test
    void testWithJobClassAndMultipleEntryClassesOnUserClasspath() throws FlinkException {
        final DefaultPackagedProgramRetriever retriever =
                DefaultPackagedProgramRetriever.create(
                        multipleEntryClassesClasspathProvider.getDirectory(),
                        multipleEntryClassesClasspathProvider.getJobClassName(),
                        new String[0],
                        new Configuration());
        assertThat(retriever.getPackagedProgram().getMainClassName())
                .isEqualTo(multipleEntryClassesClasspathProvider.getJobClassName());
    }

    @Test
    void testWithJobClassAndMultipleEntryClassesOnSystemClasspath()
            throws FlinkException, MalformedURLException {
        multipleEntryClassesClasspathProvider.setSystemClasspath();

        final DefaultPackagedProgramRetriever retriever =
                DefaultPackagedProgramRetriever.create(
                        null,
                        multipleEntryClassesClasspathProvider.getJobClassName(),
                        new String[0],
                        new Configuration());
        assertThat(retriever.getPackagedProgram().getMainClassName())
                .isEqualTo(multipleEntryClassesClasspathProvider.getJobClassName());
    }

    @Test
    void testRetrieveCorrectUserClasspathsWithoutSpecifiedEntryClass()
            throws IOException, FlinkException, ProgramInvocationException {
        final PackagedProgramRetriever retrieverUnderTest =
                DefaultPackagedProgramRetriever.create(
                        singleEntryClassClasspathProvider.getDirectory(),
                        null,
                        ClasspathProviderExtension.parametersForTestJob("suffix"),
                        new Configuration());
        final JobGraph jobGraph = retrieveJobGraph(retrieverUnderTest, new Configuration());
        final List<String> actualClasspath =
                jobGraph.getClasspaths().stream().map(URL::toString).collect(Collectors.toList());

        final List<String> expectedClasspath =
                extractRelativizedURLsForJarsFromDirectory(
                        singleEntryClassClasspathProvider.getDirectory());

        assertThat(actualClasspath).isEqualTo(expectedClasspath);
    }

    @Test
    void testRetrieveCorrectUserClasspathsWithSpecifiedEntryClass()
            throws IOException, FlinkException, ProgramInvocationException {
        final PackagedProgramRetriever retrieverUnderTest =
                DefaultPackagedProgramRetriever.create(
                        singleEntryClassClasspathProvider.getDirectory(),
                        singleEntryClassClasspathProvider.getJobClassName(),
                        ClasspathProviderExtension.parametersForTestJob("suffix"),
                        new Configuration());
        final JobGraph jobGraph = retrieveJobGraph(retrieverUnderTest, new Configuration());
        final List<String> actualClasspath =
                jobGraph.getClasspaths().stream().map(URL::toString).collect(Collectors.toList());

        final List<String> expectedClasspath =
                extractRelativizedURLsForJarsFromDirectory(
                        singleEntryClassClasspathProvider.getDirectory());

        assertThat(actualClasspath).isEqualTo(expectedClasspath);
    }

    @Test
    void testRetrieveCorrectUserClasspathsWithPipelineClasspaths() throws Exception {
        final Configuration configuration = new Configuration();
        final List<String> pipelineJars = new ArrayList<>();
        final List<URL> expectedMergedURLs = new ArrayList<>();
        for (URL jarFile : singleEntryClassClasspathProvider.getURLUserClasspath()) {
            pipelineJars.add(jarFile.toString());
            expectedMergedURLs.add(jarFile);
        }
        configuration.set(PipelineOptions.CLASSPATHS, pipelineJars);

        final PackagedProgramRetriever retrieverUnderTest =
                DefaultPackagedProgramRetriever.create(
                        null,
                        singleEntryClassClasspathProvider.getJobClassName(),
                        ClasspathProviderExtension.parametersForTestJob("suffix"),
                        configuration);
        final JobGraph jobGraph = retrieveJobGraph(retrieverUnderTest, new Configuration());
        assertThat(jobGraph.getClasspaths()).isEqualTo(expectedMergedURLs);
    }

    @Test
    void testRetrieveFromJarFileWithoutUserLib()
            throws IOException, FlinkException, ProgramInvocationException {
        final PackagedProgramRetriever retrieverUnderTest =
                DefaultPackagedProgramRetriever.create(
                        null,
                        testJobEntryClassClasspathProvider.getJobJar(),
                        null,
                        ClasspathProviderExtension.parametersForTestJob("suffix"),
                        new Configuration());
        final JobGraph jobGraph = retrieveJobGraph(retrieverUnderTest, new Configuration());

        assertThat(jobGraph.getUserJars())
                .contains(
                        new org.apache.flink.core.fs.Path(
                                testJobEntryClassClasspathProvider.getJobJar().toURI()));
        assertThat(jobGraph.getClasspaths()).isEmpty();
    }

    @Test
    void testRetrieveFromJarFileWithUserLib()
            throws IOException, FlinkException, ProgramInvocationException {
        final PackagedProgramRetriever retrieverUnderTest =
                DefaultPackagedProgramRetriever.create(
                        singleEntryClassClasspathProvider.getDirectory(),
                        // the testJob jar is not on the user classpath
                        testJobEntryClassClasspathProvider.getJobJar(),
                        null,
                        ClasspathProviderExtension.parametersForTestJob("suffix"),
                        new Configuration());
        final JobGraph jobGraph = retrieveJobGraph(retrieverUnderTest, new Configuration());

        assertThat(jobGraph.getUserJars())
                .contains(
                        new org.apache.flink.core.fs.Path(
                                testJobEntryClassClasspathProvider.getJobJar().toURI()));
        final List<String> actualClasspath =
                jobGraph.getClasspaths().stream().map(URL::toString).collect(Collectors.toList());
        final List<String> expectedClasspath =
                extractRelativizedURLsForJarsFromDirectory(
                        singleEntryClassClasspathProvider.getDirectory());

        assertThat(actualClasspath).isEqualTo(expectedClasspath);
    }

    @Test
    void testChildFirstDefaultConfiguration() throws FlinkException {
        // this is a sanity check to backup testConfigurationIsConsidered
        final Configuration configuration = new Configuration();
        // CHECK_LEAKED_CLASSLOADER has to be disabled to enable the instanceof check later on in
        // this test. Otherwise, the actual instance would be hidden by a wrapper
        configuration.set(CoreOptions.CHECK_LEAKED_CLASSLOADER, false);

        final PackagedProgramRetriever retriever =
                DefaultPackagedProgramRetriever.create(
                        singleEntryClassClasspathProvider.getDirectory(),
                        null,
                        singleEntryClassClasspathProvider.getJobClassName(),
                        new String[0],
                        configuration);

        assertThat(retriever.getPackagedProgram().getUserCodeClassLoader())
                .isInstanceOf(ChildFirstClassLoader.class);
    }

    @Test
    void testConfigurationIsConsidered() throws FlinkException {
        final String parentFirstConfigValue = "parent-first";
        // we want to make sure that parent-first is not set as a default
        assertThat(CoreOptions.CLASSLOADER_RESOLVE_ORDER.defaultValue())
                .isNotEqualTo(parentFirstConfigValue);

        final Configuration configuration = new Configuration();
        configuration.set(CoreOptions.CLASSLOADER_RESOLVE_ORDER, parentFirstConfigValue);
        // CHECK_LEAKED_CLASSLOADER has to be disabled to enable the instanceof check later on in
        // this test. Otherwise, the actual instance would be hidden by a wrapper
        configuration.set(CoreOptions.CHECK_LEAKED_CLASSLOADER, false);

        final PackagedProgramRetriever retriever =
                DefaultPackagedProgramRetriever.create(
                        singleEntryClassClasspathProvider.getDirectory(),
                        null,
                        singleEntryClassClasspathProvider.getJobClassName(),
                        new String[0],
                        configuration);

        assertThat(retriever.getPackagedProgram().getUserCodeClassLoader())
                .isInstanceOf(FlinkUserCodeClassLoaders.ParentFirstClassLoader.class);
    }

    private JobGraph retrieveJobGraph(
            PackagedProgramRetriever retrieverUnderTest, Configuration configuration)
            throws FlinkException, ProgramInvocationException, MalformedURLException {
        final PackagedProgram packagedProgram = retrieverUnderTest.getPackagedProgram();

        final int defaultParallelism = configuration.getInteger(CoreOptions.DEFAULT_PARALLELISM);
        ConfigUtils.encodeCollectionToConfig(
                configuration,
                PipelineOptions.JARS,
                packagedProgram.getJobJarAndDependencies(),
                URL::toString);
        ConfigUtils.encodeCollectionToConfig(
                configuration,
                PipelineOptions.CLASSPATHS,
                packagedProgram.getClasspaths(),
                URL::toString);

        final Pipeline pipeline =
                PackagedProgramUtils.getPipelineFromProgram(
                        packagedProgram, configuration, defaultParallelism, false);
        return PipelineExecutorUtils.getJobGraph(
                pipeline, configuration, packagedProgram.getUserCodeClassLoader());
    }

    private static List<String> extractRelativizedURLsForJarsFromDirectory(File directory)
            throws MalformedURLException {
        Preconditions.checkArgument(
                directory.listFiles() != null,
                "The passed File does not seem to be a directory or is not accessible: "
                        + directory.getAbsolutePath());

        final List<String> relativizedURLs = new ArrayList<>();
        final Path workingDirectory = FileUtils.getCurrentWorkingDirectory();
        for (File file : Preconditions.checkNotNull(directory.listFiles())) {
            if (!FileUtils.isJarFile(file.toPath())) {
                // any non-JARs are filtered by PackagedProgramRetrieverImpl
                continue;
            }

            Path relativePath = FileUtils.relativizePath(workingDirectory, file.toPath());
            relativizedURLs.add(FileUtils.toURL(relativePath).toString());
        }

        return relativizedURLs;
    }
}
