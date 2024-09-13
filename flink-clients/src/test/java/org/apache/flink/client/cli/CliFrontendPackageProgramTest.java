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

package org.apache.flink.client.cli;

import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;

import org.apache.commons.cli.CommandLine;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.FileNotFoundException;
import java.net.URL;
import java.util.Collections;

import static org.apache.flink.client.cli.CliFrontendTestUtils.TEST_JAR_MAIN_CLASS;
import static org.apache.flink.client.cli.CliFrontendTestUtils.getNonJarFilePath;
import static org.apache.flink.client.cli.CliFrontendTestUtils.getTestJarPath;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests for the RUN command with {@link PackagedProgram PackagedPrograms}. */
class CliFrontendPackageProgramTest {

    private CliFrontend frontend;

    @BeforeAll
    static void init() {
        CliFrontendTestUtils.pipeSystemOutToNull();
    }

    @AfterAll
    static void shutdown() {
        CliFrontendTestUtils.restoreSystemOut();
    }

    @BeforeEach
    void setup() {
        final Configuration configuration = new Configuration();
        frontend = new CliFrontend(configuration, Collections.singletonList(new DefaultCLI()));
    }

    @Test
    void testNonExistingJarFile() {
        ProgramOptions programOptions = mock(ProgramOptions.class);
        when(programOptions.getJarFilePath()).thenReturn("/some/none/existing/path");

        assertThatThrownBy(() -> frontend.buildProgram(programOptions))
                .isInstanceOf(FileNotFoundException.class);
    }

    @Test
    void testFileNotJarFile() {
        ProgramOptions programOptions = mock(ProgramOptions.class);
        when(programOptions.getJarFilePath()).thenReturn(getNonJarFilePath());
        when(programOptions.getProgramArgs()).thenReturn(new String[0]);
        when(programOptions.getSavepointRestoreSettings())
                .thenReturn(SavepointRestoreSettings.none());

        assertThatThrownBy(() -> frontend.buildProgram(programOptions))
                .isInstanceOf(ProgramInvocationException.class);
    }

    @Test
    void testVariantWithExplicitJarAndArgumentsOption() throws Exception {
        String[] arguments = {
            "--classpath",
            "file:///tmp/foo",
            "--classpath",
            "file:///tmp/bar",
            "-j",
            getTestJarPath(),
            "-a",
            "--debug",
            "true",
            "arg1",
            "arg2"
        };
        URL[] classpath = new URL[] {new URL("file:///tmp/foo"), new URL("file:///tmp/bar")};
        String[] reducedArguments = new String[] {"--debug", "true", "arg1", "arg2"};

        CommandLine commandLine =
                CliFrontendParser.parse(CliFrontendParser.RUN_OPTIONS, arguments, true);
        ProgramOptions programOptions = ProgramOptions.create(commandLine);

        assertThat(programOptions.getJarFilePath()).isEqualTo(getTestJarPath());
        assertThat(programOptions.getClasspaths().toArray()).isEqualTo(classpath);
        assertThat(programOptions.getProgramArgs()).isEqualTo(reducedArguments);

        PackagedProgram prog = frontend.buildProgram(programOptions);

        assertThat(prog.getArguments()).isEqualTo(reducedArguments);
        assertThat(prog.getMainClassName()).isEqualTo(TEST_JAR_MAIN_CLASS);
    }

    @Test
    void testVariantWithExplicitJarAndNoArgumentsOption() throws Exception {
        String[] arguments = {
            "--classpath",
            "file:///tmp/foo",
            "--classpath",
            "file:///tmp/bar",
            "-j",
            getTestJarPath(),
            "--debug",
            "true",
            "arg1",
            "arg2"
        };
        URL[] classpath = new URL[] {new URL("file:///tmp/foo"), new URL("file:///tmp/bar")};
        String[] reducedArguments = new String[] {"--debug", "true", "arg1", "arg2"};

        CommandLine commandLine =
                CliFrontendParser.parse(CliFrontendParser.RUN_OPTIONS, arguments, true);
        ProgramOptions programOptions = ProgramOptions.create(commandLine);

        assertThat(programOptions.getJarFilePath()).isEqualTo(getTestJarPath());
        assertThat(programOptions.getClasspaths().toArray()).isEqualTo(classpath);
        assertThat(programOptions.getProgramArgs()).isEqualTo(reducedArguments);

        PackagedProgram prog = frontend.buildProgram(programOptions);

        assertThat(prog.getArguments()).isEqualTo(reducedArguments);
        assertThat(prog.getMainClassName()).isEqualTo(TEST_JAR_MAIN_CLASS);
    }

    @Test
    void testValidVariantWithNoJarAndNoArgumentsOption() throws Exception {
        String[] arguments = {
            "--classpath",
            "file:///tmp/foo",
            "--classpath",
            "file:///tmp/bar",
            getTestJarPath(),
            "--debug",
            "true",
            "arg1",
            "arg2"
        };
        URL[] classpath = new URL[] {new URL("file:///tmp/foo"), new URL("file:///tmp/bar")};
        String[] reducedArguments = {"--debug", "true", "arg1", "arg2"};

        CommandLine commandLine =
                CliFrontendParser.parse(CliFrontendParser.RUN_OPTIONS, arguments, true);
        ProgramOptions programOptions = ProgramOptions.create(commandLine);

        assertThat(programOptions.getJarFilePath()).isEqualTo(getTestJarPath());
        assertThat(programOptions.getClasspaths().toArray()).isEqualTo(classpath);
        assertThat(programOptions.getProgramArgs()).isEqualTo(reducedArguments);

        PackagedProgram prog = frontend.buildProgram(programOptions);

        assertThat(prog.getArguments()).isEqualTo(reducedArguments);
        assertThat(prog.getMainClassName()).isEqualTo(TEST_JAR_MAIN_CLASS);
    }

    @Test
    void testNoJarNoArgumentsAtAll() {
        assertThatThrownBy(() -> frontend.run(new String[0])).isInstanceOf(CliArgsException.class);
    }

    @Test
    void testNonExistingFileWithArguments() throws Exception {
        String[] arguments = {
            "--classpath",
            "file:///tmp/foo",
            "--classpath",
            "file:///tmp/bar",
            "/some/none/existing/path",
            "--debug",
            "true",
            "arg1",
            "arg2"
        };
        URL[] classpath = new URL[] {new URL("file:///tmp/foo"), new URL("file:///tmp/bar")};
        String[] reducedArguments = {"--debug", "true", "arg1", "arg2"};

        CommandLine commandLine =
                CliFrontendParser.parse(CliFrontendParser.RUN_OPTIONS, arguments, true);
        ProgramOptions programOptions = ProgramOptions.create(commandLine);

        assertThat(programOptions.getJarFilePath()).isEqualTo(arguments[4]);
        assertThat(programOptions.getClasspaths().toArray()).isEqualTo(classpath);
        assertThat(programOptions.getProgramArgs()).isEqualTo(reducedArguments);

        assertThatThrownBy(() -> frontend.buildProgram(programOptions))
                .isInstanceOf(FileNotFoundException.class);
    }

    @Test
    void testNonExistingFileWithoutArguments() throws Exception {
        String[] arguments = {"/some/none/existing/path"};

        CommandLine commandLine =
                CliFrontendParser.parse(CliFrontendParser.RUN_OPTIONS, arguments, true);
        ProgramOptions programOptions = ProgramOptions.create(commandLine);

        assertThat(programOptions.getJarFilePath()).isEqualTo(arguments[0]);
        assertThat(programOptions.getProgramArgs()).isEqualTo(new String[0]);

        try {
            frontend.buildProgram(programOptions);
        } catch (FileNotFoundException e) {
            // that's what we want
        }
    }
}
