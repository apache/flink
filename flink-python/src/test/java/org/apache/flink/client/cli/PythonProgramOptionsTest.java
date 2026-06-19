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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.python.PythonOptions;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.apache.flink.client.cli.CliFrontendParser.PYARCHIVE_OPTION;
import static org.apache.flink.client.cli.CliFrontendParser.PYEXEC_OPTION;
import static org.apache.flink.client.cli.CliFrontendParser.PYFILES_OPTION;
import static org.apache.flink.client.cli.CliFrontendParser.PYMODULE_OPTION;
import static org.apache.flink.client.cli.CliFrontendParser.PYREQUIREMENTS_OPTION;
import static org.apache.flink.client.cli.CliFrontendParser.PYTHON_PATH;
import static org.apache.flink.client.cli.CliFrontendParser.PY_OPTION;
import static org.apache.flink.python.PythonOptions.PYTHON_EXECUTABLE;
import static org.apache.flink.python.PythonOptions.PYTHON_REQUIREMENTS;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link PythonProgramOptions}. */
class PythonProgramOptionsTest {
    private Options options;

    @BeforeEach
    void setUp() {
        options = new Options();
        options.addOption(PY_OPTION);
        options.addOption(PYFILES_OPTION);
        options.addOption(PYMODULE_OPTION);
        options.addOption(PYREQUIREMENTS_OPTION);
        options.addOption(PYARCHIVE_OPTION);
        options.addOption(PYEXEC_OPTION);
        options.addOption(PYTHON_PATH);
    }

    @Test
    void testCreateProgramOptionsWithPythonCommandLine() throws CliArgsException {
        String[] parameters = {
            "-py", "test.py",
            "-pym", "test",
            "-pyfs", "test1.py,test2.zip,test3.egg,test4_dir",
            "-pyreq", "a.txt#b_dir",
            "-pyarch", "c.zip#venv,d.zip",
            "-pyexec", "bin/python",
            "-pypath", "bin/python/lib/:bin/python/lib64",
            "userarg1", "userarg2"
        };

        CommandLine line = CliFrontendParser.parse(options, parameters, false);
        PythonProgramOptions programOptions = (PythonProgramOptions) ProgramOptions.create(line);
        Configuration config = new Configuration();
        programOptions.applyToConfiguration(config);
        assertThat(config.get(PythonOptions.PYTHON_FILES))
                .isEqualTo("test1.py,test2.zip,test3.egg,test4_dir");
        assertThat(config.get(PYTHON_REQUIREMENTS)).isEqualTo("a.txt#b_dir");
        assertThat(config.get(PythonOptions.PYTHON_ARCHIVES)).isEqualTo("c.zip#venv,d.zip");
        assertThat(config.get(PYTHON_EXECUTABLE)).isEqualTo("bin/python");
        assertThat(config.get(PythonOptions.PYTHON_PATH))
                .isEqualTo("bin/python/lib/:bin/python/lib64");
        assertThat(programOptions.getProgramArgs())
                .containsExactly(
                        "--python", "test.py", "--pyModule", "test", "userarg1", "userarg2");
    }

    @Test
    void testCreateProgramOptionsWithLongOptions() throws CliArgsException {
        String[] args = {
            "--python",
            "xxx.py",
            "--pyModule",
            "xxx",
            "--pyFiles",
            "/absolute/a.py,relative/b.py,relative/c.py",
            "--pyRequirements",
            "d.txt#e_dir",
            "--pyExecutable",
            "/usr/bin/python",
            "--pyArchives",
            "g.zip,h.zip#data,h.zip#data2",
            "--pyPythonPath",
            "bin/python/lib/:bin/python/lib64",
            "userarg1",
            "userarg2"
        };
        CommandLine line = CliFrontendParser.parse(options, args, false);
        PythonProgramOptions programOptions = (PythonProgramOptions) ProgramOptions.create(line);
        Configuration config = new Configuration();
        programOptions.applyToConfiguration(config);
        assertThat(config.get(PythonOptions.PYTHON_FILES))
                .isEqualTo("/absolute/a.py,relative/b.py,relative/c.py");
        assertThat(config.get(PYTHON_REQUIREMENTS)).isEqualTo("d.txt#e_dir");
        assertThat(config.get(PythonOptions.PYTHON_ARCHIVES))
                .isEqualTo("g.zip,h.zip#data,h.zip#data2");
        assertThat(config.get(PYTHON_EXECUTABLE)).isEqualTo("/usr/bin/python");
        assertThat(config.get(PythonOptions.PYTHON_PATH))
                .isEqualTo("bin/python/lib/:bin/python/lib64");
        assertThat(programOptions.getProgramArgs())
                .containsExactly("--python", "xxx.py", "--pyModule", "xxx", "userarg1", "userarg2");
    }
}
