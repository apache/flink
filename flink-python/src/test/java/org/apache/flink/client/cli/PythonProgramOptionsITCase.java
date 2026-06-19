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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.python.PythonOptions;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

import static org.apache.flink.python.PythonOptions.PYTHON_EXECUTABLE;
import static org.apache.flink.python.PythonOptions.PYTHON_REQUIREMENTS;
import static org.assertj.core.api.Assertions.assertThat;

/** ITCases for {@link PythonProgramOptions}. */
class PythonProgramOptionsITCase {

    /**
     * It requires setting a job jar to build a {@link PackagedProgram}, and the dummy job jar used
     * in this test case is available only after the packaging phase completed, so we make it as an
     * ITCase.
     */
    @Test
    void testConfigurePythonExecution() throws Exception {
        final String[] args = {
            "--python", "xxx.py",
            "--pyModule", "xxx",
            "--pyFiles", "/absolute/a.py,relative/b.py,relative/c.py",
            "--pyRequirements", "d.txt#e_dir",
            "--pyExecutable", "/usr/bin/python",
            "--pyArchives", "g.zip,h.zip#data,h.zip#data2",
            "userarg1", "userarg2"
        };

        final File[] dummyJobJar = {null};
        Files.walkFileTree(
                FileSystems.getDefault()
                        .getPath(System.getProperty("user.dir") + "/target/artifacts"),
                new SimpleFileVisitor<Path>() {
                    @Override
                    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                            throws IOException {
                        FileVisitResult result = super.visitFile(file, attrs);
                        if (file.getFileName().toString().startsWith("dummy")) {
                            dummyJobJar[0] = file.toFile();
                        }
                        return result;
                    }
                });

        PackagedProgram packagedProgram =
                PackagedProgram.newBuilder().setArguments(args).setJarFile(dummyJobJar[0]).build();

        Configuration configuration = new Configuration();
        ProgramOptionsUtils.configurePythonExecution(configuration, packagedProgram);

        assertThat(configuration.get(PythonOptions.PYTHON_FILES))
                .isEqualTo("/absolute/a.py,relative/b.py,relative/c.py");
        assertThat(configuration.get(PYTHON_REQUIREMENTS)).isEqualTo("d.txt#e_dir");
        assertThat(configuration.get(PythonOptions.PYTHON_ARCHIVES))
                .isEqualTo("g.zip,h.zip#data,h.zip#data2");
        assertThat(configuration.get(PYTHON_EXECUTABLE)).isEqualTo("/usr/bin/python");
        assertThat(packagedProgram.getArguments())
                .containsExactly("--python", "xxx.py", "--pyModule", "xxx", "userarg1", "userarg2");
    }
}
