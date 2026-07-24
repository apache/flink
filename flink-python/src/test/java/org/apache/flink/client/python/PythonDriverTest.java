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

package org.apache.flink.client.python;

import org.apache.flink.client.program.ProgramAbortException;
import org.apache.flink.configuration.Configuration;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import py4j.GatewayServer;

import java.io.IOException;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the {@link PythonDriver}. */
class PythonDriverTest {
    @Test
    void testStartGatewayServer() throws ExecutionException, InterruptedException {
        GatewayServer gatewayServer = PythonEnvUtils.startGatewayServer();
        try {
            Socket socket = new Socket("localhost", gatewayServer.getListeningPort());
            assert socket.isConnected();
        } catch (IOException e) {
            throw new RuntimeException("Connect Gateway Server failed");
        } finally {
            gatewayServer.shutdown();
        }
    }

    @Test
    void testCleanupTmpDirWhenPythonClientLaunchFails(@TempDir Path tmpDir) throws IOException {
        Path entryPointScript = tmpDir.resolve("entrypoint.py");
        Files.write(entryPointScript, new byte[0]);
        Path pyflinkTmpDir = tmpDir.resolve("pyflink");
        String originalTmpDir = System.getProperty("java.io.tmpdir");

        try {
            System.setProperty("java.io.tmpdir", tmpDir.toString());

            assertThatThrownBy(
                            () ->
                                    PythonDriver.main(
                                            new String[] {
                                                "-py",
                                                entryPointScript.toString(),
                                                "-pyclientexec",
                                                tmpDir.resolve("missing-python-executable")
                                                        .toString()
                                            }))
                    .isInstanceOf(ProgramAbortException.class);

            if (Files.exists(pyflinkTmpDir)) {
                try (Stream<Path> remainingTmpDirs = Files.list(pyflinkTmpDir)) {
                    assertThat(remainingTmpDirs).isEmpty();
                }
            }
        } finally {
            if (originalTmpDir == null) {
                System.clearProperty("java.io.tmpdir");
            } else {
                System.setProperty("java.io.tmpdir", originalTmpDir);
            }
        }
    }

    @Test
    void testConstructCommandsWithEntryPointModule() {
        List<String> args = new ArrayList<>();
        args.add("--input");
        args.add("in.txt");

        PythonDriverOptions pythonDriverOptions =
                new PythonDriverOptions(new Configuration(), "xxx", null, args);
        List<String> commands = PythonDriver.constructPythonCommands(pythonDriverOptions);
        // verify the generated commands
        assertThat(commands).containsExactly("-u", "-m", "xxx", "--input", "in.txt");
    }

    @Test
    void testConstructCommandsWithEntryPointScript() {
        List<String> args = new ArrayList<>();
        args.add("--input");
        args.add("in.txt");

        PythonDriverOptions pythonDriverOptions =
                new PythonDriverOptions(new Configuration(), null, "xxx.py", args);
        List<String> commands = PythonDriver.constructPythonCommands(pythonDriverOptions);
        assertThat(commands).containsExactly("-u", "xxx.py", "--input", "in.txt");
    }
}
