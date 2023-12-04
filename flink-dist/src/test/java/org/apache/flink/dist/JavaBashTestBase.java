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

package org.apache.flink.dist;

import org.apache.flink.util.OperatingSystem;

import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.BeforeAll;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/** Abstract test class for executing bash scripts. */
abstract class JavaBashTestBase {
    @BeforeAll
    public static void checkOperatingSystem() {
        assertThat(OperatingSystem.isWindows())
                .as("This test checks shell scripts which are not available on Windows.")
                .isFalse();
    }

    /**
     * Executes the given shell script wrapper and returns its output.
     *
     * @param command command to run
     * @return raw script output
     */
    protected String executeScript(final String[] command) throws IOException {
        ProcessBuilder pb = new ProcessBuilder(command);
        pb.redirectErrorStream(true);
        Process process = pb.start();
        return IOUtils.toString(process.getInputStream());
    }
}
