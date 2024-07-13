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

package org.apache.flink.table.gateway.api.operation;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.gateway.api.utils.TestOperationValidator;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.core.testutils.CommonTestUtils.setEnv;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Test class for {@link OperationValidatorUtils}. */
public class OperationValidatorUtilsTest {

    private static final String TEST_PLUGINS = "test-plugins";
    private static final String PLUGINS_JAR = TEST_PLUGINS + "-test-jar.jar";

    @TempDir public Path tempDir;

    @Test
    public void testDiscoverValidators() throws IOException {
        Map<String, String> originalEnv = System.getenv();
        Map<String, String> newEnv = new HashMap<>(originalEnv);
        try {
            final File testPluginsFolder = new File(tempDir.toFile(), TEST_PLUGINS);
            assertTrue(testPluginsFolder.mkdirs(), "Failed to create test validator folder.");

            final File testValidatorJar = new File("target", PLUGINS_JAR);
            assertTrue(testValidatorJar.exists(), "Test validator JAR file does not exist.");

            Files.copy(
                    testValidatorJar.toPath(),
                    Paths.get(testPluginsFolder.toString(), PLUGINS_JAR));

            newEnv.put(ConfigConstants.ENV_FLINK_PLUGINS_DIR, tempDir.toAbsolutePath().toString());
            setEnv(newEnv);

            final Set<OperationValidator> discoveredValidators =
                    OperationValidatorUtils.discoverOperationValidators(new Configuration());

            assertEquals(
                    TestOperationValidator.class.getName(),
                    discoveredValidators.iterator().next().getClass().getName(),
                    "Discovered validator class name does not match the expected class name.");
        } finally {
            setEnv(originalEnv);
        }
    }
}
