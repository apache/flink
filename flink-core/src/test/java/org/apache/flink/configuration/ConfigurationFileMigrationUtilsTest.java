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

package org.apache.flink.configuration;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class ConfigurationFileMigrationUtilsTest {

    @TempDir private File tmpDir;

    @Test
    void testConfigurationWithLegacyYAML() throws FileNotFoundException {
        File confFile =
                new File(tmpDir, ConfigurationFileMigrationUtils.LEGACY_FLINK_CONF_FILENAME);
        try (PrintWriter pw = new PrintWriter(confFile)) {
            pw.println("###########################"); // should be skipped
            pw.println("# Some : comments : to skip"); // should be skipped
            pw.println("###########################"); // should be skipped
            pw.println("mykey1: myvalue1"); // OK, simple correct case
            pw.println("mykey2       : myvalue2"); // OK, whitespace before colon is correct
            pw.println("mykey3:myvalue3"); // SKIP, missing white space after colon
            pw.println(" some nonsense without colon and whitespace separator"); // SKIP
            pw.println(" :  "); // SKIP
            pw.println("   "); // SKIP (silently)
            pw.println(" "); // SKIP (silently)
            pw.println("mykey4: myvalue4# some comments"); // OK, skip comments only
            pw.println("   mykey5    :    myvalue5    "); // OK, trim unnecessary whitespace
            pw.println("mykey6: my: value6"); // OK, only use first ': ' as separator
            pw.println("mykey7: "); // SKIP, no value provided
            pw.println(": myvalue8"); // SKIP, no key provided

            pw.println("mykey9: myvalue9"); // OK
            pw.println("mykey9: myvalue10"); // OK, overwrite last value
        }

        Map<String, String> conf = ConfigurationFileMigrationUtils.loadLegacyYAMLResource(confFile);

        // all distinct keys from confFile1 + confFile2 key
        assertThat(conf.keySet()).hasSize(6);

        // keys 1, 2, 4, 5, 6, 7, 8 should be OK and match the expected values
        assertThat(conf.getOrDefault("mykey1", null)).isEqualTo("myvalue1");
        assertThat(conf.getOrDefault("mykey1", null)).isEqualTo("myvalue1");
        assertThat(conf.getOrDefault("mykey2", null)).isEqualTo("myvalue2");
        assertThat(conf.getOrDefault("mykey3", "null")).isEqualTo("null");
        assertThat(conf.getOrDefault("mykey4", null)).isEqualTo("myvalue4");
        assertThat(conf.getOrDefault("mykey5", null)).isEqualTo("myvalue5");
        assertThat(conf.getOrDefault("mykey6", null)).isEqualTo("my: value6");
        assertThat(conf.getOrDefault("mykey7", "null")).isEqualTo("null");
        assertThat(conf.getOrDefault("mykey8", "null")).isEqualTo("null");
        assertThat(conf.getOrDefault("mykey9", null)).isEqualTo("myvalue10");
    }

    @Test
    // We allow malformed YAML files if loading legacy flink conf
    void testInvalidLegacyYamlFile() throws IOException {
        final File confFile =
                new File(
                        tmpDir.getPath(),
                        ConfigurationFileMigrationUtils.LEGACY_FLINK_CONF_FILENAME);

        try (PrintWriter pw = new PrintWriter(confFile)) {
            pw.append("invalid");
        }

        assertThat(ConfigurationFileMigrationUtils.loadLegacyYAMLResource(confFile)).isNotNull();
    }
}
