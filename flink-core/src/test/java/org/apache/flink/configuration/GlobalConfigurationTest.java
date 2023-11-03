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

import org.junit.Rule;
import org.junit.jupiter.api.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * This class contains tests for the global configuration (parsing configuration directory
 * information).
 */
public class GlobalConfigurationTest {

    @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

    @Test
    void testConfigurationYAML() {
        File tmpDir = tempFolder.getRoot();
        File confFile = new File(tmpDir, GlobalConfiguration.FLINK_CONF_FILENAME);

        try {
            try (final PrintWriter pw = new PrintWriter(confFile)) {

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

            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }

            Configuration conf = GlobalConfiguration.loadConfiguration(tmpDir.getAbsolutePath());

            // all distinct keys from confFile1 + confFile2 key
            assertThat(conf.keySet().size()).isEqualTo(6);

            // keys 1, 2, 4, 5, 6, 7, 8 should be OK and match the expected values
            assertThat(conf.getString("mykey1", null)).isEqualTo("myvalue1");
            assertThat(conf.getString("mykey2", null)).isEqualTo("myvalue2");
            assertThat(conf.getString("mykey3", "null")).isEqualTo("null");
            assertThat(conf.getString("mykey4", null)).isEqualTo("myvalue4");
            assertThat(conf.getString("mykey5", null)).isEqualTo("myvalue5");
            assertThat(conf.getString("mykey6", null)).isEqualTo("my: value6");
            assertThat(conf.getString("mykey7", "null")).isEqualTo("null");
            assertThat(conf.getString("mykey8", "null")).isEqualTo("null");
            assertThat(conf.getString("mykey9", null)).isEqualTo("myvalue10");
        } finally {
            confFile.delete();
            tmpDir.delete();
        }
    }

    @Test
    public void testFailIfNull() {
        assertThatThrownBy(() -> GlobalConfiguration.loadConfiguration((String) null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testFailIfNotLoaded() {
        assertThatThrownBy(
                        () ->
                                GlobalConfiguration.loadConfiguration(
                                        "/some/path/" + UUID.randomUUID()))
                .isInstanceOf(IllegalConfigurationException.class);
    }

    @Test
    public void testInvalidConfiguration() throws IOException {
        assertThatThrownBy(
                        () ->
                                GlobalConfiguration.loadConfiguration(
                                        tempFolder.getRoot().getAbsolutePath()))
                .isInstanceOf(IllegalConfigurationException.class);
    }

    @Test
    // We allow malformed YAML files
    public void testInvalidYamlFile() throws IOException {
        final File confFile = tempFolder.newFile(GlobalConfiguration.FLINK_CONF_FILENAME);

        try (PrintWriter pw = new PrintWriter(confFile)) {
            pw.append("invalid");
        }

        assertThat(GlobalConfiguration.loadConfiguration(tempFolder.getRoot().getAbsolutePath()))
                .isNotNull();
    }

    @Test
    void testHiddenKey() {
        assertThat(GlobalConfiguration.isSensitive("password123")).isTrue();
        assertThat(GlobalConfiguration.isSensitive("123pasSword")).isTrue();
        assertThat(GlobalConfiguration.isSensitive("PasSword")).isTrue();
        assertThat(GlobalConfiguration.isSensitive("Secret")).isTrue();
        assertThat(GlobalConfiguration.isSensitive("polaris.client-secret"));
        assertTrue(GlobalConfiguration.isSensitive("client-secret"));
        assertTrue(GlobalConfiguration.isSensitive("service-key-json"));
        assertTrue(GlobalConfiguration.isSensitive("auth.basic.password"));
        assertTrue(GlobalConfiguration.isSensitive("auth.basic.token"));
        assertTrue(GlobalConfiguration.isSensitive("avro-confluent.basic-auth.user-info"));
        assertTrue(GlobalConfiguration.isSensitive("key.avro-confluent.basic-auth.user-info"));
        assertTrue(GlobalConfiguration.isSensitive("value.avro-confluent.basic-auth.user-info"));
        assertTrue(GlobalConfiguration.isSensitive("kafka.jaas.config"));
        assertTrue(GlobalConfiguration.isSensitive("properties.ssl.truststore.password"));
        assertTrue(GlobalConfiguration.isSensitive("properties.ssl.keystore.password"));

        assertTrue(
                        GlobalConfiguration.isSensitive(
                                "fs.azure.account.key.storageaccount123456.core.windows.net"))
                .isTrue();
        assertThat(GlobalConfiguration.isSensitive("Hello")).isFalse();
        assertThat(GlobalConfiguration.isSensitive("metrics.reporter.dghttp.apikey")).isTrue();
    }
}
