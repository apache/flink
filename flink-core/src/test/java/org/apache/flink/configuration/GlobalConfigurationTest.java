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
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * This class contains tests for the global configuration (parsing configuration directory
 * information).
 */
class GlobalConfigurationTest {

    @TempDir private File tmpDir;

    @Test
    void testLegacyConfigurationYAML() {
        File confFile = new File(tmpDir, GlobalConfiguration.FLINK_CONF_FILENAME);

        try {
            try (final PrintWriter pw = new PrintWriter(confFile)) {

                pw.println("###########################"); // should be skipped
                pw.println("a: "); // should be skipped
                pw.println(" - b "); // should be skipped
                pw.println(" - b2 "); // should be skipped
                pw.println(" - b3 "); // should be skipped

            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }

            Configuration conf = GlobalConfiguration.loadConfiguration(tmpDir.getAbsolutePath());
            ConfigOption<List<String>> option =
                    ConfigOptions.key("a").stringType().asList().noDefaultValue();
            List<String> strings = conf.get(option);
            System.out.println(strings);
            // all distinct keys from confFile1 + confFile2 key
            //            assertThat(conf.keySet()).hasSize(6);
            //
            //            // keys 1, 2, 4, 5, 6, 7, 8 should be OK and match the expected values
            //            assertThat(conf.getString("mykey1", null)).isEqualTo("myvalue1");
            //            assertThat(conf.getString("mykey1", null)).isEqualTo("myvalue1");
            //            assertThat(conf.getString("mykey2", null)).isEqualTo("myvalue2");
            //            assertThat(conf.getString("mykey3", "null")).isEqualTo("null");
            //            assertThat(conf.getString("mykey4", null)).isEqualTo("myvalue4");
            //            assertThat(conf.getString("mykey5", null)).isEqualTo("myvalue5");
            //            assertThat(conf.getString("mykey6", null)).isEqualTo("my: value6");
            //            assertThat(conf.getString("mykey7", "null")).isEqualTo("null");
            //            assertThat(conf.getString("mykey8", "null")).isEqualTo("null");
            //            assertThat(conf.getString("mykey9", null)).isEqualTo("myvalue10");
        } finally {
            confFile.delete();
            tmpDir.delete();
        }
    }

    @Test
    void testFailIfNull() {
        assertThatThrownBy(() -> GlobalConfiguration.loadConfiguration((String) null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testFailIfNotLoaded() {
        assertThatThrownBy(
                        () ->
                                GlobalConfiguration.loadConfiguration(
                                        "/some/path/" + UUID.randomUUID()))
                .isInstanceOf(IllegalConfigurationException.class);
    }

    @Test
    void testInvalidConfiguration() {
        assertThatThrownBy(() -> GlobalConfiguration.loadConfiguration(tmpDir.getAbsolutePath()))
                .isInstanceOf(IllegalConfigurationException.class);
    }

    @Test
    // We allow malformed YAML files
    void testInvalidYamlFile() throws IOException {
        final File confFile =
                new File(tmpDir.getPath(), GlobalConfiguration.LEGACY_FLINK_CONF_FILENAME);

        try (PrintWriter pw = new PrintWriter(confFile)) {
            pw.append("invalid");
        }

        assertThat(GlobalConfiguration.loadConfiguration(tmpDir.getAbsolutePath())).isNotNull();
    }

    @Test
    public void testHiddenKey() {
        assertThat(GlobalConfiguration.isSensitive("password123")).isTrue();
        assertThat(GlobalConfiguration.isSensitive("123pasSword")).isTrue();
        assertThat(GlobalConfiguration.isSensitive("PasSword")).isTrue();
        assertThat(GlobalConfiguration.isSensitive("Secret")).isTrue();
        assertThat(GlobalConfiguration.isSensitive("polaris.client-secret")).isTrue();
        assertThat(GlobalConfiguration.isSensitive("client-secret")).isTrue();
        assertThat(GlobalConfiguration.isSensitive("service-key-json")).isTrue();
        assertThat(GlobalConfiguration.isSensitive("auth.basic.password")).isTrue();
        assertThat(GlobalConfiguration.isSensitive("auth.basic.token")).isTrue();
        assertThat(GlobalConfiguration.isSensitive("avro-confluent.basic-auth.user-info")).isTrue();
        assertThat(GlobalConfiguration.isSensitive("key.avro-confluent.basic-auth.user-info"))
                .isTrue();
        assertThat(GlobalConfiguration.isSensitive("value.avro-confluent.basic-auth.user-info"))
                .isTrue();
        assertThat(GlobalConfiguration.isSensitive("kafka.jaas.config")).isTrue();
        assertThat(GlobalConfiguration.isSensitive("properties.ssl.truststore.password")).isTrue();
        assertThat(GlobalConfiguration.isSensitive("properties.ssl.keystore.password")).isTrue();

        assertThat(
                        GlobalConfiguration.isSensitive(
                                "fs.azure.account.key.storageaccount123456.core.windows.net"))
                .isTrue();
        assertThat(GlobalConfiguration.isSensitive("Hello")).isFalse();
        assertThat(GlobalConfiguration.isSensitive("metrics.reporter.dghttp.apikey")).isTrue();
    }
}
