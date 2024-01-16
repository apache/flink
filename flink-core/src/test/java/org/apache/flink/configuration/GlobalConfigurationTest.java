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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
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
    void testConfigurationWithLegacyYAML() {
        File confFile = new File(tmpDir, GlobalConfiguration.LEGACY_FLINK_CONF_FILENAME);

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
            assertThat(conf.keySet()).hasSize(6);

            // keys 1, 2, 4, 5, 6, 7, 8 should be OK and match the expected values
            assertThat(conf.getString("mykey1", null)).isEqualTo("myvalue1");
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
            // Clear the standard yaml flag to avoid impact to other cases.
            GlobalConfiguration.setStandardYaml(true);
            confFile.delete();
            tmpDir.delete();
        }
    }

    @Test
    void testConfigurationWithStandardYAML() {
        File confFile = new File(tmpDir, GlobalConfiguration.FLINK_CONF_FILENAME);

        try (final PrintWriter pw = new PrintWriter(confFile)) {
            pw.println("Key1: ");
            pw.println("    Key2: v1");
            pw.println("    Key3: 'v2'");
            pw.println("Key4: 1");
            pw.println("Key5: '1'");
            pw.println("Key6: '*'");
            pw.println("Key7: true");
            pw.println("Key8: 'true'");
            pw.println("Key9: [a, b, '*', 1, '2',  true, 'true']");
            pw.println("Key10: {k1: v1, k2: '2', k3: 3}");
            pw.println("Key11: [{k1: v1, k2: '2', k3: 3}, {k4: true}]");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        Configuration conf = GlobalConfiguration.loadConfiguration(tmpDir.getAbsolutePath());

        assertThat(conf.keySet()).hasSize(12);

        assertThat(conf.get(ConfigOptions.key("Key1.Key2").stringType().noDefaultValue()))
                .isEqualTo("v1");
        assertThat(conf.get(ConfigOptions.key("Key1.Key3").stringType().noDefaultValue()))
                .isEqualTo("v2");
        assertThat(conf.get(ConfigOptions.key("Key4").intType().noDefaultValue())).isOne();
        assertThat(conf.get(ConfigOptions.key("Key5").stringType().noDefaultValue()))
                .isEqualTo("1");
        assertThat(conf.get(ConfigOptions.key("Key6").stringType().noDefaultValue()))
                .isEqualTo("*");
        assertThat(conf.get(ConfigOptions.key("Key7").booleanType().noDefaultValue())).isTrue();
        assertThat(conf.get(ConfigOptions.key("Key8").stringType().noDefaultValue()))
                .isEqualTo("true");
        assertThat(conf.get(ConfigOptions.key("Key9").stringType().asList().noDefaultValue()))
                .isEqualTo(Arrays.asList("a", "b", "*", "1", "2", "true", "true"));

        Map<String, String> map = new HashMap<>();
        map.put("k1", "v1");
        map.put("k2", "2");
        map.put("k3", "3");
        assertThat(conf.get(ConfigOptions.key("Key10").mapType().noDefaultValue())).isEqualTo(map);

        Map<String, String> map2 = new HashMap<>();
        map2.put("k4", "true");
        assertThat(conf.get(ConfigOptions.key("Key11").mapType().asList().noDefaultValue()))
                .isEqualTo(Arrays.asList(map, map2));
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
    // We allow malformed YAML files if loaded legacy flink conf
    void testInvalidLegacyYamlFile() throws IOException {
        final File confFile =
                new File(tmpDir.getPath(), GlobalConfiguration.LEGACY_FLINK_CONF_FILENAME);

        try (PrintWriter pw = new PrintWriter(confFile)) {
            pw.append("invalid");
        }

        assertThat(GlobalConfiguration.loadConfiguration(tmpDir.getAbsolutePath())).isNotNull();
        // Clear the standard yaml flag to avoid impact to other cases.
        GlobalConfiguration.setStandardYaml(true);
    }

    @Test
    // We do not allow malformed YAML files if loaded standard yaml
    void testInvalidStandardYamlFile() throws IOException {
        final File confFile = new File(tmpDir.getPath(), GlobalConfiguration.FLINK_CONF_FILENAME);

        try (PrintWriter pw = new PrintWriter(confFile)) {
            pw.append("invalid");
        }

        // We do not allow malformed YAML files if loaded standard yaml
        assertThatThrownBy(() -> GlobalConfiguration.loadConfiguration(tmpDir.getAbsolutePath()))
                .isInstanceOf(RuntimeException.class)
                .hasCauseInstanceOf(ClassCastException.class)
                .satisfies(
                        e -> {
                            Throwable cause = e.getCause();
                            assertThat(cause).isNotNull();
                            assertThat(cause).hasMessageContaining("java.lang.String");
                            assertThat(cause).hasMessageContaining("java.util.Map");
                        });
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
