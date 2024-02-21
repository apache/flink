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

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the {@link DelegatingConfiguration}. */
class DelegatingConfigurationTest {

    @Test
    void testIfDelegatesImplementAllMethods() throws IllegalArgumentException {

        // For each method in the Configuration class...
        Method[] confMethods = Configuration.class.getDeclaredMethods();
        Method[] delegateMethods = DelegatingConfiguration.class.getDeclaredMethods();

        for (Method configurationMethod : confMethods) {
            final int mod = configurationMethod.getModifiers();
            if (!Modifier.isPublic(mod) || Modifier.isStatic(mod)) {
                continue;
            }

            boolean hasMethod = false;

            // Find matching method in wrapper class and call it
            lookForWrapper:
            for (Method wrapperMethod : delegateMethods) {
                if (configurationMethod.getName().equals(wrapperMethod.getName())) {

                    // Get parameters for method
                    Class<?>[] wrapperMethodParams = wrapperMethod.getParameterTypes();
                    Class<?>[] configMethodParams = configurationMethod.getParameterTypes();
                    if (wrapperMethodParams.length != configMethodParams.length) {
                        continue;
                    }

                    for (int i = 0; i < wrapperMethodParams.length; i++) {
                        if (wrapperMethodParams[i] != configMethodParams[i]) {
                            continue lookForWrapper;
                        }
                    }
                    hasMethod = true;
                    break;
                }
            }

            assertThat(hasMethod)
                    .as(
                            "Configuration method '"
                                    + configurationMethod.getName()
                                    + "' has not been wrapped correctly in DelegatingConfiguration wrapper")
                    .isTrue();
        }
    }

    @Test
    void testDelegationConfigurationWithNullOrEmptyPrefix() {
        Configuration backingConf = new Configuration();
        backingConf.setValueInternal("test-key", "value", false);

        assertThatThrownBy(() -> new DelegatingConfiguration(backingConf, null))
                .isInstanceOf(NullPointerException.class);

        DelegatingConfiguration configuration = new DelegatingConfiguration(backingConf, "");
        assertThat(backingConf.keySet()).isEqualTo(configuration.keySet());
    }

    @Test
    void testDelegationConfigurationWithPrefix() {
        String prefix = "pref-";
        String expectedKey = "key";

        /*
         * Key matches the prefix
         */
        Configuration backingConf = new Configuration();
        backingConf.setValueInternal(prefix + expectedKey, "value", false);

        DelegatingConfiguration configuration = new DelegatingConfiguration(backingConf, prefix);
        Set<String> keySet = configuration.keySet();
        assertThat(keySet).hasSize(1).containsExactly(expectedKey);

        /*
         * Key does not match the prefix
         */
        backingConf = new Configuration();
        backingConf.setValueInternal("test-key", "value", false);

        configuration = new DelegatingConfiguration(backingConf, prefix);
        assertThat(configuration.keySet()).isEmpty();
    }

    @Test
    void testDelegationConfigurationToMapConsistentWithAddAllToProperties() {
        Configuration conf = new Configuration();
        conf.setString("k0", "v0");
        conf.setString("prefix.k1", "v1");
        conf.setString("prefix.prefix.k2", "v2");
        conf.setString("k3.prefix.prefix.k3", "v3");
        DelegatingConfiguration dc = new DelegatingConfiguration(conf, "prefix.");
        // Collect all properties
        Properties properties = new Properties();
        dc.addAllToProperties(properties);
        // Convert the Map<String, String> object into a Properties object
        Map<String, String> map = dc.toMap();
        Properties mapProperties = new Properties();
        for (Map.Entry<String, String> entry : map.entrySet()) {
            mapProperties.put(entry.getKey(), entry.getValue());
        }
        // Verification
        assertThat(mapProperties).isEqualTo(properties);
    }

    @Test
    void testSetReturnsDelegatingConfiguration() {
        final Configuration conf = new Configuration();
        final DelegatingConfiguration delegatingConf = new DelegatingConfiguration(conf, "prefix.");

        assertThat(delegatingConf.set(CoreOptions.DEFAULT_PARALLELISM, 1)).isSameAs(delegatingConf);
    }

    @Test
    void testGetWithOverrideDefault() {
        Configuration original = new Configuration();
        final DelegatingConfiguration delegatingConf =
                new DelegatingConfiguration(original, "prefix.");

        // Test for integer
        ConfigOption<Integer> integerOption =
                ConfigOptions.key("integer.key").intType().noDefaultValue();

        // integerOption doesn't exist in delegatingConf, and it should be overrideDefault.
        original.setInteger(integerOption, 1);
        assertThat(delegatingConf.getInteger(integerOption, 2)).isEqualTo(2);
        assertThat(delegatingConf.get(integerOption, 2)).isEqualTo(2);

        // integerOption exists in delegatingConf, and it should be value that set before.
        delegatingConf.setInteger(integerOption, 3);
        assertThat(delegatingConf.getInteger(integerOption, 2)).isEqualTo(3);
        assertThat(delegatingConf.get(integerOption, 2)).isEqualTo(3);

        // Test for float
        ConfigOption<Float> floatOption =
                ConfigOptions.key("float.key").floatType().noDefaultValue();
        original.setFloat(floatOption, 4f);
        assertThat(delegatingConf.getFloat(floatOption, 5f)).isEqualTo(5f);
        assertThat(delegatingConf.get(floatOption, 5f)).isEqualTo(5f);
        delegatingConf.setFloat(floatOption, 6f);
        assertThat(delegatingConf.getFloat(floatOption, 5f)).isEqualTo(6f);
        assertThat(delegatingConf.get(floatOption, 5f)).isEqualTo(6f);

        // Test for double
        ConfigOption<Double> doubleOption =
                ConfigOptions.key("double.key").doubleType().noDefaultValue();
        original.setDouble(doubleOption, 7d);
        assertThat(delegatingConf.getDouble(doubleOption, 8d)).isEqualTo(8d);
        assertThat(delegatingConf.get(doubleOption, 8d)).isEqualTo(8d);
        delegatingConf.setDouble(doubleOption, 9f);
        assertThat(delegatingConf.getDouble(doubleOption, 8d)).isEqualTo(9f);
        assertThat(delegatingConf.get(doubleOption, 8d)).isEqualTo(9f);

        // Test for long
        ConfigOption<Long> longOption = ConfigOptions.key("long.key").longType().noDefaultValue();
        original.setLong(longOption, 10L);
        assertThat(delegatingConf.getLong(longOption, 11L)).isEqualTo(11L);
        assertThat(delegatingConf.get(longOption, 11L)).isEqualTo(11L);
        delegatingConf.setLong(longOption, 12L);
        assertThat(delegatingConf.getLong(longOption, 11L)).isEqualTo(12L);
        assertThat(delegatingConf.get(longOption, 11L)).isEqualTo(12L);

        // Test for boolean
        ConfigOption<Boolean> booleanOption =
                ConfigOptions.key("boolean.key").booleanType().noDefaultValue();
        original.setBoolean(booleanOption, false);
        assertThat(delegatingConf.getBoolean(booleanOption, true)).isEqualTo(true);
        assertThat(delegatingConf.get(booleanOption, true)).isEqualTo(true);
        delegatingConf.setBoolean(booleanOption, false);
        assertThat(delegatingConf.getBoolean(booleanOption, true)).isEqualTo(false);
        assertThat(delegatingConf.get(booleanOption, true)).isEqualTo(false);
    }

    @Test
    void testRemoveKeyOrConfig() {
        Configuration original = new Configuration();
        final DelegatingConfiguration delegatingConf =
                new DelegatingConfiguration(original, "prefix.");
        ConfigOption<Integer> integerOption =
                ConfigOptions.key("integer.key").intType().noDefaultValue();

        // Test for removeConfig
        delegatingConf.set(integerOption, 0);
        assertThat(delegatingConf.get(integerOption)).isZero();
        delegatingConf.removeConfig(integerOption);
        assertThat(delegatingConf.getOptional(integerOption)).isEmpty();
        assertThat(delegatingConf.getInteger(integerOption.key(), 0)).isZero();

        // Test for removeKey
        delegatingConf.set(integerOption, 0);
        assertThat(delegatingConf.getInteger(integerOption, -1)).isZero();
        delegatingConf.removeKey(integerOption.key());
        assertThat(delegatingConf.getOptional(integerOption)).isEmpty();
        assertThat(delegatingConf.getInteger(integerOption.key(), 0)).isZero();
    }
}
