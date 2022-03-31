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

package org.apache.flink.connector.pulsar.common.config;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Properties;

import static java.util.Collections.emptyMap;
import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** Unit tests for {@link PulsarConfiguration}. */
class PulsarConfigurationTest {

    private static final ConfigOption<Map<String, String>> PROP_OP =
            ConfigOptions.key("some.config").mapType().defaultValue(emptyMap());

    @Test
    void pulsarConfigurationCanGetMapWithPrefix() {
        Properties expectProp = new Properties();
        for (int i = 0; i < 10; i++) {
            expectProp.put(randomAlphabetic(10), randomAlphabetic(10));
        }

        Configuration configuration = new Configuration();

        for (String name : expectProp.stringPropertyNames()) {
            configuration.setString(PROP_OP.key() + "." + name, expectProp.getProperty(name));
        }

        TestConfiguration configuration1 = new TestConfiguration(configuration);
        Map<String, String> properties = configuration1.getProperties(PROP_OP);
        assertEquals(properties, expectProp);
    }

    private static final class TestConfiguration extends PulsarConfiguration {
        private static final long serialVersionUID = 944689984000450917L;

        private TestConfiguration(Configuration config) {
            super(config);
        }
    }
}
