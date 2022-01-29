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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Unit tests for {@link PulsarConfigValidator}. */
class PulsarConfigValidatorTest {

    @Test
    void conflictKeysAndRequiredKeysValidation() {
        ConfigOption<String> required = ConfigOptions.key("required").stringType().noDefaultValue();
        ConfigOption<String> c1 = ConfigOptions.key("conflict1").stringType().noDefaultValue();
        ConfigOption<String> c2 = ConfigOptions.key("conflict2").stringType().noDefaultValue();

        PulsarConfigValidator validator =
                PulsarConfigValidator.builder()
                        .requiredOption(required)
                        .conflictOptions(c1, c2)
                        .build();
        Configuration configuration = new Configuration();

        // Required options
        assertThrows(IllegalArgumentException.class, () -> validator.validate(configuration));
        configuration.set(required, "required");
        assertDoesNotThrow(() -> validator.validate(configuration));

        // Conflict options
        configuration.set(c1, "c1");
        assertDoesNotThrow(() -> validator.validate(configuration));
        configuration.set(c2, "c2");
        assertThrows(IllegalArgumentException.class, () -> validator.validate(configuration));
    }
}
