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

package org.apache.flink.runtime.jobmanager;

import org.apache.flink.configuration.Configuration;

import org.junit.jupiter.api.Test;

import static org.apache.flink.configuration.HighAvailabilityOptions.HA_MODE;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link HighAvailabilityMode}. */
class HighAvailabilityModeTest {

    // Default HA mode
    private static final HighAvailabilityMode DEFAULT_HA_MODE =
            HighAvailabilityMode.valueOf(HA_MODE.defaultValue());

    /** Tests HA mode configuration. */
    @Test
    void testFromConfig() {
        Configuration config = new Configuration();

        // Check default
        assertThat(HighAvailabilityMode.fromConfig(config)).isEqualTo(DEFAULT_HA_MODE);

        // Check not equals default
        config.set(HA_MODE, HighAvailabilityMode.ZOOKEEPER.name().toLowerCase());
        assertThat(HighAvailabilityMode.fromConfig(config))
                .isEqualTo(HighAvailabilityMode.ZOOKEEPER);

        // Check factory class
        config.set(HA_MODE, "factory.class.FQN");
        assertThat(HighAvailabilityMode.fromConfig(config))
                .isEqualTo(HighAvailabilityMode.FACTORY_CLASS);
    }

    /** Tests HA mode configuration with deprecated config values. */
    @Test
    void testDeprecatedFromConfig() {
        Configuration config = new Configuration();

        // Check mapping of old default to new default
        config.setString("recovery.mode", "standalone");
        assertThat(HighAvailabilityMode.fromConfig(config)).isEqualTo(DEFAULT_HA_MODE);

        // Check deprecated config
        config.setString("recovery.mode", HighAvailabilityMode.ZOOKEEPER.name().toLowerCase());
        assertThat(HighAvailabilityMode.fromConfig(config))
                .isEqualTo(HighAvailabilityMode.ZOOKEEPER);

        // Check precedence over deprecated config
        config.set(HA_MODE, HighAvailabilityMode.NONE.name().toLowerCase());
        config.setString("recovery.mode", HighAvailabilityMode.ZOOKEEPER.name().toLowerCase());

        assertThat(HighAvailabilityMode.fromConfig(config)).isEqualTo(HighAvailabilityMode.NONE);
    }

    @Test
    void testCheckHighAvailabilityModeActivated() {
        Configuration config = new Configuration();

        // check defaults
        assertThat(HighAvailabilityMode.isHighAvailabilityModeActivated(config)).isFalse();

        // check NONE
        config.setString("high-availability", HighAvailabilityMode.NONE.name().toLowerCase());
        assertThat(HighAvailabilityMode.isHighAvailabilityModeActivated(config)).isFalse();

        // check ZOOKEEPER
        config.setString("high-availability", HighAvailabilityMode.ZOOKEEPER.name().toLowerCase());
        assertThat(HighAvailabilityMode.isHighAvailabilityModeActivated(config)).isTrue();

        // check FACTORY_CLASS
        config.setString(
                "high-availability", HighAvailabilityMode.FACTORY_CLASS.name().toLowerCase());
        assertThat(HighAvailabilityMode.isHighAvailabilityModeActivated(config)).isTrue();
    }
}
