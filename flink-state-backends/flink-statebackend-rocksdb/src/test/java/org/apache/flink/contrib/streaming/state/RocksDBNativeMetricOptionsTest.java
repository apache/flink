/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.configuration.Configuration;

import org.junit.Assert;
import org.junit.Test;

/** Test all native metrics can be set using configuration. */
public class RocksDBNativeMetricOptionsTest {
    @Test
    public void testNativeMetricsConfigurable() {
        for (RocksDBProperty property : RocksDBProperty.values()) {
            Configuration config = new Configuration();
            config.setBoolean(property.getConfigKey(), true);

            RocksDBNativeMetricOptions options = RocksDBNativeMetricOptions.fromConfig(config);

            Assert.assertTrue(
                    String.format(
                            "Failed to enable native metrics with property %s",
                            property.getConfigKey()),
                    options.isEnabled());

            Assert.assertTrue(
                    String.format(
                            "Failed to enable native metric %s using config",
                            property.getConfigKey()),
                    options.getProperties().contains(property.getRocksDBProperty()));
        }
    }
}
