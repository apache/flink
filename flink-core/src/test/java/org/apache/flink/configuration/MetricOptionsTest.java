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

import static org.assertj.core.api.Assertions.assertThat;

class MetricOptionsTest {
    private static final ConfigOption<String> SUB_OPTION =
            ConfigOptions.key("option").stringType().noDefaultValue();
    private static final ConfigOption<String> FULL_OPTION =
            ConfigOptions.key(
                            ConfigConstants.METRICS_REPORTER_PREFIX
                                    + "my_reporter."
                                    + SUB_OPTION.key())
                    .stringType()
                    .noDefaultValue();

    @Test
    void testForReporterWrite() {
        Configuration configuration = new Configuration();

        MetricOptions.forReporter(configuration, "my_reporter").set(SUB_OPTION, "value");

        assertThat(configuration.get(FULL_OPTION)).isEqualTo("value");
    }

    @Test
    void testForReporterRead() {
        Configuration configuration = new Configuration();
        configuration.set(FULL_OPTION, "value");

        assertThat(MetricOptions.forReporter(configuration, "my_reporter").get(SUB_OPTION))
                .isEqualTo("value");
    }
}
