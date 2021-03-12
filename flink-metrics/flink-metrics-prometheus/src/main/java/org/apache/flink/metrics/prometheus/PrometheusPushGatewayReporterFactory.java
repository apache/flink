/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.metrics.prometheus;

import org.apache.flink.metrics.reporter.InterceptInstantiationViaReflection;
import org.apache.flink.metrics.reporter.MetricReporterFactory;

import java.util.Properties;

import static org.apache.flink.metrics.prometheus.PrometheusPushGatewayReporterOptions.DELETE_ON_SHUTDOWN;
import static org.apache.flink.metrics.prometheus.PrometheusPushGatewayReporterOptions.GROUPING_KEY;
import static org.apache.flink.metrics.prometheus.PrometheusPushGatewayReporterOptions.HOST;
import static org.apache.flink.metrics.prometheus.PrometheusPushGatewayReporterOptions.JOB_NAME;
import static org.apache.flink.metrics.prometheus.PrometheusPushGatewayReporterOptions.PORT;
import static org.apache.flink.metrics.prometheus.PrometheusPushGatewayReporterOptions.RANDOM_JOB_NAME_SUFFIX;

/** {@link MetricReporterFactory} for {@link PrometheusPushGatewayReporter}. */
@InterceptInstantiationViaReflection(
        reporterClassName = "org.apache.flink.metrics.prometheus.PrometheusPushGatewayReporter")
public class PrometheusPushGatewayReporterFactory implements MetricReporterFactory {

    @Override
    public PrometheusPushGatewayReporter createMetricReporter(Properties properties) {
        String hostConfig = properties.getString(HOST.key(), HOST.defaultValue());
        int portConfig = properties.getInteger(PORT.key(), PORT.defaultValue());
        String jobNameConfig = properties.getString(JOB_NAME.key(), JOB_NAME.defaultValue())
        boolean randomJobSuffixConfig =
                config.getBoolean(
                        RANDOM_JOB_NAME_SUFFIX.key(), RANDOM_JOB_NAME_SUFFIX.defaultValue());
        boolean deleteOnShutdownConfig =
                config.getBoolean(DELETE_ON_SHUTDOWN.key(), DELETE_ON_SHUTDOWN.defaultValue());
        Map<String, String> groupingKeyConfig =
                parseGroupingKey(config.getString(GROUPING_KEY.key(), GROUPING_KEY.defaultValue()));

        return new PrometheusPushGatewayReporter(
            hostConfig,
            portConfig,
            jobNameConfig,
            randomJobSuffixConfig,
            deleteOnShutdownConfig,
            groupingKeyConfig
        );
    }
}
