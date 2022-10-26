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

package org.apache.flink.metrics.prometheus;

import org.apache.flink.annotation.docs.Documentation;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.description.Description;
import org.apache.flink.configuration.description.LinkElement;
import org.apache.flink.configuration.description.TextElement;

/** Config options for the {@link PrometheusPushGatewayReporter}. */
@Documentation.SuffixOption(ConfigConstants.METRICS_REPORTER_PREFIX + "prometheus")
public class PrometheusPushGatewayReporterOptions {

    @Deprecated
    public static final ConfigOption<String> HOST =
            ConfigOptions.key("host")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("(deprecated) The PushGateway server host.");

    @Deprecated
    public static final ConfigOption<Integer> PORT =
            ConfigOptions.key("port")
                    .intType()
                    .defaultValue(-1)
                    .withDescription("(deprecated) The PushGateway server port.");

    public static final ConfigOption<String> HOST_URL =
            ConfigOptions.key("hostUrl")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The PushGateway server host URL including scheme, host name, and port.");

    public static final ConfigOption<String> JOB_NAME =
            ConfigOptions.key("jobName")
                    .stringType()
                    .defaultValue("")
                    .withDescription("The job name under which metrics will be pushed");

    public static final ConfigOption<Boolean> RANDOM_JOB_NAME_SUFFIX =
            ConfigOptions.key("randomJobNameSuffix")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Specifies whether a random suffix should be appended to the job name.");

    public static final ConfigOption<Boolean> DELETE_ON_SHUTDOWN =
            ConfigOptions.key("deleteOnShutdown")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Specifies whether to delete metrics from the PushGateway on shutdown."
                                                    + " Flink will try its best to delete the metrics but this is not guaranteed. See %s for more details.",
                                            LinkElement.link(
                                                    "https://issues.apache.org/jira/browse/FLINK-13787",
                                                    "here"))
                                    .build());

    public static final ConfigOption<Boolean> FILTER_LABEL_VALUE_CHARACTER =
            ConfigOptions.key("filterLabelValueCharacters")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Specifies whether to filter label value characters."
                                                    + " If enabled, all characters not matching [a-zA-Z0-9:_] will be removed,"
                                                    + " otherwise no characters will be removed."
                                                    + " Before disabling this option please ensure that your"
                                                    + " label values meet the %s.",
                                            LinkElement.link(
                                                    "https://prometheus.io/docs/concepts/data_model/#metric-names-and-labels",
                                                    "Prometheus requirements"))
                                    .build());

    public static final ConfigOption<String> GROUPING_KEY =
            ConfigOptions.key("groupingKey")
                    .stringType()
                    .defaultValue("")
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Specifies the grouping key which is the group and global labels of all metrics."
                                                    + " The label name and value are separated by '=', and labels are separated by ';', e.g., %s."
                                                    + " Please ensure that your grouping key meets the %s.",
                                            TextElement.code("k1=v1;k2=v2"),
                                            LinkElement.link(
                                                    "https://prometheus.io/docs/concepts/data_model/#metric-names-and-labels",
                                                    "Prometheus requirements"))
                                    .build());
}
