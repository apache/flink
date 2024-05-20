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

package org.apache.flink.externalresource.gpu;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.docs.Documentation;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.description.Description;

import static org.apache.flink.configuration.ConfigOptions.key;
import static org.apache.flink.configuration.description.LinkElement.link;

/** A collection of all configuration options for GPU driver. */
@PublicEvolving
public class GPUDriverOptions {

    private static final String NAMED_EXTERNAL_RESOURCE_CONFIG_PREFIX =
            "external-resource.<resource_name>.param";

    @Documentation.SuffixOption(NAMED_EXTERNAL_RESOURCE_CONFIG_PREFIX)
    public static final ConfigOption<String> DISCOVERY_SCRIPT_PATH =
            key("discovery-script.path")
                    .stringType()
                    .defaultValue(
                            String.format(
                                    "%s/external-resource-gpu/nvidia-gpu-discovery.sh",
                                    ConfigConstants.DEFAULT_FLINK_PLUGINS_DIRS))
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "The path of the %s."
                                                    + "It can either be an absolute path, "
                                                    + "or a relative path to FLINK_HOME when defined or the "
                                                    + "current directory otherwise. If not explicitly configured, "
                                                    + "the default script will be used.",
                                            link(
                                                    "{{.Site.BaseURL}}{{.Site.LanguagePrefix}}/docs/deployment/advanced/external_resources/#discovery-script",
                                                    "discovery script"))
                                    .build());

    @Documentation.SuffixOption(NAMED_EXTERNAL_RESOURCE_CONFIG_PREFIX)
    public static final ConfigOption<String> DISCOVERY_SCRIPT_ARG =
            key("discovery-script.args")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "The arguments passed to the discovery script. For the "
                                                    + "default discovery script, "
                                                    + "see %s for the available parameters.",
                                            link(
                                                    "{{.Site.BaseURL}}{{.Site.LanguagePrefix}}/docs/deployment/advanced/external_resources/#default-script",
                                                    "Default Script"))
                                    .build());
}
