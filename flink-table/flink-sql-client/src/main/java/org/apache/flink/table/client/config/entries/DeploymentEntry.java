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

package org.apache.flink.table.client.config.entries;

import org.apache.flink.client.cli.CliFrontendParser;
import org.apache.flink.table.client.config.ConfigUtil;
import org.apache.flink.table.client.config.Environment;
import org.apache.flink.table.descriptors.DescriptorProperties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.client.config.Environment.DEPLOYMENT_ENTRY;

/**
 * Configuration of a Flink cluster deployment. This class parses the `deployment` part in an
 * environment file. In the future, we should keep the amount of properties here little and forward
 * most properties to Flink's CLI frontend properties directly.
 *
 * <p>All properties of this entry are optional and evaluated lazily.
 *
 * @deprecated This will be removed in Flink 1.14 with dropping support of {@code sql-client.yaml}
 *     configuration file.
 */
@Deprecated
public class DeploymentEntry extends ConfigEntry {

    private static final Logger LOG = LoggerFactory.getLogger(DeploymentEntry.class);

    public static final DeploymentEntry DEFAULT_INSTANCE =
            new DeploymentEntry(new DescriptorProperties(true));

    private DeploymentEntry(DescriptorProperties properties) {
        super(properties);
    }

    @Override
    protected void validate(DescriptorProperties properties) {
        // do nothing
    }

    /**
     * Parses the given command line options from the deployment properties. Ignores properties that
     * are not defined by options.
     */
    public CommandLine getCommandLine(Options commandLineOptions) throws Exception {
        final List<String> args = new ArrayList<>();

        properties
                .asMap()
                .forEach(
                        (k, v) -> {
                            // only add supported options
                            if (commandLineOptions.hasOption(k)) {
                                final Option o = commandLineOptions.getOption(k);
                                final String argument = "--" + o.getLongOpt();
                                // options without args
                                if (!o.hasArg()) {
                                    final Boolean flag = Boolean.parseBoolean(v);
                                    // add key only
                                    if (flag) {
                                        args.add(argument);
                                    }
                                }
                                // add key and value
                                else if (!o.hasArgs()) {
                                    args.add(argument);
                                    args.add(v);
                                }
                                // options with multiple args are not supported yet
                                else {
                                    throw new IllegalArgumentException(
                                            "Option '" + o + "' is not supported yet.");
                                }
                            }
                        });

        return CliFrontendParser.parse(
                commandLineOptions, args.toArray(new String[args.size()]), true);
    }

    private <V> V useDefaultValue(String key, V defaultValue) {
        LOG.info(
                "Property '{}.{}' not specified. Using default value: {}",
                DEPLOYMENT_ENTRY,
                key,
                defaultValue);
        return defaultValue;
    }

    public Map<String, String> asTopLevelMap() {
        return properties.asPrefixedMap(DEPLOYMENT_ENTRY + '.');
    }

    // --------------------------------------------------------------------------------------------

    public static DeploymentEntry create(Map<String, Object> config) {
        return new DeploymentEntry(ConfigUtil.normalizeYaml(config));
    }

    /**
     * Merges two deployments entries. The properties of the first deployment entry might be
     * overwritten by the second one.
     */
    public static DeploymentEntry merge(DeploymentEntry deployment1, DeploymentEntry deployment2) {
        final Map<String, String> mergedProperties = new HashMap<>(deployment1.asMap());
        mergedProperties.putAll(deployment2.asMap());

        final DescriptorProperties properties = new DescriptorProperties(true);
        properties.putProperties(mergedProperties);

        return new DeploymentEntry(properties);
    }

    /**
     * Creates a new deployment entry enriched with additional properties that are prefixed with
     * {@link Environment#DEPLOYMENT_ENTRY}.
     */
    public static DeploymentEntry enrich(
            DeploymentEntry deployment, Map<String, String> prefixedProperties) {
        final Map<String, String> enrichedProperties = new HashMap<>(deployment.asMap());

        prefixedProperties.forEach(
                (k, v) -> {
                    final String normalizedKey = k.toLowerCase();
                    if (k.startsWith(DEPLOYMENT_ENTRY + '.')) {
                        enrichedProperties.put(
                                normalizedKey.substring(DEPLOYMENT_ENTRY.length() + 1), v);
                    }
                });

        final DescriptorProperties properties = new DescriptorProperties(true);
        properties.putProperties(enrichedProperties);

        return new DeploymentEntry(properties);
    }

    @Override
    public String toString() {
        return "DeploymentEntry{" + "properties=" + properties + '}';
    }
}
