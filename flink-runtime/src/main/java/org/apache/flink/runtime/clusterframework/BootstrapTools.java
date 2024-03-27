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

package org.apache.flink.runtime.clusterframework;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.runtime.entrypoint.parser.CommandLineOptions;
import org.apache.flink.util.OperatingSystem;

import org.apache.flink.shaded.guava31.com.google.common.escape.Escaper;
import org.apache.flink.shaded.guava31.com.google.common.escape.Escapers;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.stream.Stream;

import static org.apache.flink.configuration.ConfigOptions.key;

/** Tools for starting JobManager and TaskManager processes. */
public class BootstrapTools {
    /** Internal option which says if default value is used for {@link CoreOptions#TMP_DIRS}. */
    private static final ConfigOption<Boolean> USE_LOCAL_DEFAULT_TMP_DIRS =
            key("internal.io.tmpdirs.use-local-default").booleanType().defaultValue(false);

    private static final Logger LOG = LoggerFactory.getLogger(BootstrapTools.class);

    private static final Escaper UNIX_SINGLE_QUOTE_ESCAPER =
            Escapers.builder().addEscape('\'', "'\\''").build();

    private static final Escaper WINDOWS_DOUBLE_QUOTE_ESCAPER =
            Escapers.builder().addEscape('"', "\\\"").addEscape('^', "\"^^\"").build();

    @VisibleForTesting
    static final String IGNORE_UNRECOGNIZED_VM_OPTIONS = "-XX:+IgnoreUnrecognizedVMOptions";

    /**
     * Writes a Flink YAML config file from a Flink Configuration object.
     *
     * @param cfg The Flink config
     * @param file The File to write to
     * @throws IOException
     */
    public static void writeConfiguration(Configuration cfg, File file) throws IOException {
        try (FileWriter fwrt = new FileWriter(file);
                PrintWriter out = new PrintWriter(fwrt)) {
            for (String s : ConfigurationUtils.convertConfigToWritableLines(cfg, false)) {
                out.println(s);
            }
        }
    }

    /**
     * Sets the value of a new config key to the value of a deprecated config key.
     *
     * @param config Config to write
     * @param deprecated The old config key
     * @param designated The new config key
     */
    public static void substituteDeprecatedConfigKey(
            Configuration config, String deprecated, String designated) {
        // set the designated key only if it is not set already
        if (!config.containsKey(designated)) {
            final String valueForDeprecated = config.getString(deprecated, null);
            if (valueForDeprecated != null) {
                config.setString(designated, valueForDeprecated);
            }
        }
    }

    /**
     * Sets the value of a new config key to the value of a deprecated config key. Taking into
     * account the changed prefix.
     *
     * @param config Config to write
     * @param deprecatedPrefix Old prefix of key
     * @param designatedPrefix New prefix of key
     */
    public static void substituteDeprecatedConfigPrefix(
            Configuration config, String deprecatedPrefix, String designatedPrefix) {

        // set the designated key only if it is not set already
        final int prefixLen = deprecatedPrefix.length();

        Configuration replacement = new Configuration();

        for (String key : config.keySet()) {
            if (key.startsWith(deprecatedPrefix)) {
                String newKey = designatedPrefix + key.substring(prefixLen);
                if (!config.containsKey(newKey)) {
                    replacement.setString(newKey, config.getString(key, null));
                }
            }
        }

        config.addAll(replacement);
    }

    private static final String DYNAMIC_PROPERTIES_OPT = "D";

    /**
     * Get an instance of the dynamic properties option.
     *
     * <p>Dynamic properties allow the user to specify additional configuration values with -D, such
     * as <tt> -Dfs.overwrite-files=true -Dtaskmanager.memory.network.min=536346624</tt>
     */
    public static Option newDynamicPropertiesOption() {
        return new Option(DYNAMIC_PROPERTIES_OPT, true, "Dynamic properties");
    }

    /** Parse the dynamic properties (passed on the command line). */
    public static Configuration parseDynamicProperties(CommandLine cmd) {
        final Configuration config = new Configuration();

        String[] values = cmd.getOptionValues(DYNAMIC_PROPERTIES_OPT);
        if (values != null) {
            for (String value : values) {
                String[] pair = value.split("=", 2);
                if (pair.length == 1) {
                    config.setString(pair[0], Boolean.TRUE.toString());
                } else if (pair.length == 2) {
                    config.setString(pair[0], pair[1]);
                }
            }
        }

        return config;
    }

    // ------------------------------------------------------------------------

    /** Private constructor to prevent instantiation. */
    private BootstrapTools() {}

    /**
     * Set temporary configuration directories if necessary.
     *
     * @param configuration flink config to patch
     * @param defaultDirs in case no tmp directories is set, next directories will be applied
     */
    public static void updateTmpDirectoriesInConfiguration(
            Configuration configuration, @Nullable String defaultDirs) {
        if (configuration.contains(CoreOptions.TMP_DIRS)) {
            LOG.info(
                    "Overriding Flink's temporary file directories with those "
                            + "specified in the Flink config: {}",
                    configuration.getValue(CoreOptions.TMP_DIRS));
        } else if (defaultDirs != null) {
            LOG.info("Setting directories for temporary files to: {}", defaultDirs);
            configuration.set(CoreOptions.TMP_DIRS, defaultDirs);
            configuration.set(USE_LOCAL_DEFAULT_TMP_DIRS, true);
        }
    }

    /**
     * Clones the given configuration and resets instance specific config options.
     *
     * @param configuration to clone
     * @return Cloned configuration with reset instance specific config options
     */
    public static Configuration cloneConfiguration(Configuration configuration) {
        final Configuration clonedConfiguration = new Configuration(configuration);

        if (clonedConfiguration.get(USE_LOCAL_DEFAULT_TMP_DIRS)) {
            clonedConfiguration.removeConfig(CoreOptions.TMP_DIRS);
            clonedConfiguration.removeConfig(USE_LOCAL_DEFAULT_TMP_DIRS);
        }

        return clonedConfiguration;
    }

    /**
     * Get dynamic properties based on two Flink configurations. If base config does not contain and
     * target config contains the key or the value is different, it should be added to results.
     * Otherwise, if the base config contains and target config does not contain the key, it will be
     * ignored.
     *
     * @param baseConfig The base configuration.
     * @param targetConfig The target configuration.
     * @return Dynamic properties as string, separated by whitespace.
     */
    public static String getDynamicPropertiesAsString(
            Configuration baseConfig, Configuration targetConfig) {

        String[] newAddedConfigs =
                targetConfig.keySet().stream()
                        .flatMap(
                                (String key) -> {
                                    final String baseValue =
                                            baseConfig.get(
                                                    ConfigOptions.key(key)
                                                            .stringType()
                                                            .noDefaultValue());
                                    final String targetValue =
                                            targetConfig.get(
                                                    ConfigOptions.key(key)
                                                            .stringType()
                                                            .noDefaultValue());

                                    if (!baseConfig.keySet().contains(key)
                                            || !baseValue.equals(targetValue)) {
                                        return Stream.of(
                                                "-"
                                                        + CommandLineOptions.DYNAMIC_PROPERTY_OPTION
                                                                .getOpt()
                                                        + key
                                                        + CommandLineOptions.DYNAMIC_PROPERTY_OPTION
                                                                .getValueSeparator()
                                                        + escapeForDifferentOS(targetValue));
                                    } else {
                                        return Stream.empty();
                                    }
                                })
                        .toArray(String[]::new);
        return String.join(" ", newAddedConfigs);
    }

    /**
     * Escape all the dynamic property values. For Unix-like OS(Linux, MacOS, FREE_BSD, etc.), each
     * value will be surrounded with single quotes. This works for all chars except single quote
     * itself. To escape the single quote, close the quoting before it, insert the escaped single
     * quote, and then re-open the quoting. For example, the value is foo'bar and the escaped value
     * is 'foo'\''bar'. See <a
     * href="https://stackoverflow.com/questions/15783701/which-characters-need-to-be-escaped-when-using-bash">https://stackoverflow.com/questions/15783701/which-characters-need-to-be-escaped-when-using-bash</a>
     * for more information about Unix escaping.
     *
     * <p>For Windows OS, each value will be surrounded with double quotes. The double quote itself
     * needs to be escaped with back slash. Also the caret symbol need to be escaped with double
     * carets since Windows uses it to escape characters. See <a
     * href="https://en.wikibooks.org/wiki/Windows_Batch_Scripting">https://en.wikibooks.org/wiki/Windows_Batch_Scripting</a>
     * for more information about Windows escaping.
     *
     * @param value value to be escaped
     * @return escaped value
     */
    public static String escapeForDifferentOS(String value) {
        if (OperatingSystem.isWindows()) {
            return escapeWithDoubleQuote(value);
        } else {
            return escapeWithSingleQuote(value);
        }
    }

    public static String escapeWithSingleQuote(String value) {
        return "'" + UNIX_SINGLE_QUOTE_ESCAPER.escape(value) + "'";
    }

    public static String escapeWithDoubleQuote(String value) {
        return "\"" + WINDOWS_DOUBLE_QUOTE_ESCAPER.escape(value) + "\"";
    }
}
