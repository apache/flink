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

import org.apache.flink.util.TimeUtils;

import org.apache.flink.shaded.jackson2.org.yaml.snakeyaml.DumperOptions;
import org.apache.flink.shaded.jackson2.org.yaml.snakeyaml.LoaderOptions;
import org.apache.flink.shaded.jackson2.org.yaml.snakeyaml.Yaml;
import org.apache.flink.shaded.jackson2.org.yaml.snakeyaml.constructor.Constructor;
import org.apache.flink.shaded.jackson2.org.yaml.snakeyaml.error.Mark;
import org.apache.flink.shaded.jackson2.org.yaml.snakeyaml.error.MarkedYAMLException;
import org.apache.flink.shaded.jackson2.org.yaml.snakeyaml.error.YAMLException;
import org.apache.flink.shaded.jackson2.org.yaml.snakeyaml.nodes.Node;
import org.apache.flink.shaded.jackson2.org.yaml.snakeyaml.nodes.Tag;
import org.apache.flink.shaded.jackson2.org.yaml.snakeyaml.representer.Represent;
import org.apache.flink.shaded.jackson2.org.yaml.snakeyaml.representer.Representer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.time.Duration;
import java.util.Map;

/**
 * This class contains utility methods to load standard yaml file and convert object to standard
 * yaml syntax.
 */
public class YamlParserUtils {

    private static final Logger LOG = LoggerFactory.getLogger(YamlParserUtils.class);

    private static final Yaml yaml;

    private static final DumperOptions dumperOptions = new DumperOptions();

    private static final LoaderOptions loaderOptions = new LoaderOptions();

    static {
        // Make the dump output is in single line
        dumperOptions.setDefaultFlowStyle(DumperOptions.FlowStyle.FLOW);
        dumperOptions.setWidth(Integer.MAX_VALUE);
        // The standard YAML do not allow duplicate keys.
        loaderOptions.setAllowDuplicateKeys(false);

        yaml =
                new Yaml(
                        new Constructor(loaderOptions),
                        new FlinkConfigRepresenter(dumperOptions),
                        dumperOptions,
                        loaderOptions);
    }

    public static synchronized Map<String, Object> loadYamlFile(File file) throws Exception {
        try (FileInputStream inputStream = new FileInputStream((file))) {
            return yaml.load(inputStream);
        } catch (FileNotFoundException e) {
            LOG.error("Failed to find YAML file", e);
            throw e;
        } catch (IOException | YAMLException e) {
            if (e instanceof MarkedYAMLException) {
                YAMLException exception =
                        wrapExceptionToHiddenSensitiveData((MarkedYAMLException) e);
                LOG.error("Failed to parse YAML configuration", exception);
                throw exception;
            } else {
                throw e;
            }
        }
    }

    /**
     * Converts the given value to a string representation in the YAML syntax. This method uses a
     * YAML parser to convert the object to YAML format.
     *
     * <p>The resulting YAML string may have line breaks at the end of each line. This method
     * removes the line break at the end of the string if it exists.
     *
     * <p>Note: This method may perform escaping on certain characters in the value to ensure proper
     * YAML syntax.
     *
     * @param value The value to be converted.
     * @return The string representation of the value in YAML syntax.
     */
    public static synchronized String toYAMLString(Object value) {
        try {
            String output = yaml.dump(value);
            // remove the line break
            String linebreak = dumperOptions.getLineBreak().getString();
            if (output.endsWith(linebreak)) {
                output = output.substring(0, output.length() - linebreak.length());
            }
            return output;
        } catch (MarkedYAMLException exception) {
            throw wrapExceptionToHiddenSensitiveData(exception);
        }
    }

    public static synchronized <T> T convertToObject(String value, Class<T> type) {
        try {
            return yaml.loadAs(value, type);
        } catch (MarkedYAMLException exception) {
            throw wrapExceptionToHiddenSensitiveData(exception);
        }
    }

    /**
     * This method wraps a MarkedYAMLException to hide sensitive data in its message. Before using
     * this method, an exception message might include sensitive information like:
     *
     * <pre>{@code
     * while constructing a mapping
     * in 'reader', line 1, column 1:
     *     key1: secret1
     *     ^
     * found duplicate key key1
     * in 'reader', line 2, column 1:
     *     key1: secret2
     *     ^
     * }</pre>
     *
     * <p>After using this method, the message will be sanitized to hide the sensitive details:
     *
     * <pre>{@code
     * while constructing a mapping
     * in 'reader', line 1, column 1
     * found duplicate key key1
     * in 'reader', line 2, column 1
     * }</pre>
     *
     * @param exception The MarkedYAMLException containing potentially sensitive data.
     * @return A YAMLException with a message that has sensitive data hidden.
     */
    private static YAMLException wrapExceptionToHiddenSensitiveData(MarkedYAMLException exception) {
        StringBuilder lines = new StringBuilder();
        String context = exception.getContext();
        Mark contextMark = exception.getContextMark();
        String problem = exception.getProblem();
        Mark problemMark = exception.getProblemMark();

        if (context != null) {
            lines.append(context);
            lines.append("\n");
        }
        if (contextMark != null
                && (problem == null
                        || problemMark == null
                        || contextMark.getName().equals(problemMark.getName())
                        || (contextMark.getLine() != problemMark.getLine())
                        || (contextMark.getColumn() != problemMark.getColumn()))) {
            lines.append(hiddenSensitiveDataInMark(contextMark));
            lines.append("\n");
        }
        if (problem != null) {
            lines.append(problem);
            lines.append("\n");
        }
        if (problemMark != null) {
            lines.append(hiddenSensitiveDataInMark(problemMark));
            lines.append("\n");
        }

        Throwable cause = exception.getCause();
        if (cause instanceof MarkedYAMLException) {
            cause = wrapExceptionToHiddenSensitiveData((MarkedYAMLException) cause);
        }

        YAMLException yamlException = new YAMLException(lines.toString(), cause);
        yamlException.setStackTrace(exception.getStackTrace());
        return yamlException;
    }

    /**
     * This method is a mock implementation of the Mark#toString() method, specifically designed to
     * exclude the Mark#get_snippet(), to prevent leaking any sensitive data.
     */
    private static String hiddenSensitiveDataInMark(Mark mark) {
        return " in "
                + mark.getName()
                + ", line "
                + (mark.getLine() + 1)
                + ", column "
                + (mark.getColumn() + 1);
    }

    private static class FlinkConfigRepresenter extends Representer {
        public FlinkConfigRepresenter(DumperOptions options) {
            super(options);
            representers.put(Duration.class, new RepresentDuration());
            representers.put(MemorySize.class, new RepresentMemorySize());
            multiRepresenters.put(Enum.class, new RepresentEnum());
        }

        private class RepresentDuration implements Represent {
            @Override
            public Node representData(Object data) {
                Duration duration = (Duration) data;
                String durationString = TimeUtils.formatWithHighestUnit(duration);
                return representScalar(getTag(duration.getClass(), Tag.STR), durationString, null);
            }
        }

        private class RepresentMemorySize implements Represent {
            @Override
            public Node representData(Object data) {
                MemorySize memorySize = (MemorySize) data;
                return representScalar(
                        getTag(memorySize.getClass(), Tag.STR), memorySize.toString(), null);
            }
        }

        private class RepresentEnum implements Represent {
            @Override
            public Node representData(Object data) {
                return representScalar(getTag(data.getClass(), Tag.STR), data.toString(), null);
            }
        }
    }
}
