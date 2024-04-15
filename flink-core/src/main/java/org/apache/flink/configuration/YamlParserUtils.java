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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.snakeyaml.engine.v2.api.Dump;
import org.snakeyaml.engine.v2.api.DumpSettings;
import org.snakeyaml.engine.v2.api.Load;
import org.snakeyaml.engine.v2.api.LoadSettings;
import org.snakeyaml.engine.v2.common.FlowStyle;
import org.snakeyaml.engine.v2.exceptions.Mark;
import org.snakeyaml.engine.v2.exceptions.MarkedYamlEngineException;
import org.snakeyaml.engine.v2.exceptions.YamlEngineException;
import org.snakeyaml.engine.v2.nodes.Node;
import org.snakeyaml.engine.v2.nodes.ScalarNode;
import org.snakeyaml.engine.v2.nodes.Tag;
import org.snakeyaml.engine.v2.representer.StandardRepresenter;
import org.snakeyaml.engine.v2.schema.CoreSchema;

import javax.annotation.Nonnull;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * This class contains utility methods to load standard yaml file and convert object to standard
 * yaml syntax.
 */
public class YamlParserUtils {

    private static final Logger LOG = LoggerFactory.getLogger(YamlParserUtils.class);

    private static final DumpSettings blockerDumperSettings =
            DumpSettings.builder()
                    .setDefaultFlowStyle(FlowStyle.BLOCK)
                    // Disable split long lines to avoid add unexpected line breaks
                    .setSplitLines(false)
                    .setSchema(new CoreSchema())
                    .build();

    private static final DumpSettings flowDumperSettings =
            DumpSettings.builder()
                    .setDefaultFlowStyle(FlowStyle.FLOW)
                    // Disable split long lines to avoid add unexpected line breaks
                    .setSplitLines(false)
                    .setSchema(new CoreSchema())
                    .build();

    private static final Dump blockerDumper =
            new Dump(blockerDumperSettings, new FlinkConfigRepresenter(blockerDumperSettings));

    private static final Dump flowDumper =
            new Dump(flowDumperSettings, new FlinkConfigRepresenter(flowDumperSettings));

    private static final Load loader =
            new Load(LoadSettings.builder().setSchema(new CoreSchema()).build());

    /**
     * Loads the contents of the given YAML file into a map.
     *
     * @param file the YAML file to load.
     * @return a non-null map representing the YAML content. If the file is empty or only contains
     *     comments, an empty map is returned.
     * @throws FileNotFoundException if the YAML file is not found.
     * @throws YamlEngineException if the file cannot be parsed.
     * @throws IOException if an I/O error occurs while reading from the file stream.
     */
    public static synchronized @Nonnull Map<String, Object> loadYamlFile(File file)
            throws Exception {
        try (FileInputStream inputStream = new FileInputStream((file))) {
            Map<String, Object> yamlResult =
                    (Map<String, Object>) loader.loadFromInputStream(inputStream);
            return yamlResult == null ? new HashMap<>() : yamlResult;
        } catch (FileNotFoundException e) {
            LOG.error("Failed to find YAML file", e);
            throw e;
        } catch (IOException | YamlEngineException e) {
            if (e instanceof MarkedYamlEngineException) {
                YamlEngineException exception =
                        wrapExceptionToHiddenSensitiveData((MarkedYamlEngineException) e);
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
            String output = flowDumper.dumpToString(value);
            // remove the line break
            String linebreak = flowDumperSettings.getBestLineBreak();
            if (output.endsWith(linebreak)) {
                output = output.substring(0, output.length() - linebreak.length());
            }
            return output;
        } catch (MarkedYamlEngineException exception) {
            throw wrapExceptionToHiddenSensitiveData(exception);
        }
    }

    /**
     * Converts a flat map into a nested map structure and outputs the result as a list of
     * YAML-formatted strings. Each item in the list represents a single line of the YAML data. The
     * method is synchronized and thus thread-safe.
     *
     * @param flattenMap A map containing flattened keys (e.g., "parent.child.key") associated with
     *     their values.
     * @return A list of strings that represents the YAML data, where each item corresponds to a
     *     line of the data.
     */
    @SuppressWarnings("unchecked")
    public static synchronized List<String> convertAndDumpYamlFromFlatMap(
            Map<String, Object> flattenMap) {
        try {
            Map<String, Object> nestedMap = new LinkedHashMap<>();
            for (Map.Entry<String, Object> entry : flattenMap.entrySet()) {
                String[] keys = entry.getKey().split("\\.");
                Map<String, Object> currentMap = nestedMap;
                for (int i = 0; i < keys.length - 1; i++) {
                    currentMap =
                            (Map<String, Object>)
                                    currentMap.computeIfAbsent(keys[i], k -> new LinkedHashMap<>());
                }
                currentMap.put(keys[keys.length - 1], entry.getValue());
            }
            String data = blockerDumper.dumpToString(nestedMap);
            String linebreak = blockerDumperSettings.getBestLineBreak();
            return Arrays.asList(data.split(linebreak));
        } catch (MarkedYamlEngineException exception) {
            throw wrapExceptionToHiddenSensitiveData(exception);
        }
    }

    public static synchronized <T> T convertToObject(String value, Class<T> type) {
        try {
            return type.cast(loader.loadFromString(value));
        } catch (MarkedYamlEngineException exception) {
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
     * @param exception The MarkedYamlEngineException containing potentially sensitive data.
     * @return A YamlEngineException with a message that has sensitive data hidden.
     */
    private static YamlEngineException wrapExceptionToHiddenSensitiveData(
            MarkedYamlEngineException exception) {
        StringBuilder lines = new StringBuilder();
        String context = exception.getContext();
        Optional<Mark> contextMark = exception.getContextMark();
        Optional<Mark> problemMark = exception.getProblemMark();
        String problem = exception.getProblem();

        if (context != null) {
            lines.append(context);
            lines.append("\n");
        }

        if (contextMark.isPresent()
                && (problem == null
                        || !problemMark.isPresent()
                        || contextMark.get().getName().equals(problemMark.get().getName())
                        || contextMark.get().getLine() != problemMark.get().getLine()
                        || contextMark.get().getColumn() != problemMark.get().getColumn())) {
            lines.append(hiddenSensitiveDataInMark(contextMark.get()));
            lines.append("\n");
        }

        if (problem != null) {
            lines.append(problem);
            lines.append("\n");
        }

        if (problemMark.isPresent()) {
            lines.append(hiddenSensitiveDataInMark(problemMark.get()));
            lines.append("\n");
        }

        Throwable cause = exception.getCause();
        if (cause instanceof MarkedYamlEngineException) {
            cause = wrapExceptionToHiddenSensitiveData((MarkedYamlEngineException) cause);
        }

        YamlEngineException yamlException = new YamlEngineException(lines.toString(), cause);
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

    private static class FlinkConfigRepresenter extends StandardRepresenter {
        public FlinkConfigRepresenter(DumpSettings dumpSettings) {
            super(dumpSettings);
            representers.put(Duration.class, this::representDuration);
            representers.put(MemorySize.class, this::representMemorySize);
            parentClassRepresenters.put(Enum.class, this::representEnum);
        }

        private Node representDuration(Object data) {
            Duration duration = (Duration) data;
            String durationString = TimeUtils.formatWithHighestUnit(duration);
            return new ScalarNode(Tag.STR, durationString, settings.getDefaultScalarStyle());
        }

        private Node representMemorySize(Object data) {
            MemorySize memorySize = (MemorySize) data;
            return new ScalarNode(Tag.STR, memorySize.toString(), settings.getDefaultScalarStyle());
        }

        private Node representEnum(Object data) {
            return new ScalarNode(Tag.STR, data.toString(), settings.getDefaultScalarStyle());
        }
    }
}
