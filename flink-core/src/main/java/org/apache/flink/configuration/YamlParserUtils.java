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
import org.apache.flink.shaded.jackson2.org.yaml.snakeyaml.Yaml;
import org.apache.flink.shaded.jackson2.org.yaml.snakeyaml.error.Mark;
import org.apache.flink.shaded.jackson2.org.yaml.snakeyaml.error.MarkedYAMLException;
import org.apache.flink.shaded.jackson2.org.yaml.snakeyaml.error.YAMLException;
import org.apache.flink.shaded.jackson2.org.yaml.snakeyaml.nodes.Node;
import org.apache.flink.shaded.jackson2.org.yaml.snakeyaml.nodes.Tag;
import org.apache.flink.shaded.jackson2.org.yaml.snakeyaml.representer.Represent;
import org.apache.flink.shaded.jackson2.org.yaml.snakeyaml.representer.Representer;

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

    private static final Yaml yaml;

    static {
        DumperOptions options = new DumperOptions();
        // Make the dump output is in single line
        options.setDefaultFlowStyle(DumperOptions.FlowStyle.FLOW);
        options.setWidth(Integer.MAX_VALUE);

        Representer representer = new CustomRepresenter(options);

        yaml = new Yaml(representer, options);
    }

    public static Map<String, Object> loadYamlFile(File file) {
        try (FileInputStream inputStream = new FileInputStream((file))) {
            return yaml.load(inputStream);
        } catch (FileNotFoundException e) {
            throw new RuntimeException("Error finding YAML configuration file.", e);
        } catch (IOException | YAMLException e) {
            if (e instanceof MarkedYAMLException) {
                throw new RuntimeException(
                        "Error parsing YAML configuration.",
                        wrapExceptionToHiddenSensitiveData((MarkedYAMLException) e));
            }
            throw new RuntimeException("Error parsing YAML configuration.", e);
        }
    }

    public static String convertToString(Object value) {
        try {
            String output = yaml.dump(value);
            if (output.endsWith("\n") || output.endsWith("\r")) {
                output = output.substring(0, output.length() - 1);
            }
            return output;
        } catch (MarkedYAMLException exception) {
            throw wrapExceptionToHiddenSensitiveData(exception);
        }
    }

    public static Object convertToObject(String value) {
        try {
            return yaml.load(value);
        } catch (MarkedYAMLException exception) {
            throw wrapExceptionToHiddenSensitiveData(exception);
        }
    }

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

        YAMLException yamlException = new YAMLException(lines.toString(), exception.getCause());
        yamlException.setStackTrace(exception.getStackTrace());
        return yamlException;
    }

    private static String hiddenSensitiveDataInMark(Mark mark) {
        return " in "
                + mark.getName()
                + ", line "
                + (mark.getLine() + 1)
                + ", column "
                + (mark.getColumn() + 1);
    }

    private static class CustomRepresenter extends Representer {
        public CustomRepresenter(DumperOptions options) {
            super(options);
            representers.put(Duration.class, new RepresentDuration());
            representers.put(MemorySize.class, new RepresentMemorySize());
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
    }
}
