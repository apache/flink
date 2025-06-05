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

package org.apache.flink.table.test.program;

import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Test step for registering a model. */
public class ModelTestStep implements TestStep {

    public final String name;
    public final List<String> inputSchema;
    public final List<String> outputSchema;
    public final Map<String, String> options;
    public final Map<Row, List<Row>> data;

    public ModelTestStep(
            String name,
            List<String> inputSchemaComponents,
            List<String> outputSchemaComponents,
            Map<String, String> options,
            Map<Row, List<Row>> data) {
        this.name = name;
        this.inputSchema = inputSchemaComponents;
        this.outputSchema = outputSchemaComponents;
        this.options = options;
        this.data = data;
    }

    public static Builder newBuilder(String name) {
        return new Builder(name);
    }

    @Override
    public TestKind getKind() {
        return TestKind.MODEL;
    }

    public TableResult apply(TableEnvironment env, Map<String, String> extraOptions) {
        final Map<String, String> allOptions = new HashMap<>(options);
        allOptions.putAll(extraOptions);
        final String createModel =
                String.format(
                        "CREATE MODEL `%s`\n" + "INPUT (%s)\n" + "OUTPUT (%s)\nWITH (\n%s)",
                        name,
                        String.join(",", inputSchema),
                        String.join(",", outputSchema),
                        allOptions.entrySet().stream()
                                .map(e -> String.format("'%s'='%s'", e.getKey(), e.getValue()))
                                .collect(Collectors.joining(",\n")));
        return env.executeSql(createModel);
    }

    /** Builder pattern for {@link ModelTestStep}. */
    public static class Builder {

        final String name;

        final List<String> inputSchemaComponents = new ArrayList<>();
        final List<String> outputSchemaComponents = new ArrayList<>();
        final Map<String, String> options = new HashMap<>();
        final Map<Row, List<Row>> data = new HashMap<>();

        public Builder(String name) {
            this.name = name;
        }

        public Builder addInputSchema(String... schemaComponents) {
            this.inputSchemaComponents.addAll(Arrays.asList(schemaComponents));
            return this;
        }

        public Builder addInputSchema(List<String> schemaComponents) {
            this.inputSchemaComponents.addAll(schemaComponents);
            return this;
        }

        public Builder addOutputSchema(String... schemaComponents) {
            this.outputSchemaComponents.addAll(Arrays.asList(schemaComponents));
            return this;
        }

        public Builder addOutputSchema(List<String> schemaComponents) {
            this.outputSchemaComponents.addAll(schemaComponents);
            return this;
        }

        public Builder addOption(String key, String value) {
            this.options.put(key, value);
            return this;
        }

        public Builder addOptions(Map<String, String> options) {
            this.options.putAll(options);
            return this;
        }

        public Builder data(Map<Row, List<Row>> data) {
            this.data.putAll(data);
            return this;
        }

        public ModelTestStep build() {
            return new ModelTestStep(
                    name, inputSchemaComponents, outputSchemaComponents, options, data);
        }
    }
}
