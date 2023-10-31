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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** Abstract class for {@link SourceTestStep} and {@link SinkTestStep}. */
public abstract class TableTestStep implements TestStep {

    public final String name;
    public final List<String> schemaComponents;
    public final List<String> partitionKeys;
    public final Map<String, String> options;

    TableTestStep(
            String name,
            List<String> schemaComponents,
            List<String> partitionKeys,
            Map<String, String> options) {
        this.name = name;
        this.schemaComponents = schemaComponents;
        this.partitionKeys = partitionKeys;
        this.options = options;
    }

    public TableResult apply(TableEnvironment env) {
        return apply(env, Collections.emptyMap());
    }

    public TableResult apply(TableEnvironment env, Map<String, String> extraOptions) {
        final Map<String, String> allOptions = new HashMap<>(options);
        allOptions.putAll(extraOptions);

        final String partitionedBy =
                partitionKeys.isEmpty()
                        ? ""
                        : "PARTITIONED BY (" + String.join(", ", partitionKeys) + ")\n";
        final String createTable =
                String.format(
                        "CREATE TABLE %s (\n%s)\n%sWITH (\n%s)",
                        name,
                        String.join(",\n", schemaComponents),
                        partitionedBy,
                        allOptions.entrySet().stream()
                                .map(e -> String.format("'%s'='%s'", e.getKey(), e.getValue()))
                                .collect(Collectors.joining(",\n")));

        return env.executeSql(createTable);
    }

    /** Builder pattern for {@link SourceTestStep} and {@link SinkTestStep}. */
    @SuppressWarnings("unchecked")
    protected abstract static class AbstractBuilder<
            SpecificBuilder extends AbstractBuilder<SpecificBuilder>> {

        protected final String name;

        protected final List<String> schemaComponents = new ArrayList<>();
        protected final List<String> partitionKeys = new ArrayList<>();
        protected final Map<String, String> options = new HashMap<>();

        protected AbstractBuilder(String name) {
            this.name = name;
        }

        /**
         * Define the schema like you would in SQL e.g. "my_col INT", "PRIMARY KEY (uid) NOT
         * ENFORCED", or "WATERMARK FOR ts AS ts".
         */
        public SpecificBuilder addSchema(String... schemaComponents) {
            this.schemaComponents.addAll(Arrays.asList(schemaComponents));
            return (SpecificBuilder) this;
        }

        /**
         * Unless the test requires a very specific configuration, try to avoid calling this method
         * and fill in options later via {@link TableTestStep#apply(TableEnvironment, Map)}.
         */
        public SpecificBuilder addOptions(Map<String, String> options) {
            this.options.putAll(options);
            return (SpecificBuilder) this;
        }

        /**
         * Unless the test requires a very specific configuration, try to avoid calling this method
         * and fill in options later via {@link TableTestStep#apply(TableEnvironment, Map)}.
         */
        public SpecificBuilder addOption(String key, String value) {
            this.options.put(key, value);
            return (SpecificBuilder) this;
        }

        /**
         * Unless the test requires a very specific configuration, try to avoid calling this method
         * and fill in options later via {@link TableTestStep#apply(TableEnvironment, Map)}.
         */
        public <T> SpecificBuilder addOption(ConfigOption<T> option, String value) {
            this.options.put(option.key(), ConfigurationUtils.convertValue(value, String.class));
            return (SpecificBuilder) this;
        }

        public SpecificBuilder addPartitionKeys(String... partitionKeys) {
            this.partitionKeys.addAll(Arrays.asList(partitionKeys));
            return (SpecificBuilder) this;
        }
    }
}
