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
}
