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

package org.apache.flink.table.catalog;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.StringUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;

/** Abstract class for catalogs. */
@Internal
public abstract class AbstractCatalog implements Catalog {
    private final String catalogName;
    private final String defaultDatabase;

    public AbstractCatalog(String name, String defaultDatabase) {
        checkArgument(!StringUtils.isNullOrWhitespaceOnly(name), "name cannot be null or empty");
        checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(defaultDatabase),
                "defaultDatabase cannot be null or empty");

        this.catalogName = name;
        this.defaultDatabase = defaultDatabase;
    }

    public String getName() {
        return catalogName;
    }

    @Override
    public String getDefaultDatabase() {
        return defaultDatabase;
    }

    @Override
    public String explainCatalog() {
        StringBuilder sb = new StringBuilder();
        Map<String, String> extraExplainInfo = extraExplainInfo();
        sb.append("default database: ").append(defaultDatabase);
        if (!extraExplainInfo.isEmpty()) {
            sb.append("\n");
            sb.append(
                    extraExplainInfo.entrySet().stream()
                            .map(entry -> entry.getKey() + ": " + entry.getValue())
                            .collect(Collectors.joining("\n")));
        }

        // put the class name at the end
        sb.append("\n").append(Catalog.super.explainCatalog());
        return sb.toString();
    }

    /**
     * Extra explain information used to print by DESCRIBE CATALOG catalogName statement.
     *
     * <p>Note: The class name and default database name of this catalog are no need to be added.
     */
    protected Map<String, String> extraExplainInfo() {
        return new HashMap<>();
    }
}
