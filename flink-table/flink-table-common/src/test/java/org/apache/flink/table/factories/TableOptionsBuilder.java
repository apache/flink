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

package org.apache.flink.table.factories;

import org.apache.flink.configuration.ConfigOption;

import java.util.HashMap;
import java.util.Map;

/**
 * Convenience syntax for constructing option maps for testing {@link DynamicTableFactory}
 * implementations.
 */
public class TableOptionsBuilder {
    private final Map<String, String> options;
    private final String connector;
    private final String format;

    public TableOptionsBuilder(String connector, String format) {
        this.options = new HashMap<>();
        this.connector = connector;
        this.format = format;
    }

    public TableOptionsBuilder withTableOption(ConfigOption<?> option, String value) {
        return withTableOption(option.key(), value);
    }

    public TableOptionsBuilder withFormatOption(ConfigOption<?> option, String value) {
        return withFormatOption(format + "." + option.key(), value);
    }

    public TableOptionsBuilder withTableOption(String key, String value) {
        options.put(key, value);
        return this;
    }

    public TableOptionsBuilder withFormatOption(String key, String value) {
        options.put(key, value);
        return this;
    }

    public Map<String, String> build() {
        withTableOption(FactoryUtil.CONNECTOR, connector);
        withTableOption(FactoryUtil.FORMAT, format);
        return options;
    }
}
