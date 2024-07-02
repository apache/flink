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

package org.apache.flink.sql.tests;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.HashSet;
import java.util.Set;

/** Table factory for the custom generator implemented in these tests. */
public class GeneratorTableSourceFactory implements DynamicTableSourceFactory {
    public static final String CONNECTOR_ID = "batch-sql-test-generator";
    public static final ConfigOption<Integer> NUM_KEYS =
            ConfigOptions.key("num-keys").intType().noDefaultValue();
    public static final ConfigOption<Float> ROWS_PER_KEY_PER_SECOND =
            ConfigOptions.key("rows-per-key-per-second").floatType().noDefaultValue();
    public static final ConfigOption<Integer> DURATION_SECONDS =
            ConfigOptions.key("duration-seconds").intType().noDefaultValue();
    public static final ConfigOption<Integer> OFFSET_SECONDS =
            ConfigOptions.key("offset-seconds").intType().noDefaultValue();

    public static Schema getSchema() {
        return Schema.newBuilder()
                .column("key", DataTypes.INT())
                .column("rowtime", DataTypes.TIMESTAMP(3))
                .column("payload", DataTypes.STRING())
                .build();
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper factoryHelper =
                FactoryUtil.createTableFactoryHelper(this, context);
        factoryHelper.validate();

        Generator generator =
                Generator.create(
                        factoryHelper.getOptions().get(NUM_KEYS),
                        factoryHelper.getOptions().get(ROWS_PER_KEY_PER_SECOND),
                        factoryHelper.getOptions().get(DURATION_SECONDS),
                        factoryHelper.getOptions().get(OFFSET_SECONDS));
        return new GeneratorTableSource(generator);
    }

    @Override
    public String factoryIdentifier() {
        return CONNECTOR_ID;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        HashSet<ConfigOption<?>> options = new HashSet<>();
        options.add(NUM_KEYS);
        options.add(ROWS_PER_KEY_PER_SECOND);
        options.add(DURATION_SECONDS);
        options.add(OFFSET_SECONDS);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return new HashSet<>();
    }
}
