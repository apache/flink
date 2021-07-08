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

package org.apache.flink.connector.jdbc.catalog.factory;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.connector.jdbc.catalog.JdbcCatalog;
import org.apache.flink.table.catalog.CommonCatalogOptions;

/** {@link ConfigOption}s for {@link JdbcCatalog}. */
@Internal
public class JdbcCatalogFactoryOptions {

    public static final String IDENTIFIER = "jdbc";

    public static final ConfigOption<String> DEFAULT_DATABASE =
            ConfigOptions.key(CommonCatalogOptions.DEFAULT_DATABASE_KEY)
                    .stringType()
                    .noDefaultValue();

    public static final ConfigOption<String> USERNAME =
            ConfigOptions.key("username").stringType().noDefaultValue();

    public static final ConfigOption<String> PASSWORD =
            ConfigOptions.key("password").stringType().noDefaultValue();

    public static final ConfigOption<String> BASE_URL =
            ConfigOptions.key("base-url").stringType().noDefaultValue();

    private JdbcCatalogFactoryOptions() {}
}
