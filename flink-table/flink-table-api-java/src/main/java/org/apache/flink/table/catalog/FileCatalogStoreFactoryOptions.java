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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.nio.charset.StandardCharsets;

/** {@link ConfigOption}s for {@link FileCatalogStoreFactory}. */
public class FileCatalogStoreFactoryOptions {

    public static final String IDENTIFIER = "file";

    public static final ConfigOption<String> PATH =
            ConfigOptions.key("path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The configuration option for specifying the path to the file catalog store.");

    public static final ConfigOption<String> CHARSET =
            ConfigOptions.key("charset")
                    .stringType()
                    .defaultValue(StandardCharsets.UTF_8.displayName())
                    .withDescription(
                            "The charset used for storing/reading the catalog configuration.");

    private FileCatalogStoreFactoryOptions() {}
}
