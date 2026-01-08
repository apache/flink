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

package org.apache.flink.table.secret;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/** A collection of {@link ConfigOption} which are used for secret store configuration. */
@Internal
public class CommonSecretOptions {

    public static final String DEFAULT_SECRET_STORE_KIND = "default_in_memory";
    public static final ConfigOption<String> TABLE_SECRET_STORE_KIND =
            ConfigOptions.key("table.secret-store.kind")
                    .stringType()
                    .defaultValue(DEFAULT_SECRET_STORE_KIND)
                    .withDescription(
                            "The kind of secret store to be used. Out of the box, 'default_in_memory' option is supported. "
                                    + "Implementations can provide custom secret stores for different backends "
                                    + "(e.g., cloud-specific secret managers).");

    /** Used to filter the specific options for secret store. */
    public static final String TABLE_SECRET_STORE_OPTION_PREFIX = "table.secret-store.";
}
