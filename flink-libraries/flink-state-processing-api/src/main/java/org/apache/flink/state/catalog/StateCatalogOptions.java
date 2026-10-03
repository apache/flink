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

package org.apache.flink.state.catalog;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/** Configuration options for {@link StateCatalog}. */
@PublicEvolving
public class StateCatalogOptions {

    /**
     * Prefix for directory options. Each option of the form {@code directory.{label}} maps a
     * human-readable label to a filesystem path. The label becomes the first segment of every
     * database name derived from that directory (e.g. {@code my-app/savepoint-abc}).
     */
    public static final String DIRECTORY_PREFIX = "directory.";

    public static final ConfigOption<Integer> LISTING_PARALLELISM =
            ConfigOptions.key("listing-parallelism")
                    .intType()
                    .defaultValue(10)
                    .withDescription(
                            "Maximum number of concurrent directory listing requests issued "
                                    + "during a scan. Directories at the same depth are listed "
                                    + "in parallel. Increase for high-latency remote filesystems "
                                    + "(e.g. S3); decrease to reduce load on the filesystem.");

    public static final ConfigOption<Boolean> DB_NAME_INCLUDE_TS =
            ConfigOptions.key("db-name.include-ts")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Whether derived database names include the snapshot's creation "
                                    + "timestamp as a segment, i.e. label/creationTs/relativePath "
                                    + "instead of label/relativePath. The timestamp is the "
                                    + "modification time of the snapshot's _metadata file, "
                                    + "formatted with yyyy-MM-dd'T'HH:mm:ssX (e.g. "
                                    + "2026-07-22T10:30:45Z).");

    private StateCatalogOptions() {}
}
