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

package org.apache.flink.table.sinks;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.core.fs.FileSystem;

import static org.apache.flink.configuration.ConfigOptions.key;

/**
 * All {@link ConfigOption}s supported in {@link LegacyCsvDynamicTableSinkFactory}.
 *
 * <p>More {@link ConfigOption}s can be added to work around using deprecated {@link CsvTableSink}.
 *
 * @deprecated The legacy CSV connector has been replaced by {@code FileSink}. It is kept only to
 *     support tests for the legacy connector stack.
 */
@Internal
@Deprecated
public class LegacyCsvDynamicTableSinkOptions {

    public static final String IDENTIFIER = "legacy-csv";

    public static final ConfigOption<String> PATH =
            key("path").stringType().noDefaultValue().withDescription("The path of a file");

    public static final ConfigOption<String> FIELD_DELIM =
            key("field-delimiter")
                    .stringType()
                    .defaultValue(",")
                    .withDescription("The delimiter between each field in a single row");

    public static final ConfigOption<Integer> NUM_FILES =
            key("num-files")
                    .intType()
                    .defaultValue(-1)
                    .withDescription("The number of files to be created");

    public static final ConfigOption<FileSystem.WriteMode> WRITE_MODE =
            key("write-mode")
                    .enumType(FileSystem.WriteMode.class)
                    .noDefaultValue()
                    .withDescription("The write mode when writing to the file");
}
