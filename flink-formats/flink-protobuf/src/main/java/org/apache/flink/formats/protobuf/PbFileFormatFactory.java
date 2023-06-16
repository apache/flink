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

package org.apache.flink.formats.protobuf;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.BulkWriter.Factory;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.file.table.factories.BulkReaderFormatFactory;
import org.apache.flink.connector.file.table.factories.BulkWriterFormatFactory;
import org.apache.flink.connector.file.table.format.BulkDecodingFormat;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableFactory;

import java.util.Collections;
import java.util.Set;

/**
 * Throw a {@link ValidationException} when using Protobuf format factory for file system.
 *
 * <p>In practice, there is <a href="https://protobuf.dev/programming-guides/techniques/#streaming">
 * no standard</a> for storing bulk protobuf messages. This factory is present to prevent falling
 * back to the {@link org.apache.flink.connector.file.table.DeserializationSchemaAdapter}, a
 * line-based format which could silently succeed but write unrecoverable data to disk.
 *
 * <p>If your use case requires storing bulk protobuf messages on disk, the parquet file format
 * might be the appropriate container and has an API for mapping records to protobuf messages.
 */
@Internal
public class PbFileFormatFactory implements BulkReaderFormatFactory, BulkWriterFormatFactory {

    @Override
    public String factoryIdentifier() {
        return PbFormatFactory.IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Collections.emptySet();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Collections.emptySet();
    }

    @Override
    public Set<ConfigOption<?>> forwardOptions() {
        return Collections.emptySet();
    }

    @Override
    public BulkDecodingFormat<RowData> createDecodingFormat(
            DynamicTableFactory.Context context, ReadableConfig formatOptions) {
        throw new ValidationException(
                "The 'protobuf' format is not supported for the 'filesystem' connector.");
    }

    @Override
    public EncodingFormat<Factory<RowData>> createEncodingFormat(
            DynamicTableFactory.Context context, ReadableConfig formatOptions) {
        throw new ValidationException(
                "The 'protobuf' format is not supported for the 'filesystem' connector.");
    }
}
