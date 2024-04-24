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

package org.apache.flink.connector.file.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.TestFileSource;
import org.apache.flink.connector.file.src.enumerate.BlockSplittingRecursiveAllDirEnumerator;
import org.apache.flink.connector.file.src.enumerate.NonSplittingRecursiveAllDirEnumerator;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

import javax.annotation.Nullable;

import java.util.List;

/** Test file system table source. */
@Internal
public class TestFileSystemTableSource extends FileSystemTableSource {

    public TestFileSystemTableSource(
            ObjectIdentifier tableIdentifier,
            DataType physicalRowDataType,
            List<String> partitionKeys,
            ReadableConfig tableOptions,
            @Nullable DecodingFormat<BulkFormat<RowData, FileSourceSplit>> bulkReaderFormat,
            @Nullable DecodingFormat<DeserializationSchema<RowData>> deserializationFormat) {
        super(
                tableIdentifier,
                physicalRowDataType,
                partitionKeys,
                tableOptions,
                bulkReaderFormat,
                deserializationFormat);
    }

    @Override
    protected SourceProvider createSourceProvider(BulkFormat<RowData, FileSourceSplit> bulkFormat) {
        final TestFileSource.TestFileSourceBuilder<RowData> fileSourceBuilder =
                TestFileSource.forBulkFileFormat(bulkFormat, paths());

        tableOptions
                .getOptional(FileSystemConnectorOptions.SOURCE_MONITOR_INTERVAL)
                .ifPresent(fileSourceBuilder::monitorContinuously);
        tableOptions
                .getOptional(FileSystemConnectorOptions.SOURCE_PATH_REGEX_PATTERN)
                .ifPresent(
                        regex ->
                                fileSourceBuilder.setFileEnumerator(
                                        bulkFormat.isSplittable()
                                                ? () ->
                                                        new BlockSplittingRecursiveAllDirEnumerator(
                                                                regex)
                                                : () ->
                                                        new NonSplittingRecursiveAllDirEnumerator(
                                                                regex)));

        boolean isStreamingMode =
                tableOptions.get(ExecutionOptions.RUNTIME_MODE) == RuntimeExecutionMode.STREAMING;
        fileSourceBuilder.setStreamingMode(isStreamingMode);

        return SourceProvider.of(fileSourceBuilder.build());
    }
}
