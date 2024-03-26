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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.enumerate.NonSplittingRecursiveEnumerator;
import org.apache.flink.connector.file.src.reader.BulkFormat;
import org.apache.flink.connector.file.src.util.RecordAndPosition;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.data.RowData;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;

public class FileSystemBulkFormatPartitionReader implements PartitionReader<Path, RowData> {

    private static final long serialVersionUID = 1L;

    private final BulkFormat<RowData, FileSourceSplit> bulkFormat;
    private final Configuration configuration;

    private transient ArrayList<FileSourceSplit> fileSourceSplits;
    private transient BulkFormat.Reader<RowData> reader;
    private transient BulkFormat.RecordIterator<RowData> recordIterator;
    private transient int readingSplitId;

    public FileSystemBulkFormatPartitionReader(
            BulkFormat<RowData, FileSourceSplit> bulkFormat, ReadableConfig configuration) {
        this.bulkFormat = bulkFormat;
        checkArgument(configuration instanceof Configuration);
        this.configuration = (Configuration) configuration;
    }

    @Override
    public void open(List<Path> partitions) throws IOException {
        NonSplittingRecursiveEnumerator enumerator = new NonSplittingRecursiveEnumerator();
        fileSourceSplits = new ArrayList<>();
        fileSourceSplits.addAll(enumerator.enumerateSplits(partitions.toArray(new Path[0]), 1));
        readingSplitId = 0;
        if (fileSourceSplits.size() > 0) {
            reader = bulkFormat.createReader(configuration, fileSourceSplits.get(readingSplitId));
            recordIterator = reader.readBatch();
        }
    }

    @Nullable
    @Override
    public RowData read(RowData reuse) throws IOException {
        while (readingSplitId < fileSourceSplits.size()) {
            while (recordIterator != null) {
                RecordAndPosition<RowData> recordAndPosition = recordIterator.next();
                if (recordAndPosition != null) {
                    return recordAndPosition.getRecord();
                }
                recordIterator.releaseBatch();
                recordIterator = reader.readBatch();
            }
            readingSplitId++;
            if (readingSplitId < fileSourceSplits.size()) {
                reader =
                        bulkFormat.createReader(
                                configuration, fileSourceSplits.get(readingSplitId));
                recordIterator = reader.readBatch();
            }
        }
        return null;
    }

    @Override
    public void close() throws IOException {
        if (reader != null) {
            reader.close();
        }
    }
}
