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

package org.apache.flink.connectors.hive.read;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.util.CheckpointedPosition;
import org.apache.flink.connectors.hive.HiveTablePartition;
import org.apache.flink.core.fs.Path;

import org.apache.hadoop.mapred.FileSplit;

import javax.annotation.Nullable;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A sub-class of {@link FileSourceSplit} that contains extra information needed to read a hive
 * table.
 */
@PublicEvolving
public class HiveSourceSplit extends FileSourceSplit {

    private static final long serialVersionUID = 1L;

    private final HiveTablePartition hiveTablePartition;

    public HiveSourceSplit(
            FileSplit fileSplit,
            HiveTablePartition hiveTablePartition,
            @Nullable CheckpointedPosition readerPosition)
            throws IOException {
        this(
                fileSplit.toString(),
                new Path(fileSplit.getPath().toString()),
                fileSplit.getStart(),
                fileSplit.getLength(),
                fileSplit.getLocations(),
                readerPosition,
                hiveTablePartition);
    }

    public HiveSourceSplit(
            String id,
            Path filePath,
            long offset,
            long length,
            String[] hostnames,
            @Nullable CheckpointedPosition readerPosition,
            HiveTablePartition hiveTablePartition) {
        super(id, filePath, offset, length, hostnames, readerPosition);
        this.hiveTablePartition =
                checkNotNull(hiveTablePartition, "hiveTablePartition can not be null");
    }

    public HiveTablePartition getHiveTablePartition() {
        return hiveTablePartition;
    }

    public FileSplit toMapRedSplit() {
        return new FileSplit(
                new org.apache.hadoop.fs.Path(path().toString()), offset(), length(), hostnames());
    }

    @Override
    public FileSourceSplit updateWithCheckpointedPosition(@Nullable CheckpointedPosition position) {
        return new HiveSourceSplit(
                splitId(), path(), offset(), length(), hostnames(), position, hiveTablePartition);
    }

    @Override
    public String toString() {
        return "HiveSourceSplit{"
                + String.format("Path=%s, ", path())
                + String.format("Offset=%d, ", offset())
                + String.format("Length=%d, ", length())
                + String.format(
                        "Position=%s, ",
                        getReaderPosition().map(CheckpointedPosition::toString).orElse("null"))
                + String.format("HiveTablePartition=%s", getHiveTablePartition())
                + '}';
    }
}
