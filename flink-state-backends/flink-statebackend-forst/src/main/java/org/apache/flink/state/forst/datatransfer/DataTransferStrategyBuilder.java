/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.state.forst.datatransfer;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.state.forst.fs.filemapping.FileOwnershipDecider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Builder for {@link DataTransferStrategy}. */
public class DataTransferStrategyBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(DataTransferStrategyBuilder.class);

    private FileSystem dbDelegateFileSystem;

    private FileOwnershipDecider fileOwnershipDecider;

    public void withoutDbRemotePath() {
        this.dbDelegateFileSystem = null;
    }

    public void withDbRemotePath(FileSystem dbDelegateFileSystem, Path dbRemotePath) {
        this.dbDelegateFileSystem = dbDelegateFileSystem;
    }

    public void withFileOwnershipDecider(FileOwnershipDecider fileOwnershipDecider) {
        this.fileOwnershipDecider = fileOwnershipDecider;
    }

    public DataTransferStrategyBuilder() {
        this.dbDelegateFileSystem = null;
        this.fileOwnershipDecider = FileOwnershipDecider.getDefault();
    }

    public DataTransferStrategy build() {
        DataTransferStrategy strategy;
        if (dbDelegateFileSystem == null || !fileOwnershipDecider.isDbPathUnderCheckpointPath()) {
            strategy =
                    dbDelegateFileSystem == null
                            ? new CopyDataTransferStrategy()
                            : new CopyDataTransferStrategy(dbDelegateFileSystem);
            LOG.info("Build DataTransferStrategy: {}", strategy);
            return strategy;
        }

        strategy = new ReusableDataTransferStrategy(fileOwnershipDecider, dbDelegateFileSystem);
        LOG.info("Build DataTransferStrategy: {}", strategy);
        return strategy;
    }
}
