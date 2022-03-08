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
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Partition commit policy to add success file to directory. Success file is configurable and empty
 * file.
 */
@Internal
public class SuccessFileCommitPolicy implements PartitionCommitPolicy {

    private static final Logger LOG = LoggerFactory.getLogger(SuccessFileCommitPolicy.class);

    private final String fileName;
    private final FileSystem fileSystem;

    public SuccessFileCommitPolicy(String fileName, FileSystem fileSystem) {
        this.fileName = fileName;
        this.fileSystem = fileSystem;
    }

    @Override
    public void commit(Context context) throws Exception {
        fileSystem
                .create(new Path(context.partitionPath(), fileName), FileSystem.WriteMode.OVERWRITE)
                .close();
        LOG.info("Committed partition {} with success file", context.partitionSpec());
    }
}
