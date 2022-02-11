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
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.types.DataType;

import java.util.List;

/** Abstract File system table for providing some common methods. */
abstract class AbstractFileSystemTable {

    final ObjectIdentifier tableIdentifier;
    final Configuration tableOptions;
    final DataType physicalRowDataType;
    final Path path;
    final String defaultPartName;

    List<String> partitionKeys;

    AbstractFileSystemTable(
            ObjectIdentifier tableIdentifier,
            DataType physicalRowDataType,
            List<String> partitionKeys,
            ReadableConfig tableOptions) {
        this.tableIdentifier = tableIdentifier;
        this.tableOptions = (Configuration) tableOptions;
        this.physicalRowDataType = physicalRowDataType;
        this.path = new Path(this.tableOptions.get(FileSystemConnectorOptions.PATH));
        this.defaultPartName =
                this.tableOptions.get(FileSystemConnectorOptions.PARTITION_DEFAULT_NAME);
        this.partitionKeys = partitionKeys;
    }
}
