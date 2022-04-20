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

package org.apache.flink.table.planner.delegation.hive.operation;

import org.apache.flink.table.catalog.ObjectPath;

import org.apache.hadoop.fs.Path;

/** HiveLoadOperation. */
public class HiveLoadOperation {
    private final Path path;
    private final ObjectPath tablePath;
    private final boolean isOverwrite;
    private final boolean isSrcLocal;

    public HiveLoadOperation(
            Path path, ObjectPath tablePath, boolean isOverwrite, boolean isSrcLocal) {
        this.path = path;
        this.tablePath = tablePath;
        this.isOverwrite = isOverwrite;
        this.isSrcLocal = isSrcLocal;
    }

    public Path getPath() {
        return path;
    }

    public ObjectPath getTablePath() {
        return tablePath;
    }

    public boolean isOverwrite() {
        return isOverwrite;
    }

    public boolean isSrcLocal() {
        return isSrcLocal;
    }
}
