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

package org.apache.flink.table.connector.sink.abilities;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.core.execution.JobStatusHook;
import org.apache.flink.table.catalog.StagedTable;
import org.apache.flink.table.connector.sink.DynamicTableSink;

/**
 * Enables different staged operations to ensure atomicity in a {@link DynamicTableSink}.
 *
 * <p>By default, if this interface is not implemented, indicating that atomic operations are not
 * supported, then a non-atomic implementation is used.
 */
@PublicEvolving
public interface SupportsStaging {

    /**
     * Provides a {@link StagedTable} that provided transaction abstraction. StagedTable will be
     * combined with {@link JobStatusHook} to achieve atomicity support in the Flink framework. Call
     * the relevant API of StagedTable when the Job state is switched.
     *
     * <p>This method will be called at the compile stage.
     *
     * @param StagingContext Tell DynamicTableSink, the operation type of this StagedTable,
     *     expandable
     * @return {@link StagedTable} that can be serialized and provides atomic operations
     */
    StagedTable applyStaging(StagingContext context);

    /**
     * The context is intended to tell DynamicTableSink the type of this operation. In this way,
     * DynamicTableSink can return the corresponding implementation of StagedTable according to the
     * specific operation. More types of operations can be extended in the future.
     */
    interface StagingContext {
        StagingPurpose getStagingPurpose();
    }

    enum StagingPurpose {
        CREATE_TABLE_AS,
        CREATE_TABLE_AS_IF_NOT_EXISTS
    }
}
