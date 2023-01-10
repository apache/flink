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

package org.apache.flink.table.connector.source.abilities;

import org.apache.flink.table.connector.RowLevelModificationScanContext;
import org.apache.flink.table.connector.sink.abilities.SupportsRowLevelDelete;
import org.apache.flink.table.connector.sink.abilities.SupportsRowLevelUpdate;
import org.apache.flink.table.connector.source.ScanTableSource;

import javax.annotation.Nullable;

/**
 * Enables to tell the {@link ScanTableSource} the type of row-level modification the table source
 * scan is for, and return {@link RowLevelModificationScanContext} which will then be passed to the
 * sink which implements {@link SupportsRowLevelUpdate} or {@link SupportsRowLevelDelete}. It allows
 * the table source to pass information to the table sink which is to be updated/deleted.
 *
 * <p>Note: This interface is optional for table source to support update/delete existing data. For
 * the case that the table source won't need to know the type of row-level modification or pass
 * information to sink, the table source doesn't need to implement it.
 *
 * <p>Note: Only if the {@link ScanTableSource} implements this interface, and the table is to be
 * scanned to update/delete a table, the method {@link #applyRowLevelModificationScan} for the
 * corresponding table source will be involved . For more details, please see the method {@link
 * #applyRowLevelModificationScan}.
 */
public interface SupportsRowLevelModificationScan {

    /**
     * Apply the type of row-level modification and the previous {@link
     * RowLevelModificationScanContext} returned by previous table source scan, return a new {@link
     * RowLevelModificationScanContext} which will then finally to be passed to the table sink.
     *
     * <p>Note: for the all tables in the UPDATE/DELETE statement, this method will be involved for
     * the corresponding table source scan.
     *
     * <p>Note: it may have multiple table sources in the case of sub-query. In such case, it will
     * return multiple {@link RowLevelModificationScanContext}. To handle such case, the planner
     * will also pass the previous {@link RowLevelModificationScanContext} to the current table
     * source scan to make it decide how to deal with the previous {@link
     * RowLevelModificationScanContext}. The order is consistent to the order that the table source
     * compiles. The planer will only pass the last context returned to the sink.
     *
     * @param previousContext the context returned by previous table source, if there's no previous
     *     context, it'll be null.
     */
    RowLevelModificationScanContext applyRowLevelModificationScan(
            RowLevelModificationType rowLevelModificationType,
            @Nullable RowLevelModificationScanContext previousContext);

    /**
     * Type of the row-level modification for table.
     *
     * <p>Currently, two types are supported:
     *
     * <ul>
     *   <li>UPDATE
     *   <li>DELETE
     * </ul>
     */
    enum RowLevelModificationType {
        UPDATE,
        DELETE
    }
}
