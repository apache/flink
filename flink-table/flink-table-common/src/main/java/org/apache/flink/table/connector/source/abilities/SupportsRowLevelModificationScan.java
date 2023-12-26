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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.connector.RowLevelModificationScanContext;
import org.apache.flink.table.connector.sink.abilities.SupportsRowLevelDelete;
import org.apache.flink.table.connector.sink.abilities.SupportsRowLevelUpdate;
import org.apache.flink.table.connector.source.ScanTableSource;

import javax.annotation.Nullable;

/**
 * Interface for {@link ScanTableSource}s that support the row-level modification. The table source
 * is responsible for returning the information described by {@link
 * RowLevelModificationScanContext}. The context will be propagated to the sink which implements
 * {@link SupportsRowLevelUpdate} or {@link SupportsRowLevelDelete}.
 *
 * <p>Note: This interface is optional for table sources to implement. For cases where the table
 * source neither needs to know the type of row-level modification nor propagate information to
 * sink, the table source doesn't need to implement this interface. See more details at {@link
 * #applyRowLevelModificationScan(RowLevelModificationType, RowLevelModificationScanContext)}.
 */
@PublicEvolving
public interface SupportsRowLevelModificationScan {

    /**
     * Applies the type of row-level modification and the previous {@link
     * RowLevelModificationScanContext} returned by previous table source scan, return a new {@link
     * RowLevelModificationScanContext}. If the table source is the last one, the {@link
     * RowLevelModificationScanContext} will be passed to the table sink. Otherwise, it will be
     * passed to the following table source.
     *
     * <p>Note: For the all tables in the UPDATE/DELETE statement, this method will be involved for
     * the corresponding table source scan.
     *
     * <p>Note: It may have multiple table sources in the case of sub-query. In such case, it will
     * return multiple {@link RowLevelModificationScanContext}s. To handle such case, the planner
     * will also pass the previous {@link RowLevelModificationScanContext} to the current table
     * source scan which is expected to decide what to do with the previous {@link
     * RowLevelModificationScanContext}. The order is consistent with the compilation order of the
     * table sources. The planer will only pass the last context returned to the sink.
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
    @PublicEvolving
    enum RowLevelModificationType {
        UPDATE,

        DELETE
    }
}
