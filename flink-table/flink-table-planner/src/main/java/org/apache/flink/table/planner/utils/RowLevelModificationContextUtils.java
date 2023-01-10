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

package org.apache.flink.table.planner.utils;

import org.apache.flink.table.connector.RowLevelModificationScanContext;
import org.apache.flink.table.connector.source.abilities.SupportsRowLevelModificationScan;

/** Utils for put/get in current thread. */
public class RowLevelModificationContextUtils {
    private static final ThreadLocal<SupportsRowLevelModificationScan.RowLevelModificationType>
            modificationTypeThreadLocal = new ThreadLocal<>();

    private static final ThreadLocal<RowLevelModificationScanContext> scanContextThreadLocal =
            new ThreadLocal<>();

    public static void putRowLevelModificationType(
            SupportsRowLevelModificationScan.RowLevelModificationType rowLevelModificationType) {
        modificationTypeThreadLocal.set(rowLevelModificationType);
    }

    public static SupportsRowLevelModificationScan.RowLevelModificationType
            getRowLevelModificationType() {
        return modificationTypeThreadLocal.get();
    }

    public static void putRowLevelModificationScanContext(
            RowLevelModificationScanContext scanContext) {
        scanContextThreadLocal.set(scanContext);
    }

    public static RowLevelModificationScanContext getScanContext() {
        return scanContextThreadLocal.get();
    }
}
