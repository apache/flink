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

package org.apache.flink.table.operations.materializedtable;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.internal.TableResultInternal;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.operations.OperationUtils;

import javax.annotation.Nullable;

import java.util.Map;

/**
 * Operation to describe clause like: ALTER MATERIALIZED TABLE [catalog_name.][db_name.]table_name
 * REFRESH [PARTITION (key1=val1, key2=val2, ...)].
 */
@Internal
public class AlterMaterializedTableRefreshOperation extends AlterMaterializedTableOperation {

    private final Map<String, String> partitionSpec;
    private final boolean isPeriodic;
    private final @Nullable String scheduleTime;
    private final Map<String, String> dynamicOptions;

    public AlterMaterializedTableRefreshOperation(
            ObjectIdentifier tableIdentifier, Map<String, String> partitionSpec) {
        this(tableIdentifier, partitionSpec, false, null, Map.of());
    }

    private AlterMaterializedTableRefreshOperation(
            ObjectIdentifier tableIdentifier,
            Map<String, String> partitionSpec,
            boolean isPeriodic,
            @Nullable String scheduleTime,
            Map<String, String> dynamicOptions) {
        super(tableIdentifier);
        this.partitionSpec = partitionSpec;
        this.isPeriodic = isPeriodic;
        this.scheduleTime = scheduleTime;
        this.dynamicOptions = dynamicOptions;
    }

    public static AlterMaterializedTableRefreshOperation oneTime(
            ObjectIdentifier tableIdentifier,
            Map<String, String> partitionSpec,
            Map<String, String> dynamicOptions) {
        return new AlterMaterializedTableRefreshOperation(
                tableIdentifier, partitionSpec, false, null, dynamicOptions);
    }

    public static AlterMaterializedTableRefreshOperation periodic(
            ObjectIdentifier tableIdentifier,
            @Nullable String scheduleTime,
            Map<String, String> dynamicOptions) {
        return new AlterMaterializedTableRefreshOperation(
                tableIdentifier, Map.of(), true, scheduleTime, dynamicOptions);
    }

    @Override
    public TableResultInternal execute(Context ctx) {
        // TableEnvironmentImpl#executeInternal routes every MaterializedTableOperation
        // to the MaterializedTableExecutor before ExecutableOperation dispatch.
        throw new UnsupportedOperationException(
                "AlterMaterializedTableRefreshOperation does not support ExecutableOperation yet.");
    }

    public Map<String, String> getPartitionSpec() {
        return partitionSpec;
    }

    public boolean isPeriodic() {
        return isPeriodic;
    }

    public @Nullable String getScheduleTime() {
        return scheduleTime;
    }

    public Map<String, String> getDynamicOptions() {
        return dynamicOptions;
    }

    @Override
    public String asSummaryString() {
        StringBuilder sb =
                new StringBuilder(
                        String.format("ALTER MATERIALIZED TABLE %s REFRESH", tableIdentifier));
        if (!partitionSpec.isEmpty()) {
            sb.append(
                    String.format(
                            " PARTITION (%s)", OperationUtils.formatPartitionSpec(partitionSpec)));
        }

        return sb.toString();
    }
}
