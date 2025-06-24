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

import org.apache.flink.annotation.Internal;
import org.apache.flink.sql.parser.ddl.SqlRefreshMode;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogMaterializedTable;
import org.apache.flink.table.catalog.IntervalFreshness;

import org.apache.calcite.sql.SqlIntervalLiteral;
import org.apache.calcite.sql.type.SqlTypeFamily;

import java.time.Duration;

/** The utils for materialized table. */
@Internal
public class MaterializedTableUtils {

    public static IntervalFreshness getMaterializedTableFreshness(
            SqlIntervalLiteral sqlIntervalLiteral) {
        if (sqlIntervalLiteral.signum() < 0) {
            throw new ValidationException(
                    "Materialized table freshness doesn't support negative value.");
        }
        if (sqlIntervalLiteral.getTypeName().getFamily() != SqlTypeFamily.INTERVAL_DAY_TIME) {
            throw new ValidationException(
                    "Materialized table freshness only support SECOND, MINUTE, HOUR, DAY as the time unit.");
        }

        SqlIntervalLiteral.IntervalValue intervalValue =
                sqlIntervalLiteral.getValueAs(SqlIntervalLiteral.IntervalValue.class);
        String interval = intervalValue.getIntervalLiteral();
        switch (intervalValue.getIntervalQualifier().typeName()) {
            case INTERVAL_DAY:
                return IntervalFreshness.ofDay(interval);
            case INTERVAL_HOUR:
                return IntervalFreshness.ofHour(interval);
            case INTERVAL_MINUTE:
                return IntervalFreshness.ofMinute(interval);
            case INTERVAL_SECOND:
                return IntervalFreshness.ofSecond(interval);
            default:
                throw new ValidationException(
                        "Materialized table freshness only support SECOND, MINUTE, HOUR, DAY as the time unit.");
        }
    }

    public static CatalogMaterializedTable.LogicalRefreshMode deriveLogicalRefreshMode(
            SqlRefreshMode sqlRefreshMode) {
        if (sqlRefreshMode == null) {
            return CatalogMaterializedTable.LogicalRefreshMode.AUTOMATIC;
        }

        switch (sqlRefreshMode) {
            case FULL:
                return CatalogMaterializedTable.LogicalRefreshMode.FULL;
            case CONTINUOUS:
                return CatalogMaterializedTable.LogicalRefreshMode.CONTINUOUS;
            default:
                throw new ValidationException(
                        String.format("Unsupported logical refresh mode: %s.", sqlRefreshMode));
        }
    }

    public static CatalogMaterializedTable.RefreshMode deriveRefreshMode(
            Duration threshold,
            Duration definedFreshness,
            CatalogMaterializedTable.LogicalRefreshMode definedRefreshMode) {
        // If the refresh mode is specified manually, use it directly.
        if (definedRefreshMode == CatalogMaterializedTable.LogicalRefreshMode.FULL) {
            return CatalogMaterializedTable.RefreshMode.FULL;
        } else if (definedRefreshMode == CatalogMaterializedTable.LogicalRefreshMode.CONTINUOUS) {
            return CatalogMaterializedTable.RefreshMode.CONTINUOUS;
        }

        // derive the actual refresh mode via defined freshness
        if (definedFreshness.compareTo(threshold) < 0) {
            return CatalogMaterializedTable.RefreshMode.CONTINUOUS;
        } else {
            return CatalogMaterializedTable.RefreshMode.FULL;
        }
    }
}
