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

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.planner.calcite.CalciteConfig;
import org.apache.flink.table.planner.calcite.CalciteConfig$;
import org.apache.flink.table.planner.plan.utils.OperatorType;

import java.time.ZoneId;
import java.util.HashSet;
import java.util.Set;

import static java.time.ZoneId.SHORT_IDS;
import static org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS;
import static org.apache.flink.table.api.config.OptimizerConfigOptions.TABLE_OPTIMIZER_AGG_PHASE_STRATEGY;

/** Utility class for {@link TableConfig} related helper functions. */
public class TableConfigUtils {

    /**
     * Returns whether the given operator type is disabled.
     *
     * @param tableConfig TableConfig object
     * @param operatorType operator type to check
     * @return true if the given operator is disabled.
     */
    public static boolean isOperatorDisabled(TableConfig tableConfig, OperatorType operatorType) {
        String value = tableConfig.get(TABLE_EXEC_DISABLED_OPERATORS);
        if (value == null) {
            return false;
        }
        String[] operators = value.split(",");
        Set<OperatorType> operatorSets = new HashSet<>();
        for (String operator : operators) {
            operator = operator.trim();
            if (operator.isEmpty()) {
                continue;
            }
            if (operator.equals("HashJoin")) {
                operatorSets.add(OperatorType.BroadcastHashJoin);
                operatorSets.add(OperatorType.ShuffleHashJoin);
            } else {
                operatorSets.add(OperatorType.valueOf(operator));
            }
        }
        return operatorSets.contains(operatorType);
    }

    /**
     * Returns the aggregate phase strategy configuration.
     *
     * @param tableConfig TableConfig object
     * @return the aggregate phase strategy
     */
    public static AggregatePhaseStrategy getAggPhaseStrategy(ReadableConfig tableConfig) {
        String aggPhaseConf = tableConfig.get(TABLE_OPTIMIZER_AGG_PHASE_STRATEGY).trim();
        if (aggPhaseConf.isEmpty()) {
            return AggregatePhaseStrategy.AUTO;
        } else {
            return AggregatePhaseStrategy.valueOf(aggPhaseConf);
        }
    }

    /**
     * Returns {@link CalciteConfig} wraps in the given TableConfig.
     *
     * @param tableConfig TableConfig object
     * @return wrapped CalciteConfig.
     */
    public static CalciteConfig getCalciteConfig(TableConfig tableConfig) {
        return tableConfig
                .getPlannerConfig()
                .unwrap(CalciteConfig.class)
                .orElse(CalciteConfig$.MODULE$.DEFAULT());
    }

    /**
     * Similar to {@link TableConfig#getLocalTimeZone()} but extracting it from a generic {@link
     * ReadableConfig}.
     *
     * @see TableConfig#getLocalTimeZone()
     */
    public static ZoneId getLocalTimeZone(ReadableConfig tableConfig) {
        String zone = tableConfig.get(TableConfigOptions.LOCAL_TIME_ZONE);
        validateTimeZone(zone);
        return TableConfigOptions.LOCAL_TIME_ZONE.defaultValue().equals(zone)
                ? ZoneId.systemDefault()
                : ZoneId.of(zone);
    }

    /**
     * Similar to {@link TableConfig#getMaxIdleStateRetentionTime()}.
     *
     * @see TableConfig#getMaxIdleStateRetentionTime()
     */
    @Deprecated
    public static long getMaxIdleStateRetentionTime(ReadableConfig tableConfig) {
        return tableConfig.get(ExecutionConfigOptions.IDLE_STATE_RETENTION).toMillis() * 3 / 2;
    }

    /** Validates user configured time zone. */
    private static void validateTimeZone(String zone) {
        final String zoneId = zone.toUpperCase();
        if (zoneId.startsWith("UTC+")
                || zoneId.startsWith("UTC-")
                || SHORT_IDS.containsKey(zoneId)) {
            throw new IllegalArgumentException(
                    String.format(
                            "The supported Zone ID is either a full name such as "
                                    + "'America/Los_Angeles', or a custom timezone id such as "
                                    + "'GMT-08:00', but configured Zone ID is '%s'.",
                            zone));
        }
    }

    // Make sure that we cannot instantiate this class
    private TableConfigUtils() {}
}
