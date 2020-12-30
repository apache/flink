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

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.planner.calcite.CalciteConfig;
import org.apache.flink.table.planner.calcite.CalciteConfig$;
import org.apache.flink.table.planner.plan.utils.OperatorType;

import java.util.HashSet;
import java.util.Set;

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
        String value = tableConfig.getConfiguration().getString(TABLE_EXEC_DISABLED_OPERATORS);
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
    public static AggregatePhaseStrategy getAggPhaseStrategy(TableConfig tableConfig) {
        String aggPhaseConf =
                tableConfig.getConfiguration().getString(TABLE_OPTIMIZER_AGG_PHASE_STRATEGY).trim();
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

    // Make sure that we cannot instantiate this class
    private TableConfigUtils() {}
}
