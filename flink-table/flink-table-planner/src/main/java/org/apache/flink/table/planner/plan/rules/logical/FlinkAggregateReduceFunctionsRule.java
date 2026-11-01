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

package org.apache.flink.table.planner.plan.rules.logical;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.rules.AggregateReduceFunctionsRule;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Flink's instance of {@link AggregateReduceFunctionsRule}, which reduces aggregate functions like
 * AVG and STDDEV_POP to simpler forms.
 *
 * <p>AVG on TINYINT, SMALLINT and INT is not reduced. The rule rewrites AVG(x) into SUM(x) /
 * COUNT(x), and SUM keeps the type of its argument, so the sum would overflow where the AVG
 * aggregate function itself accumulates these types as BIGINT.
 */
public class FlinkAggregateReduceFunctionsRule {

    public static final AggregateReduceFunctionsRule INSTANCE =
            AggregateReduceFunctionsRule.Config.DEFAULT
                    .withExtraCondition(FlinkAggregateReduceFunctionsRule::canReduce)
                    .toRule();

    /** Returns whether the given aggregate call can be reduced without changing its result. */
    public static boolean canReduce(AggregateCall call) {
        if (call.getAggregation().getKind() != SqlKind.AVG) {
            return true;
        }
        // the result type of AVG is the type of its argument, except for DECIMAL
        final SqlTypeName typeName = call.getType().getSqlTypeName();
        return typeName != SqlTypeName.TINYINT
                && typeName != SqlTypeName.SMALLINT
                && typeName != SqlTypeName.INTEGER;
    }

    private FlinkAggregateReduceFunctionsRule() {}
}
