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

package org.apache.flink.table.planner.functions.aggfunctions;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.expressions.LocalReferenceExpression;
import org.apache.flink.table.functions.DeclarativeAggregateFunction;

import static org.apache.flink.table.expressions.ApiExpressionUtils.localRef;

/**
 * Some functions like CUME_DIST/PERCENT_RANK/NTILE need the size of current window for calculation.
 * Such function need to implement the interface to provide accessing to the window size.
 *
 * <p>NOTE: Now, it can only be used by {@link DeclarativeAggregateFunction}.
 */
public interface SizeBasedWindowFunction {

    /** The field for the window size. */
    default LocalReferenceExpression windowSizeAttribute() {
        return localRef("window_size", DataTypes.INT());
    }
}
