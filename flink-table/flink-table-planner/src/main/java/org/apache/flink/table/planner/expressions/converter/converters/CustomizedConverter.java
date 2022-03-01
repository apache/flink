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

package org.apache.flink.table.planner.expressions.converter.converters;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.planner.expressions.converter.CallExpressionConvertRule;
import org.apache.flink.table.planner.expressions.converter.CustomizedConvertRule;

import org.apache.calcite.rex.RexNode;

/** Customized converter used by {@link CustomizedConvertRule}. */
@Internal
public abstract class CustomizedConverter {

    public abstract RexNode convert(
            CallExpression call, CallExpressionConvertRule.ConvertContext context);

    // ---------------------------------------------------------------------------------------------

    protected static void checkArgumentNumber(CallExpression call, int... validArgumentCounts) {
        boolean hasValidArgumentCount = false;
        for (int argumentCount : validArgumentCounts) {
            if (call.getChildren().size() == argumentCount) {
                hasValidArgumentCount = true;
                break;
            }
        }

        checkArgument(call, hasValidArgumentCount);
    }

    protected static void checkArgument(CallExpression call, boolean check) {
        if (!check) {
            throw new TableException("Invalid arguments for call: " + call);
        }
    }
}
