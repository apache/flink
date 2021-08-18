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

package org.apache.flink.table.planner.delegation.hive.copy;

import org.apache.flink.table.planner.delegation.hive.SqlOperatorExprNodeDesc;

import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeFieldDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.HiveDecimalUtils;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.util.List;
import java.util.Map;

/** Counterpart of hive's ExprNodeDescUtils, but only contains methods that need overriding. */
public class HiveParserExprNodeDescUtils {

    private HiveParserExprNodeDescUtils() {}

    public static void getExprNodeColumnDesc(
            List<ExprNodeDesc> exprDescList, Map<Integer, ExprNodeDesc> hashCodeToColumnDesc) {
        for (ExprNodeDesc exprNodeDesc : exprDescList) {
            getExprNodeColumnDesc(exprNodeDesc, hashCodeToColumnDesc);
        }
    }

    private static void getExprNodeColumnDesc(
            ExprNodeDesc exprDesc, Map<Integer, ExprNodeDesc> hashCodeToColumnDescMap) {
        if (exprDesc instanceof ExprNodeColumnDesc) {
            hashCodeToColumnDescMap.put(exprDesc.hashCode(), exprDesc);
        } else if (exprDesc instanceof HiveParserExprNodeColumnListDesc) {
            for (ExprNodeDesc child : exprDesc.getChildren()) {
                getExprNodeColumnDesc(child, hashCodeToColumnDescMap);
            }
        } else if (exprDesc instanceof ExprNodeGenericFuncDesc
                || exprDesc instanceof SqlOperatorExprNodeDesc) {
            for (ExprNodeDesc child : exprDesc.getChildren()) {
                getExprNodeColumnDesc(child, hashCodeToColumnDescMap);
            }
        } else if (exprDesc instanceof ExprNodeFieldDesc) {
            getExprNodeColumnDesc(
                    ((ExprNodeFieldDesc) exprDesc).getDesc(), hashCodeToColumnDescMap);
        } else if (exprDesc instanceof HiveParserExprNodeSubQueryDesc) {
            getExprNodeColumnDesc(
                    ((HiveParserExprNodeSubQueryDesc) exprDesc).getSubQueryLhs(),
                    hashCodeToColumnDescMap);
        }
    }

    public static boolean isAllConstants(List<ExprNodeDesc> value) {
        for (ExprNodeDesc expr : value) {
            if (!(expr instanceof ExprNodeConstantDesc)) {
                return false;
            }
        }
        return true;
    }

    public static PrimitiveTypeInfo deriveMinArgumentCast(
            ExprNodeDesc childExpr, TypeInfo targetType) {
        assert targetType instanceof PrimitiveTypeInfo : "Not a primitive type" + targetType;
        PrimitiveTypeInfo pti = (PrimitiveTypeInfo) targetType;
        // We only do the minimum cast for decimals. Other types are assumed safe; fix if needed.
        // We also don't do anything for non-primitive children (maybe we should assert).
        if ((pti.getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.DECIMAL)
                || (!(childExpr.getTypeInfo() instanceof PrimitiveTypeInfo))) {
            return pti;
        }
        PrimitiveTypeInfo childTi = (PrimitiveTypeInfo) childExpr.getTypeInfo();
        // If the child is also decimal, no cast is needed (we hope - can target type be narrower?).
        return HiveDecimalUtils.getDecimalTypeForPrimitiveCategory(childTi);
    }
}
