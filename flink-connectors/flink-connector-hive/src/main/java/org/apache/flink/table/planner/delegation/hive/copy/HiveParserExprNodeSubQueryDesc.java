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

import org.apache.calcite.rel.RelNode;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.io.Serializable;

/** Counterpart of hive's org.apache.hadoop.hive.ql.plan.ExprNodeSubQueryDesc. */
public class HiveParserExprNodeSubQueryDesc extends ExprNodeDesc implements Serializable {
    private static final long serialVersionUID = 1L;

    /** SubqueryType. */
    public enum SubqueryType {
        IN,
        EXISTS,
        SCALAR
    }

    /** RexNode corresponding to subquery. */
    private final RelNode rexSubQuery;

    private final ExprNodeDesc subQueryLhs;
    private final SubqueryType type;

    public HiveParserExprNodeSubQueryDesc(TypeInfo typeInfo, RelNode subQuery, SubqueryType type) {
        super(typeInfo);
        this.rexSubQuery = subQuery;
        this.subQueryLhs = null;
        this.type = type;
    }

    public HiveParserExprNodeSubQueryDesc(
            TypeInfo typeInfo, RelNode subQuery, SubqueryType type, ExprNodeDesc lhs) {
        super(typeInfo);
        this.rexSubQuery = subQuery;
        this.subQueryLhs = lhs;
        this.type = type;
    }

    public SubqueryType getType() {
        return type;
    }

    public ExprNodeDesc getSubQueryLhs() {
        return subQueryLhs;
    }

    public RelNode getRexSubQuery() {
        return rexSubQuery;
    }

    @Override
    public ExprNodeDesc clone() {
        return new HiveParserExprNodeSubQueryDesc(typeInfo, rexSubQuery, type, subQueryLhs);
    }

    @Override
    public boolean isSame(Object o) {
        if (!(o instanceof HiveParserExprNodeSubQueryDesc)) {
            return false;
        }
        HiveParserExprNodeSubQueryDesc dest = (HiveParserExprNodeSubQueryDesc) o;
        if (subQueryLhs != null && dest.getSubQueryLhs() != null) {
            if (!subQueryLhs.equals(dest.getSubQueryLhs())) {
                return false;
            }
        }
        if (!typeInfo.equals(dest.getTypeInfo())) {
            return false;
        }
        if (!rexSubQuery.equals(dest.getRexSubQuery())) {
            return false;
        }
        return type == dest.getType();
    }
}
