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

package org.apache.flink.table.planner.delegation.hive;

import org.apache.calcite.sql.SqlOperator;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.util.List;

/** An ExprNodeDesc to represent functions in Flink. */
public class SqlOperatorExprNodeDesc extends ExprNodeDesc {

    private static final long serialVersionUID = 1L;

    private final String funcName;
    private final SqlOperator sqlOperator;
    private final List<ExprNodeDesc> children;

    public SqlOperatorExprNodeDesc(
            String funcName,
            SqlOperator sqlOperator,
            List<ExprNodeDesc> children,
            TypeInfo returnType) {
        super(returnType);
        this.funcName = funcName;
        this.sqlOperator = sqlOperator;
        this.children = children;
    }

    @Override
    public ExprNodeDesc clone() {
        return new SqlOperatorExprNodeDesc(funcName, sqlOperator, children, typeInfo);
    }

    @Override
    public boolean isSame(Object o) {
        return this == o;
    }

    public SqlOperator getSqlOperator() {
        return sqlOperator;
    }

    @Override
    public List<ExprNodeDesc> getChildren() {
        return children;
    }
}
