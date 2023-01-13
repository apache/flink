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

import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.util.ArrayList;
import java.util.List;

/** Counterpart of hive's org.apache.hadoop.hive.ql.plan.ExprNodeColumnListDesc. */
public class HiveParserExprNodeColumnListDesc extends ExprNodeDesc {

    private static final long serialVersionUID = 1L;
    // column or constant
    final List<ExprNodeDesc> columns = new ArrayList<>();

    public void addColumn(ExprNodeDesc column) {
        columns.add(column);
    }

    @Override
    public ExprNodeDesc clone() {
        HiveParserExprNodeColumnListDesc clone = new HiveParserExprNodeColumnListDesc();
        clone.columns.addAll(columns);
        return clone;
    }

    @Override
    public boolean isSame(Object o) {
        if (o instanceof HiveParserExprNodeColumnListDesc) {
            return columns.equals(((HiveParserExprNodeColumnListDesc) o).columns);
        }
        return false;
    }

    @Override
    public TypeInfo getTypeInfo() {
        throw new IllegalStateException();
    }

    @Override
    public void setTypeInfo(TypeInfo typeInfo) {
        throw new IllegalStateException();
    }

    @Override
    public ObjectInspector getWritableObjectInspector() {
        throw new IllegalStateException();
    }

    @Override
    public String getTypeString() {
        throw new IllegalStateException();
    }

    @Override
    public List<String> getCols() {
        throw new IllegalStateException();
    }

    @Override
    public List<ExprNodeDesc> getChildren() {
        return new ArrayList<ExprNodeDesc>(columns);
    }
}
