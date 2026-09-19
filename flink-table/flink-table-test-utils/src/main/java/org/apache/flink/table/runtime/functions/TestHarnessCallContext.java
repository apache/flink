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

package org.apache.flink.table.runtime.functions;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.ModelSemantics;
import org.apache.flink.table.functions.TableSemantics;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.CallContext;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * {@link CallContext} implementation for {@link ProcessTableFunctionTestHarness} for use in
 * deriving output schemas.
 */
@Internal
class TestHarnessCallContext implements CallContext {
    DataTypeFactory typeFactory;
    List<DataType> argumentDataTypes;
    FunctionDefinition functionDefinition;
    Map<Integer, TableSemantics> tableSemantics;
    Map<Integer, Object> argumentValues = new HashMap<>();
    String name;

    @Override
    public DataTypeFactory getDataTypeFactory() {
        return typeFactory;
    }

    @Override
    public FunctionDefinition getFunctionDefinition() {
        return functionDefinition;
    }

    @Override
    public boolean isArgumentLiteral(int pos) {
        return argumentValues.containsKey(pos);
    }

    @Override
    public boolean isArgumentNull(int pos) {
        return false;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Optional<T> getArgumentValue(int pos, Class<T> clazz) {
        Object value = argumentValues.get(pos);
        if (value != null && clazz.isInstance(value)) {
            return Optional.of((T) value);
        }
        return Optional.empty();
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public List<DataType> getArgumentDataTypes() {
        return argumentDataTypes;
    }

    @Override
    public Optional<DataType> getOutputDataType() {
        return Optional.empty();
    }

    @Override
    public boolean isGroupedAggregation() {
        return false;
    }

    @Override
    public Optional<TableSemantics> getTableSemantics(int pos) {
        if (tableSemantics == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(tableSemantics.get(pos));
    }

    @Override
    public Optional<ModelSemantics> getModelSemantics(int pos) {
        return Optional.empty();
    }
}
