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

package org.apache.flink.table.types.inference.utils;

import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.CallContext;

import java.util.List;
import java.util.Optional;

/** {@link CallContext} mock for testing purposes. */
public class CallContextMock implements CallContext {

    public DataTypeFactory typeFactory;

    public List<DataType> argumentDataTypes;

    public FunctionDefinition functionDefinition;

    public List<Boolean> argumentLiterals;

    public List<Boolean> argumentNulls;

    public List<Optional<?>> argumentValues;

    public String name;

    public Optional<DataType> outputDataType;

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
        return argumentLiterals.get(pos);
    }

    @Override
    public boolean isArgumentNull(int pos) {
        return argumentNulls.get(pos);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Optional<T> getArgumentValue(int pos, Class<T> clazz) {
        return (Optional<T>)
                argumentValues.get(pos).filter(v -> clazz.isAssignableFrom(v.getClass()));
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
        return outputDataType;
    }
}
