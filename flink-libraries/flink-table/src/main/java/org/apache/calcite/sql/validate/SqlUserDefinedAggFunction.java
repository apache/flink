/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.sql.validate;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.AggregateFunction;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.util.Util;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * User-defined aggregate function.
 *
 * <p>Created by the validator, after resolving a function call to a function
 * defined in a Calcite schema.</p>
 */
public class SqlUserDefinedAggFunction extends SqlAggFunction {
    public final AggregateFunction function;

    public SqlUserDefinedAggFunction(SqlIdentifier opName,
                                     SqlReturnTypeInference returnTypeInference,
                                     SqlOperandTypeInference operandTypeInference,
                                     SqlOperandTypeChecker operandTypeChecker, AggregateFunction function) {
        this(opName, returnTypeInference, operandTypeInference, operandTypeChecker, function,false);
    }

    public SqlUserDefinedAggFunction(SqlIdentifier opName,
                                     SqlReturnTypeInference returnTypeInference,
                                     SqlOperandTypeInference operandTypeInference,
                                     SqlOperandTypeChecker operandTypeChecker,
                                     AggregateFunction function,
                                     Boolean requestsOver) {
        super(Util.last(opName.names), opName, SqlKind.OTHER_FUNCTION,
              returnTypeInference, operandTypeInference, operandTypeChecker,
              SqlFunctionCategory.USER_DEFINED_FUNCTION, false, requestsOver);
        this.function = function;
    }

    @SuppressWarnings("deprecation")
    public List<RelDataType> getParameterTypes(
            final RelDataTypeFactory typeFactory) {
        return Lists.transform(function.getParameters(),
                               new Function<FunctionParameter, RelDataType>() {
                                   public RelDataType apply(FunctionParameter input) {
                                       return input.getType(typeFactory);
                                   }
                               });
    }

    @SuppressWarnings("deprecation")
    public RelDataType getReturnType(RelDataTypeFactory typeFactory) {
        return function.getReturnType(typeFactory);
    }
}

// End SqlUserDefinedAggFunction.java
